use crate::io::{
    get_meta_path, AssetReader, AssetReaderError, EmptyPathStream, PathStream, Reader, AsyncRead, LocalStackFuture, STACK_FUTURE_SIZE, AsyncSeekForward,
};
use bevy_utils::tracing::error;
use js_sys::JSON;
use core::task::{Poll, Context};
use std::path::{Path, PathBuf};
use wasm_bindgen::{prelude::wasm_bindgen, JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::Response;
use core::pin::Pin;

pub use js_sys::Uint8Array;

/// Represents the global object in the JavaScript context
#[wasm_bindgen]
extern "C" {
    /// The [Global](https://developer.mozilla.org/en-US/docs/Glossary/Global_object) object.
    type Global;

    /// The [window](https://developer.mozilla.org/en-US/docs/Web/API/Window) global object.
    #[wasm_bindgen(method, getter, js_name = Window)]
    fn window(this: &Global) -> JsValue;

    /// The [WorkerGlobalScope](https://developer.mozilla.org/en-US/docs/Web/API/WorkerGlobalScope) global object.
    #[wasm_bindgen(method, getter, js_name = WorkerGlobalScope)]
    fn worker(this: &Global) -> JsValue;
}

/// Reader implementation for loading assets via HTTP in Wasm.
pub struct HttpWasmAssetReader {
    root_path: PathBuf,
}

impl HttpWasmAssetReader {
    /// Creates a new `WasmAssetReader`. The path provided will be used to build URLs to query for assets.
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            root_path: path.as_ref().to_owned(),
        }
    }
}

fn js_value_to_err(context: &str) -> impl FnOnce(JsValue) -> std::io::Error + '_ {
    move |value| {
        let message = match JSON::stringify(&value) {
            Ok(js_str) => format!("Failed to {context}: {js_str}"),
            Err(_) => {
                format!("Failed to {context} and also failed to stringify the JSValue of the error")
            }
        };

        std::io::Error::new(std::io::ErrorKind::Other, message)
    }
}

impl HttpWasmAssetReader {
    async fn fetch_bytes<'a>(&self, path: PathBuf) -> Result<impl Reader, AssetReaderError> {
        // The JS global scope includes a self-reference via a specializing name, which can be used to determine the type of global context available.
        let global: Global = js_sys::global().unchecked_into();
        let promise = if !global.window().is_undefined() {
            let window: web_sys::Window = global.unchecked_into();
            window.fetch_with_str(path.to_str().unwrap())
        } else if !global.worker().is_undefined() {
            let worker: web_sys::WorkerGlobalScope = global.unchecked_into();
            worker.fetch_with_str(path.to_str().unwrap())
        } else {
            let error = std::io::Error::new(
                std::io::ErrorKind::Other,
                "Unsupported JavaScript global context",
            );
            return Err(AssetReaderError::Io(error.into()));
        };
        let resp_value = JsFuture::from(promise)
            .await
            .map_err(js_value_to_err("fetch path"))?;
        let resp = resp_value
            .dyn_into::<Response>()
            .map_err(js_value_to_err("convert fetch to Response"))?;
        match resp.status() {
            200 => {
                let data = JsFuture::from(resp.array_buffer().unwrap()).await.unwrap();
                let bytes = Uint8Array::new(&data);
                let reader = Uint8ArrayReader::new(bytes);
                Ok(reader)
            }
            404 => Err(AssetReaderError::NotFound(path)),
            status => Err(AssetReaderError::HttpError(status)),
        }
    }
}

impl AssetReader for HttpWasmAssetReader {
    async fn read<'a>(&'a self, path: &'a Path) -> Result<impl Reader + 'a, AssetReaderError> {
        let path = self.root_path.join(path);
        self.fetch_bytes(path).await
    }

    async fn read_meta<'a>(&'a self, path: &'a Path) -> Result<impl Reader + 'a, AssetReaderError> {
        let meta_path = get_meta_path(&self.root_path.join(path));
        self.fetch_bytes(meta_path).await
    }

    async fn read_directory<'a>(
        &'a self,
        _path: &'a Path,
    ) -> Result<Box<PathStream>, AssetReaderError> {
        let stream: Box<PathStream> = Box::new(EmptyPathStream);
        error!("Reading directories is not supported with the HttpWasmAssetReader");
        Ok(stream)
    }

    async fn is_directory<'a>(&'a self, _path: &'a Path) -> Result<bool, AssetReaderError> {
        error!("Reading directories is not supported with the HttpWasmAssetReader");
        Ok(false)
    }
}

/// An [`AsyncRead`] implementation capable of reading a [`Uint8Array`].
pub struct Uint8ArrayReader {
    array: Uint8Array,
    initial_offset: u32,
}

impl Uint8ArrayReader {
    /// Create a new [`Uint8ArrayReader`] for `bytes`.
    pub fn new(array: Uint8Array) -> Self {
        Self {
            initial_offset: array.byte_offset(),
            array,
        }
    }
}

impl AsyncRead for Uint8ArrayReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<futures_io::Result<usize>> {
        let array_len = self.array.length();
        let n = u32::min(buf.len() as u32, array_len);
        self.array.subarray(0, n).copy_to(&mut buf[..n as usize]); // NOTE: copy_to will panic if the lengths do not exactly match
        self.array = self.array.subarray(n, array_len);
        Poll::Ready(Ok(n as usize))
    }
}

impl AsyncSeekForward for Uint8ArrayReader {
    fn poll_seek_forward(
        mut self: Pin<&mut Self>,
        _cx: &mut Context,
        offset: u64,
    ) -> Poll<std::io::Result<u64>> {
        let array_len = self.array.length();
        let offset_u32 = u32::min(offset as u32, array_len); // NOTE: this trait allows seeking past the end of the internal stream
        self.array = self.array.subarray(offset_u32, array_len);
        let new_offset = self.array.byte_offset() - self.initial_offset + offset_u32;
        Poll::Ready(Ok(new_offset.into()))
    }
}

impl Reader for Uint8ArrayReader {
    fn read_to_end<'a>(
        &'a mut self,
        buf: &'a mut Vec<u8>,
    ) -> LocalStackFuture<'a, std::io::Result<usize>, STACK_FUTURE_SIZE> {
        #[expect(unsafe_code)]
        LocalStackFuture::from(async {
            let n = self.array.length();
            let n_usize = n as usize;

            buf.reserve_exact(n_usize);
            let spare_capacity =  buf.spare_capacity_mut();
            debug_assert!(spare_capacity.len() >= n_usize);
            // NOTE: `copy_to_uninit` requires the lengths to match exactly,
            // and `reserve_exact` may reserve more capacity than required.
            self.array.copy_to_uninit(&mut spare_capacity[..n_usize]);
            // SAFETY:
            // * the vector has enough spare capacity for `n` additional bytes due to `reserve_exact` above
            // * the bytes have been initialized due to `copy_to_uninit` above.
            unsafe {
                let new_len = buf.len() + n_usize;
                buf.set_len(new_len);
            }
            self.array = self.array.subarray(n, n);

            Ok(n_usize)
        })
    }
}
