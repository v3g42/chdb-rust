use std::{
    ffi::{c_char, CStr, CString},
    slice,
    time::Duration,
};

use crate::bindings;

pub fn query(query: &str, format: &str, input_format: Option<&str>) -> Option<LocalResultV2> {
    let mut argv: Vec<String> = Vec::new();
    argv.push("clickhouse".to_string());
    argv.push("--multiquery".to_string());
    argv.push(format!("--output-format={format}"));

    if let Some(input) = input_format {
        argv.push(format!("--input-format={input}"));
    }
    argv.push(format!("--query={query}"));

    let argc = argv.len() as i32;
    let mut argv: Vec<*mut c_char> = argv
        .into_iter()
        .map(|arg| CString::new(arg).unwrap().into_raw())
        .collect();

    let argv = argv.as_mut_ptr();
    let local = unsafe { bindings::query_stable_v2(argc, argv) };

    if local.is_null() {
        return None;
    }

    Some(LocalResultV2 { local })
}

#[derive(Debug, Clone)]
pub struct LocalResult {
    pub(crate) local: *mut bindings::local_result,
}

impl LocalResult {
    pub fn rows_read(&self) -> u64 {
        (unsafe { *self.local }).rows_read
    }

    pub fn bytes_read(&self) -> u64 {
        unsafe { (*self.local).bytes_read }
    }

    pub fn buf(&self) -> &[u8] {
        let buf = unsafe { (*self.local).buf };
        let len = unsafe { (*self.local).len };
        let bytes: &[u8] = unsafe { slice::from_raw_parts(buf as *const u8, len) };
        bytes
    }

    pub fn elapsed(&self) -> Duration {
        let elapsed = unsafe { (*self.local).elapsed };
        Duration::from_secs_f64(elapsed)
    }
}

impl Drop for LocalResult {
    fn drop(&mut self) {
        unsafe { bindings::free_result(self.local) };
    }
}

#[derive(Debug, Clone)]
pub struct LocalResultV2 {
    pub(crate) local: *mut bindings::local_result_v2,
}

impl LocalResultV2 {
    pub fn rows_read(&self) -> u64 {
        (unsafe { *self.local }).rows_read
    }

    pub fn bytes_read(&self) -> u64 {
        unsafe { (*self.local).bytes_read }
    }

    pub fn buf(&self) -> &[u8] {
        let len = unsafe { (*self.local).len };
        let buf = unsafe { (*self.local).buf };
        let bytes: &[u8] = unsafe { slice::from_raw_parts(buf as *const u8, len) };
        bytes
    }

    pub fn elapsed(&self) -> Duration {
        let elapsed = unsafe { (*self.local).elapsed };
        Duration::from_secs_f64(elapsed)
    }

    pub fn error_message(&self) -> &str {
        let c_str: &CStr = unsafe { CStr::from_ptr((*self.local).error_message as *const c_char) };
        let str_slice: &str = c_str.to_str().unwrap();
        str_slice
    }

    pub fn has_error(&self) -> bool {
        unsafe { !(*self.local).error_message.is_null() }
    }
}

impl Drop for LocalResultV2 {
    fn drop(&mut self) {
        unsafe { bindings::free_result_v2(self.local) };
    }
}
