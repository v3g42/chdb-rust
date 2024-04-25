use std::ffi::{c_char, CString};

use crate::{bindings, LocalResultV2};

pub struct Session {
    pub(crate) format: String,
    pub(crate) data_path: Option<String>,
    pub(crate) udf_path: Option<String>,
    pub(crate) log_level: Option<String>,
}

impl Session {
    pub fn execute(&self, query: impl Into<String>) -> Option<LocalResultV2> {
        let mut argv = vec![
            "clickhouse".to_string(),
            "--multiquery".to_string(),
            format!("--output-format={}", self.format),
            format!("--query={}", query.into()),
        ];
        if let Some(data_path) = &self.data_path {
            argv.push(format!("--path={}", data_path));
        }

        if let Some(udf_path) = &self.udf_path {
            argv.extend([
                "--".to_string(),
                format!("--user_scripts_path={}", udf_path),
                format!(
                    "--user_defined_executable_functions_config={}/*.xml",
                    udf_path
                ),
            ]);
        }
        if let Some(log_level) = &self.log_level {
            argv.push(format!("--log-level={}", log_level));
        }

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
}
