use std::{fs, path::PathBuf};

use tracing::error;

use crate::{session::Session, Error};

pub struct SessionBuilder {
    format: String,
    log_level: Option<String>,
    data_path: Option<std::path::PathBuf>,
    udf_path: Option<std::path::PathBuf>,
}

impl SessionBuilder {
    pub fn new() -> Self {
        SessionBuilder {
            format: "CSV".to_owned(),
            log_level: None,
            data_path: None,
            udf_path: None,
        }
    }

    pub fn format(mut self, format: impl Into<String>) -> Self {
        self.format = format.into();
        self
    }

    pub fn data_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_path = Some(path.into());
        self
    }

    pub fn udf_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.udf_path = Some(path.into());
        self
    }

    pub fn log_level(mut self, level: &str) -> Self {
        self.log_level = Some(match level {
            "trace" => "trace".to_string(),
            "debug" => "debug".to_string(),
            "info" => "information".to_string(),
            "warn" => "warning".to_string(),
            _ => {
                error!("Invalid log level. Setting to info");
                "information".to_string()
            }
        });
        self
    }

    pub fn build(self) -> Result<Session, Error> {
        let data_path = if let Some(data_path) = self.data_path {
            std::fs::create_dir_all(&data_path)?;
            if fs::metadata(&data_path)?.permissions().readonly() {
                return Err(Error::InsufficientPermissions);
            }
            Some(data_path.to_str().ok_or(Error::PathError)?.to_string())
        } else {
            None
        };

        let udf_path = if let Some(udf_path) = self.udf_path {
            std::fs::create_dir_all(&udf_path)?;
            if fs::metadata(&udf_path)?.permissions().readonly() {
                return Err(Error::InsufficientPermissions);
            }
            Some(udf_path.to_str().ok_or(Error::PathError)?.to_string())
        } else {
            None
        };

        Ok(Session {
            format: self.format,
            data_path,
            udf_path,
            log_level: self.log_level,
        })
    }
}
