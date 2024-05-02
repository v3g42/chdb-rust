mod basic;
#[allow(
    dead_code,
    unused,
    non_snake_case,
    non_camel_case_types,
    non_upper_case_globals
)]
mod bindings;
mod builder;
mod session;

use std::string::FromUtf8Error;

pub use basic::*;
pub use builder::*;
pub use session::*;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("An unknown error has occurred")]
    Unknown,
    #[error("Invalid data: {0}")]
    InvalidData(String),
    #[error("Invalid path")]
    PathError,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Insufficient dir permissions")]
    InsufficientPermissions,
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
}
pub type ChdbResult<T> = Result<T, Error>;
