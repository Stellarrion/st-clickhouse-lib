use super::super::error::{Error, Result};
#[cfg(feature = "tokio")]
use crate::runtime::io::{AsyncRead, AsyncReadExt};

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/shared/async_wire.rs"));
include!(concat!(env!("CARGO_MANIFEST_DIR"), "/shared/wire.rs"));
