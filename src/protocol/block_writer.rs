use super::super::compression::{CompressionMethod, encode_frame};
use super::super::error::Result;
use super::block::Block;
use super::wire;

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/shared/block_writer.rs"
));
