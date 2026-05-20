use crate::protocol::block::Block;

/// Query execution progress reported by the server.
pub struct Progress {
    pub rows: u64,
    pub bytes: u64,
    pub total_rows: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
}

/// Query profile info reported by the server.
pub struct Profile {
    pub rows: u64,
    pub blocks: u64,
    pub bytes: u64,
    pub rows_before_limit: u64,
    pub applied_limit: bool,
    pub calculated_rows_before_limit: bool,
}

/// User-provided callbacks invoked during query response processing.
#[derive(Default)]
pub struct QueryCallbacks {
    pub on_progress: Option<Box<dyn Fn(Progress) + Send + Sync>>,
    pub on_profile: Option<Box<dyn Fn(Profile) + Send + Sync>>,
    pub on_log: Option<Box<dyn Fn(&Block) + Send + Sync>>,
    pub on_profile_events: Option<Box<dyn Fn(&Block) + Send + Sync>>,
    pub on_timezone_update: Option<Box<dyn Fn(&str) + Send + Sync>>,
    pub on_part_uuids: Option<Box<dyn Fn(&[[u8; 16]]) + Send + Sync>>,
}
