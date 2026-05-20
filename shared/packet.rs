use super::super::error::{Error, Result};

// ───────────────────────────────────────────────
// Packet type enums (from Protocol.h)
// ───────────────────────────────────────────────

/// Server → Client packet types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum ServerPacket {
    Hello = 0,
    Data = 1,
    Exception = 2,
    Progress = 3,
    Pong = 4,
    EndOfStream = 5,
    ProfileInfo = 6,
    Totals = 7,
    Extremes = 8,
    TablesStatusResponse = 9,
    Log = 10,
    TableColumns = 11,
    PartUUIDs = 12,
    ReadTaskRequest = 13,
    ProfileEvents = 14,
    MergeTreeAllRangesAnnouncement = 15,
    MergeTreeReadTaskRequest = 16,
    TimezoneUpdate = 17,
    SSHChallenge = 18,
}

impl ServerPacket {
    pub fn from_u64(code: u64) -> Result<Self> {
        Ok(match code {
            0 => Self::Hello,
            1 => Self::Data,
            2 => Self::Exception,
            3 => Self::Progress,
            4 => Self::Pong,
            5 => Self::EndOfStream,
            6 => Self::ProfileInfo,
            7 => Self::Totals,
            8 => Self::Extremes,
            9 => Self::TablesStatusResponse,
            10 => Self::Log,
            11 => Self::TableColumns,
            12 => Self::PartUUIDs,
            13 => Self::ReadTaskRequest,
            14 => Self::ProfileEvents,
            15 => Self::MergeTreeAllRangesAnnouncement,
            16 => Self::MergeTreeReadTaskRequest,
            17 => Self::TimezoneUpdate,
            18 => Self::SSHChallenge,
            _ => {
                return Err(Error::Protocol(format!("unknown server packet: {code}")))
            },
        })
    }
}

/// Client → Server packet types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum ClientPacket {
    Hello = 0,
    Query = 1,
    Data = 2,
    Cancel = 3,
    Ping = 4,
    TablesStatusRequest = 5,
    KeepAlive = 6,
    Scalar = 7,
    IgnoredPartUUIDs = 8,
    ReadTaskResponse = 9,
    MergeTreeReadTaskResponse = 10,
    SSHChallengeRequest = 11,
    SSHChallengeResponse = 12,
    QueryPlan = 13,
}
