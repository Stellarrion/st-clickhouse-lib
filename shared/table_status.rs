use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use super::super::error::{Error, Result};
use super::packet::ClientPacket;
use super::revision;
use super::wire;

const MAX_TABLE_STATUS_ENTRIES: usize = 0x00FF_FFFF;

/// Fully qualified ClickHouse table name used by TablesStatus requests.
#[derive(Clone, Debug, Eq)]
pub struct QualifiedTableName {
    pub database: String,
    pub table: String,
}

impl QualifiedTableName {
    pub fn new(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
        }
    }
}

impl PartialEq for QualifiedTableName {
    fn eq(&self, other: &Self) -> bool {
        self.database == other.database && self.table == other.table
    }
}

impl Hash for QualifiedTableName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.database.hash(state);
        self.table.hash(state);
    }
}

impl From<(&str, &str)> for QualifiedTableName {
    fn from(value: (&str, &str)) -> Self {
        Self::new(value.0, value.1)
    }
}

/// Status returned by ClickHouse for a table in a TablesStatus response.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableStatus {
    pub is_replicated: bool,
    pub absolute_delay: u64,
    pub is_readonly: bool,
}

/// Response to a TablesStatus request.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TablesStatusResponse {
    pub table_states_by_id: HashMap<QualifiedTableName, TableStatus>,
}

impl TablesStatusResponse {
    pub fn get(&self, database: &str, table: &str) -> Option<&TableStatus> {
        self.table_states_by_id
            .get(&QualifiedTableName::new(database, table))
    }
}

pub(crate) fn build_tables_status_request(
    tables: &[QualifiedTableName],
    revision: u64,
) -> Result<Vec<u8>> {
    validate_tables_status_revision(revision)?;
    let mut buf = Vec::with_capacity(2 + tables.iter().map(table_name_capacity).sum::<usize>());
    wire::write_varint_to_vec(&mut buf, ClientPacket::TablesStatusRequest as u64);
    wire::write_varint_to_vec(&mut buf, tables.len() as u64);
    for table in tables {
        wire::write_string_to_vec(&mut buf, &table.database);
        wire::write_string_to_vec(&mut buf, &table.table);
    }
    Ok(buf)
}

pub fn read_tables_status_response<R: std::io::Read>(
    reader: &mut R,
    revision: u64,
) -> Result<TablesStatusResponse> {
    validate_tables_status_revision(revision)?;
    let count = checked_entry_count(wire::read_varint(reader)?)?;
    let mut table_states_by_id = HashMap::with_capacity(count);
    for _ in 0..count {
        let database = wire::read_string(reader)?;
        let table = wire::read_string(reader)?;
        let status = read_table_status(reader, revision)?;
        table_states_by_id.insert(QualifiedTableName::new(database, table), status);
    }
    Ok(TablesStatusResponse { table_states_by_id })
}

fn read_table_status<R: std::io::Read>(reader: &mut R, revision: u64) -> Result<TableStatus> {
    let mut flag = [0u8; 1];
    reader.read_exact(&mut flag)?;
    let is_replicated = flag[0] != 0;
    if !is_replicated {
        return Ok(TableStatus::default());
    }
    let absolute_delay = wire::read_varint(reader)?;
    let is_readonly = if revision >= revision::DBMS_MIN_REVISION_WITH_TABLE_READ_ONLY_CHECK {
        wire::read_varint(reader)? != 0
    } else {
        false
    };
    Ok(TableStatus {
        is_replicated,
        absolute_delay,
        is_readonly,
    })
}

fn validate_tables_status_revision(rev: u64) -> Result<()> {
    if rev < revision::DBMS_MIN_REVISION_WITH_TABLES_STATUS {
        return Err(Error::Protocol(format!(
            "TablesStatus requires protocol revision >= {}",
            revision::DBMS_MIN_REVISION_WITH_TABLES_STATUS
        )));
    }
    Ok(())
}

fn checked_entry_count(value: u64) -> Result<usize> {
    let count = usize::try_from(value)
        .map_err(|_| Error::Protocol("TablesStatus entry count too large".into()))?;
    if count > MAX_TABLE_STATUS_ENTRIES {
        return Err(Error::Protocol(format!(
            "TablesStatus entry count {count} exceeds limit {MAX_TABLE_STATUS_ENTRIES}"
        )));
    }
    Ok(count)
}

fn table_name_capacity(table: &QualifiedTableName) -> usize {
    table.database.len() + table.table.len() + 18
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_status_request_matches_clickhouse_layout() {
        let tables = [QualifiedTableName::new("db", "tbl")];
        let packet = build_tables_status_request(&tables, 54483).expect("request should encode");
        let mut cursor = &packet[..];
        assert_eq!(wire::read_varint(&mut cursor).expect("packet type"), 5);
        assert_eq!(wire::read_varint(&mut cursor).expect("count"), 1);
        assert_eq!(wire::read_string(&mut cursor).expect("database"), "db");
        assert_eq!(wire::read_string(&mut cursor).expect("table"), "tbl");
        assert!(cursor.is_empty());
    }

    #[test]
    fn tables_status_response_reads_readonly_gate() {
        let mut packet = Vec::new();
        wire::write_varint_to_vec(&mut packet, 1);
        wire::write_string_to_vec(&mut packet, "db");
        wire::write_string_to_vec(&mut packet, "tbl");
        packet.push(1);
        wire::write_varint_to_vec(&mut packet, 42);
        wire::write_varint_to_vec(&mut packet, 1);

        let response =
            read_tables_status_response(&mut &packet[..], 54483).expect("response should decode");
        let status = response.get("db", "tbl").expect("table status");
        assert!(status.is_replicated);
        assert_eq!(status.absolute_delay, 42);
        assert!(status.is_readonly);
    }
}
