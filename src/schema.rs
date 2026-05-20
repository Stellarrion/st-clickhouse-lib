use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::protocol::block::Block;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableColumn {
    pub name: String,
    pub type_name: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableSchema {
    pub columns: Vec<TableColumn>,
}

impl TableSchema {
    pub fn validate_insert_block(&self, table: &str, block: &Block) -> Result<()> {
        let mut columns = HashMap::with_capacity(self.columns.len());
        for col in &self.columns {
            columns.insert(col.name.as_str(), col.type_name.as_str());
        }

        for col in &block.columns {
            let Some(expected_type) = columns.get(col.name.as_str()) else {
                return Err(Error::Protocol(format!(
                    "insert block for {table} contains unknown column '{}'",
                    col.name
                )));
            };
            if *expected_type != col.type_name {
                return Err(Error::Protocol(format!(
                    "insert block for {table} column '{}' has type '{}', expected '{}'",
                    col.name, col.type_name, expected_type
                )));
            }
        }
        Ok(())
    }
}

pub fn quote_identifier_path(path: &str) -> Result<String> {
    let parts = split_identifier_path(path)?;
    if parts.is_empty() {
        return Err(Error::Protocol("empty table name".into()));
    }
    let mut out = String::with_capacity(path.len() + parts.len() * 2);
    for (idx, part) in parts.iter().enumerate() {
        if idx != 0 {
            out.push('.');
        }
        quote_identifier(part, &mut out);
    }
    Ok(out)
}

pub fn query_may_change_schema(query: &str) -> bool {
    let Some(first) = query.split_whitespace().next() else {
        return false;
    };
    matches!(
        first.to_ascii_uppercase().as_str(),
        "ALTER" | "ATTACH" | "CREATE" | "DETACH" | "DROP" | "EXCHANGE" | "RENAME" | "TRUNCATE"
    )
}

fn quote_identifier(identifier: &str, out: &mut String) {
    out.push('`');
    for ch in identifier.chars() {
        if ch == '`' {
            out.push('`');
        }
        out.push(ch);
    }
    out.push('`');
}

fn split_identifier_path(path: &str) -> Result<Vec<String>> {
    let mut parts = Vec::new();
    let mut part = String::new();
    let mut chars = path.trim().chars().peekable();
    let mut quoted = false;

    while let Some(ch) = chars.next() {
        match ch {
            '`' if quoted && chars.peek() == Some(&'`') => {
                part.push('`');
                let _ = chars.next();
            },
            '`' => quoted = !quoted,
            '.' if !quoted => {
                let trimmed = part.trim();
                if trimmed.is_empty() {
                    return Err(Error::Protocol(format!("invalid table name '{path}'")));
                }
                parts.push(trimmed.to_owned());
                part.clear();
            },
            ch => part.push(ch),
        }
    }

    if quoted {
        return Err(Error::Protocol(format!(
            "unterminated quoted identifier in '{path}'"
        )));
    }
    let trimmed = part.trim();
    if trimmed.is_empty() {
        return Err(Error::Protocol(format!("invalid table name '{path}'")));
    }
    parts.push(trimmed.to_owned());
    Ok(parts)
}
