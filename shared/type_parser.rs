// ClickHouse type name parser.
//
// Converts type name strings (e.g. "Nullable(Array(UInt64))") into a structured
// `ColumnType` enum. Used instead of fragile string matching for dispatch in
// column skip/read functions.

use std::fmt;

/// A structured ClickHouse column type, parsed from its type name string.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    // Fixed-size primitives
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    Float32,
    Float64,
    Bool,
    // Variable-size
    String,
    FixedString(usize),
    // Temporal
    Date,
    Date32,
    DateTime,
    DateTime64(u32), // scale
    // Decimal
    Decimal(u32, u32), // precision, scale
    // UUID/IP
    UUID,
    IPv4,
    IPv6,
    // Enum
    Enum8,
    Enum16,
    // Compound
    Nullable(Box<ColumnType>),
    Array(Box<ColumnType>),
    Map(Box<ColumnType>, Box<ColumnType>),
    Tuple(Vec<ColumnType>),
    LowCardinality(Box<ColumnType>),
    /// AggregateFunction — binary aggregate state (any function/type).
    AggregateFunction,
    /// SimpleAggregateFunction — like AggregateFunction.
    SimpleAggregateFunction,
    Nothing,
    Time,
    Time64(u32),
    /// JSON / Object('json') — sub-column tree (CH 24.8+).
    JSON,
    /// Variant(T1, T2, ...) — discriminated union (CH 24.8+).
    Variant(Vec<ColumnType>),
    /// Dynamic — fully dynamic type (CH 24.8+).
    Dynamic,
    /// Geo aliases with tuple/array wire layouts.
    Point,
    Ring,
    Polygon,
    MultiPolygon,
    /// Fallback for unknown types — preserves the raw string.
    Other(String),
}

impl ColumnType {
    /// Number of bytes per row for fixed-size types, or None for variable.
    pub fn fixed_width(&self) -> Option<usize> {
        use ColumnType::*;
        Some(match self {
            UInt8 | Int8 | Bool | Enum8 => 1,
            UInt16 | Int16 | Date | Enum16 => 2,
            UInt32 | Int32 | Float32 | DateTime | IPv4 => 4,
            UInt64 | Int64 | Float64 | DateTime64(_) | Time64(_) => 8,
            IPv6 => 16,
            UInt128 | Int128 | UUID => 16,
            UInt256 | Int256 => 32,
            FixedString(n) => *n,
            Decimal(1..=9, _) => 4,
            Decimal(10..=18, _) => 8,
            Decimal(19..=38, _) => 16,
            Decimal(39..=76, _) => 32,
            Decimal(_, _) => return None,
            Nothing => 0,
            Point => 16,
            JSON
            | Dynamic
            | Variant(_)
            | AggregateFunction
            | SimpleAggregateFunction
            | Ring
            | Polygon
            | MultiPolygon => return None,
            _ => return None,
        })
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ColumnType::*;
        match self {
            UInt8 => write!(f, "UInt8"),
            UInt16 => write!(f, "UInt16"),
            UInt32 => write!(f, "UInt32"),
            UInt64 => write!(f, "UInt64"),
            UInt128 => write!(f, "UInt128"),
            UInt256 => write!(f, "UInt256"),
            Int8 => write!(f, "Int8"),
            Int16 => write!(f, "Int16"),
            Int32 => write!(f, "Int32"),
            Int64 => write!(f, "Int64"),
            Int128 => write!(f, "Int128"),
            Int256 => write!(f, "Int256"),
            Float32 => write!(f, "Float32"),
            Float64 => write!(f, "Float64"),
            Bool => write!(f, "Bool"),
            String => write!(f, "String"),
            FixedString(n) => write!(f, "FixedString({n})"),
            Date => write!(f, "Date"),
            Date32 => write!(f, "Date32"),
            DateTime => write!(f, "DateTime"),
            DateTime64(s) => write!(f, "DateTime64({s})"),
            Decimal(p, s) => write!(f, "Decimal({p}, {s})"),
            UUID => write!(f, "UUID"),
            IPv4 => write!(f, "IPv4"),
            IPv6 => write!(f, "IPv6"),
            Enum8 => write!(f, "Enum8"),
            Enum16 => write!(f, "Enum16"),
            Nullable(inner) => write!(f, "Nullable({inner})"),
            Array(inner) => write!(f, "Array({inner})"),
            Map(k, v) => write!(f, "Map({k}, {v})"),
            Tuple(types) => {
                write!(f, "Tuple(")?;
                for (i, t) in types.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{t}")?;
                }
                write!(f, ")")
            },
            LowCardinality(inner) => write!(f, "LowCardinality({inner})"),
            AggregateFunction => write!(f, "AggregateFunction"),
            SimpleAggregateFunction => write!(f, "SimpleAggregateFunction"),
            Nothing => write!(f, "Nothing"),
            Time => write!(f, "Time"),
            Time64(s) => write!(f, "Time64({s})"),
            JSON => write!(f, "JSON"),
            Variant(types) => {
                write!(f, "Variant(")?;
                for (i, t) in types.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{t}")?;
                }
                write!(f, ")")
            },
            Dynamic => write!(f, "Dynamic"),
            Point => write!(f, "Point"),
            Ring => write!(f, "Ring"),
            Polygon => write!(f, "Polygon"),
            MultiPolygon => write!(f, "MultiPolygon"),
            Other(s) => write!(f, "{s}"),
        }
    }
}

/// Parse a ClickHouse type name string into a `ColumnType`.
pub fn parse_type(s: &str) -> Result<ColumnType, String> {
    let s = s.trim();
    // Find the base type name: alphanumeric characters, underscores
    let paren = s.find('(');
    let base_end = paren.unwrap_or(s.len());
    let base = &s[..base_end];

    if base.is_empty() {
        return Err("empty type name".into());
    }

    // Check for nested types (have parenthesized parameters)
    if let Some(paren_pos) = paren {
        let inner = s[paren_pos + 1..s.len() - 1].trim();
        match base {
            "Nullable" => {
                let inner_type = parse_type(inner)?;
                Ok(ColumnType::Nullable(Box::new(inner_type)))
            },
            "Array" => {
                let inner_type = parse_type(inner)?;
                Ok(ColumnType::Array(Box::new(inner_type)))
            },
            "Map" => {
                // Parse two comma-separated types at the top level
                let comma = find_top_level_comma(inner)
                    .ok_or_else(|| format!("cannot parse Map types from: {inner}"))?;
                let kt = parse_type(&inner[..comma])?;
                let vt = parse_type(&inner[comma + 1..])?;
                Ok(ColumnType::Map(Box::new(kt), Box::new(vt)))
            },
            "Tuple" => {
                let mut types = Vec::new();
                let mut remaining = inner;
                while !remaining.is_empty() {
                    let comma = find_top_level_comma(remaining);
                    match comma {
                        Some(pos) => {
                            types.push(parse_type(&remaining[..pos])?);
                            remaining = &remaining[pos + 1..];
                        },
                        None => {
                            types.push(parse_type(remaining)?);
                            break;
                        },
                    }
                }
                Ok(ColumnType::Tuple(types))
            },
            "LowCardinality" => {
                let inner_type = parse_type(inner)?;
                Ok(ColumnType::LowCardinality(Box::new(inner_type)))
            },
            "Enum8" | "Enum16" => {
                // Enum names/values don't matter for byte size — just Int8/Int16
                if base == "Enum8" {
                    Ok(ColumnType::Enum8)
                } else {
                    Ok(ColumnType::Enum16)
                }
            },
            "FixedString" => {
                let n = inner
                    .trim()
                    .parse::<usize>()
                    .map_err(|e| format!("FixedString: {e}"))?;
                Ok(ColumnType::FixedString(n))
            },
            "DateTime64" => {
                // May be "DateTime64(3)" or "DateTime64(3, 'UTC')"
                let first = inner.split(',').next().unwrap_or("0").trim();
                let scale = first
                    .parse::<u32>()
                    .map_err(|e| format!("DateTime64: {e}"))?;
                Ok(ColumnType::DateTime64(scale))
            },
            "DateTime" => {
                // DateTime('UTC') has the same UInt32 native layout.
                Ok(ColumnType::DateTime)
            },
            "Time64" => {
                let scale = inner
                    .split(',')
                    .next()
                    .unwrap_or("0")
                    .trim()
                    .parse::<u32>()
                    .map_err(|e| format!("Time64: {e}"))?;
                Ok(ColumnType::Time64(scale))
            },
            "Decimal32" => {
                let scale = inner
                    .trim()
                    .parse::<u32>()
                    .map_err(|e| format!("Decimal32: {e}"))?;
                Ok(ColumnType::Decimal(9, scale))
            },
            "Decimal64" => {
                let scale = inner
                    .trim()
                    .parse::<u32>()
                    .map_err(|e| format!("Decimal64: {e}"))?;
                Ok(ColumnType::Decimal(18, scale))
            },
            "Decimal128" => {
                let scale = inner
                    .trim()
                    .parse::<u32>()
                    .map_err(|e| format!("Decimal128: {e}"))?;
                Ok(ColumnType::Decimal(38, scale))
            },
            "Decimal256" => {
                let scale = inner
                    .trim()
                    .parse::<u32>()
                    .map_err(|e| format!("Decimal256: {e}"))?;
                Ok(ColumnType::Decimal(76, scale))
            },
            "Decimal" => {
                let parts: Vec<&str> = inner.splitn(2, ',').collect();
                if parts.len() < 2 {
                    return Err(format!("Decimal needs precision,scale: {s}"));
                }
                let precision = parts[0]
                    .trim()
                    .parse::<u32>()
                    .map_err(|e| format!("Decimal precision: {e}"))?;
                let scale = parts[1]
                    .trim()
                    .parse::<u32>()
                    .map_err(|e| format!("Decimal scale: {e}"))?;
                Ok(ColumnType::Decimal(precision, scale))
            },
            "Variant" => {
                // Variant(T1, T2, ...) — parse as tuple-like list of types
                let mut types = Vec::new();
                let mut remaining = inner;
                while !remaining.is_empty() {
                    let comma = find_top_level_comma(remaining);
                    match comma {
                        Some(pos) => {
                            types.push(parse_type(&remaining[..pos])?);
                            remaining = &remaining[pos + 1..];
                        },
                        None => {
                            types.push(parse_type(remaining)?);
                            break;
                        },
                    }
                }
                Ok(ColumnType::Variant(types))
            },
            "Object" | "object" => {
                // Object('json') — read as JSON
                Ok(ColumnType::JSON)
            },
            "AggregateFunction" => {
                // AggregateFunction(func, arg_type) — binary blob
                Ok(ColumnType::AggregateFunction)
            },
            "SimpleAggregateFunction" => Ok(ColumnType::SimpleAggregateFunction),
            // Anything else with parens that we don't know: treat as Other
            _ => Ok(ColumnType::Other(s.to_owned())),
        }
    } else {
        // Simple type without parameters
        match base {
            "UInt8" => Ok(ColumnType::UInt8),
            "UInt16" => Ok(ColumnType::UInt16),
            "UInt32" => Ok(ColumnType::UInt32),
            "UInt64" => Ok(ColumnType::UInt64),
            "UInt128" => Ok(ColumnType::UInt128),
            "UInt256" => Ok(ColumnType::UInt256),
            "Int8" => Ok(ColumnType::Int8),
            "Int16" => Ok(ColumnType::Int16),
            "Int32" => Ok(ColumnType::Int32),
            "Int64" => Ok(ColumnType::Int64),
            "Int128" => Ok(ColumnType::Int128),
            "Int256" => Ok(ColumnType::Int256),
            "Float32" => Ok(ColumnType::Float32),
            "Float64" => Ok(ColumnType::Float64),
            "Bool" => Ok(ColumnType::Bool),
            "String" => Ok(ColumnType::String),
            "JSON" | "Json" => Ok(ColumnType::JSON),
            "Dynamic" => Ok(ColumnType::Dynamic),
            "Point" => Ok(ColumnType::Point),
            "Ring" => Ok(ColumnType::Ring),
            "Polygon" => Ok(ColumnType::Polygon),
            "MultiPolygon" => Ok(ColumnType::MultiPolygon),
            "Date" => Ok(ColumnType::Date),
            "Date32" => Ok(ColumnType::Date32),
            "Nothing" => Ok(ColumnType::Nothing),
            "Time" => Ok(ColumnType::Time),
            n if n.starts_with("Time64") => {
                let s = n
                    .trim_start_matches("Time64(")
                    .trim_end_matches(')')
                    .parse::<u32>()
                    .map_err(|_| "invalid Time64")?;
                Ok(ColumnType::Time64(s))
            },
            "DateTime" => Ok(ColumnType::DateTime),
            "UUID" => Ok(ColumnType::UUID),
            "IPv4" => Ok(ColumnType::IPv4),
            "IPv6" => Ok(ColumnType::IPv6),
            _ => Ok(ColumnType::Other(s.to_owned())),
        }
    }
}

/// Find the first comma at the top level (depth 0) of a possibly-nested string.
pub fn find_top_level_comma(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => return Some(i),
            _ => {},
        }
    }
    None
}

/// Parse enum labels from a type name like `Enum8('hello' = 1, 'world' = 2)`.
/// Returns `None` if the type is not an Enum or the labels can't be parsed.
pub fn parse_enum_labels(type_name: &str) -> Option<Vec<(String, i16)>> {
    let trimmed = type_name.trim();
    let inner = trimmed
        .strip_prefix("Enum8(")
        .or_else(|| trimmed.strip_prefix("Enum16("))?;
    let inner = inner.strip_suffix(')')?.trim();
    let mut labels = Vec::new();
    let mut remaining = inner;
    while !remaining.is_empty() {
        remaining = remaining.trim().strip_prefix('\'')?;
        let quote_end = remaining.find('\'')?;
        let label = remaining[..quote_end].to_string();
        remaining = remaining[quote_end + 1..].trim();
        remaining = remaining.strip_prefix('=')?;
        remaining = remaining.trim();
        let value_end = remaining
            .find(|c: char| !c.is_ascii_digit() && c != '-')
            .unwrap_or(remaining.len());
        let value: i16 = remaining[..value_end].parse().ok()?;
        remaining = remaining[value_end..].trim();
        labels.push((label, value));
        if remaining.starts_with(',') {
            remaining = &remaining[1..];
        } else {
            break;
        }
    }
    Some(labels)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_types() {
        assert_eq!(parse_type("UInt8"), Ok(ColumnType::UInt8));
        assert_eq!(parse_type("String"), Ok(ColumnType::String));
        assert_eq!(parse_type("DateTime64(3)"), Ok(ColumnType::DateTime64(3)));
        assert_eq!(parse_type("Time64(6)"), Ok(ColumnType::Time64(6)));
        assert_eq!(parse_type("Float64"), Ok(ColumnType::Float64));
    }

    #[test]
    fn test_nested_types() {
        assert_eq!(
            parse_type("Nullable(UInt64)"),
            Ok(ColumnType::Nullable(Box::new(ColumnType::UInt64)))
        );
        assert_eq!(
            parse_type("Array(UInt8)"),
            Ok(ColumnType::Array(Box::new(ColumnType::UInt8)))
        );
        assert_eq!(
            parse_type("Map(String, UInt64)"),
            Ok(ColumnType::Map(
                Box::new(ColumnType::String),
                Box::new(ColumnType::UInt64)
            ))
        );
    }

    #[test]
    fn test_decimal_types() {
        assert_eq!(parse_type("Decimal(9, 2)"), Ok(ColumnType::Decimal(9, 2)));
        assert_eq!(parse_type("Decimal(38, 6)"), Ok(ColumnType::Decimal(38, 6)));
    }

    #[test]
    fn test_deeply_nested() {
        let result = parse_type("Array(Nullable(Map(String, Array(UInt8))))");
        assert!(
            result.is_ok(),
            "deeply nested type should parse: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_fixed_width() {
        assert_eq!(
            parse_type("UInt64")
                .expect("test operation failed")
                .fixed_width(),
            Some(8)
        );
        assert_eq!(
            parse_type("Decimal(9, 2)")
                .expect("test operation failed")
                .fixed_width(),
            Some(4)
        );
        assert_eq!(
            parse_type("Decimal(38, 6)")
                .expect("test operation failed")
                .fixed_width(),
            Some(16)
        );
        assert_eq!(
            parse_type("String")
                .expect("test operation failed")
                .fixed_width(),
            None
        );
        assert_eq!(
            parse_type("Array(UInt8)")
                .expect("test operation failed")
                .fixed_width(),
            None
        );
        assert_eq!(
            parse_type("Nullable(UInt32)")
                .expect("test operation failed")
                .fixed_width(),
            None
        );
    }

    #[test]
    fn test_parse_enum_labels_basic() {
        let labels = parse_enum_labels("Enum8('x' = 1, 'y' = 2)").expect("test operation failed");
        assert_eq!(labels, vec![("x".to_string(), 1), ("y".to_string(), 2)]);
    }

    #[test]
    fn test_parse_enum_labels_single() {
        let labels = parse_enum_labels("Enum16('only' = 42)").expect("test operation failed");
        assert_eq!(labels, vec![("only".to_string(), 42)]);
    }

    #[test]
    fn test_parse_enum_labels_non_enum_returns_none() {
        assert!(parse_enum_labels("UInt64").is_none());
        assert!(parse_enum_labels("String").is_none());
    }
}
