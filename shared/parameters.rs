// Query parameter serialization for ClickHouse native protocol.
//
// ClickHouse server-side parameters (protocol revision 54459+) are sent
// after the query text as setting-like entries:
// `name`, flags=`CUSTOM`, quoted value, then an empty-name terminator.

use super::wire;

const QUERY_PARAMETER_CUSTOM_FLAG: u64 = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryParameter {
    pub name: String,
    pub value: Option<String>,
}

impl QueryParameter {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
        }
    }

    pub fn null(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: None,
        }
    }
}

#[inline]
pub(crate) fn write_query_parameters_to_vec(buf: &mut Vec<u8>, params: &[QueryParameter]) {
    for param in params {
        wire::write_string_to_vec(buf, &param.name);
        wire::write_varint_to_vec(buf, QUERY_PARAMETER_CUSTOM_FLAG);
        match &param.value {
            Some(value) => write_quoted_string_to_vec(buf, value.as_bytes()),
            None => write_param_null_to_vec(buf),
        }
    }
    wire::write_string_to_vec(buf, "");
}

#[inline]
pub(crate) fn query_parameters_capacity(params: &[QueryParameter]) -> usize {
    if params.is_empty() {
        return 1;
    }
    params
        .iter()
        .map(|p| p.name.len() + p.value.as_ref().map_or(5, |v| quoted_len(v.as_bytes())) + 8)
        .sum::<usize>()
        + 1
}

fn write_quoted_string_to_vec(buf: &mut Vec<u8>, value: &[u8]) {
    wire::write_varint_to_vec(buf, quoted_len(value) as u64);
    buf.push(b'\'');
    for &byte in value {
        match byte {
            0 => buf.extend_from_slice(br"\x00"),
            8 => buf.extend_from_slice(br"\x08"),
            b'\t' => buf.extend_from_slice(br"\\\t"),
            b'\n' => buf.extend_from_slice(br"\\\n"),
            b'\'' => buf.extend_from_slice(br"\x27"),
            b'\\' => buf.extend_from_slice(br"\\\\"),
            _ => buf.push(byte),
        }
    }
    buf.push(b'\'');
}

fn write_param_null_to_vec(buf: &mut Vec<u8>) {
    wire::write_string_to_vec(buf, r"'\\N'");
}

fn quoted_len(value: &[u8]) -> usize {
    value
        .iter()
        .map(|byte| match byte {
            0 | 8 | b'\t' | b'\n' | b'\'' | b'\\' => 4,
            _ => 1,
        })
        .sum::<usize>()
        + 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoted_string_matches_clickhouse_cpp_shape() {
        let mut buf = Vec::new();
        write_quoted_string_to_vec(&mut buf, b"a\0b\tc\nd'e\\f");
        let s = wire::read_string(&mut &buf[..]).expect("quoted string");
        assert_eq!(s, r"'a\x00b\\\tc\\\nd\x27e\\\\f'");
    }

    #[test]
    fn serializes_query_parameters_with_custom_flag_and_terminator() {
        let mut buf = Vec::new();
        write_query_parameters_to_vec(
            &mut buf,
            &[
                QueryParameter::new("id", "42"),
                QueryParameter::null("name"),
            ],
        );
        let mut rd = &buf[..];
        assert_eq!(wire::read_string(&mut rd).expect("name"), "id");
        assert_eq!(wire::read_varint(&mut rd).expect("flag"), 2);
        assert_eq!(wire::read_string(&mut rd).expect("value"), "'42'");
        assert_eq!(wire::read_string(&mut rd).expect("name"), "name");
        assert_eq!(wire::read_varint(&mut rd).expect("flag"), 2);
        assert_eq!(wire::read_string(&mut rd).expect("value"), r"'\\N'");
        assert_eq!(wire::read_string(&mut rd).expect("terminator"), "");
        assert!(rd.is_empty());
    }
}
