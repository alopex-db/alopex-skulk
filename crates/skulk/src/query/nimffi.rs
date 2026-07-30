use crate::{Result, TsmError};
use serde::de::DeserializeOwned;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::io::Cursor;
use std::slice;
use std::sync::{Once, OnceLock};

pub(crate) const MAX_QUERY_INPUT_BYTES: usize = 1 << 20;
const MAX_AST_PAYLOAD_BYTES: usize = 8 << 20;
const MAX_DIAGNOSTIC_BYTES: usize = 64 << 10;
const MAX_MESSAGEPACK_DEPTH: usize = 512;
const EXPECTED_CONTRACT_VERSION: &str = env!("SKULK_NIM_PARSER_CONTRACT_VERSION");
const INTERNAL_DEFECT_PREFIX: &str =
    "internal parser defect (this is a parser bug, not invalid SQL): ";

#[repr(C)]
#[derive(Debug)]
struct CParseResult {
    kind: c_int,
    buffer_ptr: *mut c_void,
    buffer_len: c_int,
    error_ptr: *mut c_char,
    error_len: c_int,
}

#[cfg_attr(
    target_os = "windows",
    link(name = "alopex_sql_parser", kind = "raw-dylib")
)]
unsafe extern "C" {
    fn alopex_parser_init();
    #[cfg(feature = "promql")]
    fn alopex_parse_promql(input: *const c_char, length: c_int) -> CParseResult;
    #[cfg(feature = "sql-ts")]
    fn alopex_parse_sql(input: *const c_char, length: c_int) -> CParseResult;
    fn alopex_free_buffer(pointer: *mut c_void);
    fn alopex_parser_version() -> *const c_char;
}

static INIT: Once = Once::new();
static CONTRACT_VERSION: OnceLock<std::result::Result<String, String>> = OnceLock::new();

#[derive(Debug, Clone, Copy)]
pub(crate) enum ParserLanguage {
    #[cfg(feature = "promql")]
    PromQl,
    #[cfg(feature = "sql-ts")]
    SqlTs,
}

impl ParserLanguage {
    fn name(self) -> &'static str {
        match self {
            #[cfg(feature = "promql")]
            Self::PromQl => "PromQL",
            #[cfg(feature = "sql-ts")]
            Self::SqlTs => "SQL-TS",
        }
    }
}

pub(crate) fn parse<T>(language: ParserLanguage, input: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    validate_input(input)?;
    checked_contract_version()?;

    let input = CString::new(input).map_err(|_| {
        TsmError::InvalidInput(format!(
            "{} input contains an interior NUL byte",
            language.name()
        ))
    })?;
    let length = c_int::try_from(input.as_bytes().len()).map_err(|_| {
        TsmError::ResourceLimit(format!(
            "{} input does not fit the parser ABI length field",
            language.name()
        ))
    })?;

    initialize();
    let raw = unsafe {
        match language {
            #[cfg(feature = "promql")]
            ParserLanguage::PromQl => alopex_parse_promql(input.as_ptr(), length),
            #[cfg(feature = "sql-ts")]
            ParserLanguage::SqlTs => alopex_parse_sql(input.as_ptr(), length),
        }
    };
    let result = OwnedParseResult(raw);

    match result.0.kind {
        0 => decode_payload(result.success_payload()?),
        1 => {
            let diagnostic = result.error_diagnostic()?;
            Err(diagnostic_to_error(language, diagnostic))
        }
        other => Err(TsmError::FfiContract(format!(
            "parser returned unknown result kind {other}"
        ))),
    }
}

pub(crate) fn checked_contract_version() -> Result<&'static str> {
    let checked = CONTRACT_VERSION.get_or_init(read_and_validate_contract_version);
    match checked {
        Ok(version) => Ok(version.as_str()),
        Err(message) => Err(TsmError::FfiContract(message.clone())),
    }
}

fn initialize() {
    INIT.call_once(|| unsafe {
        alopex_parser_init();
    });
}

fn read_and_validate_contract_version() -> std::result::Result<String, String> {
    initialize();
    let pointer = unsafe { alopex_parser_version() };
    if pointer.is_null() {
        return Err("alopex_parser_version returned a null pointer".to_string());
    }
    let actual = unsafe { CStr::from_ptr(pointer) }
        .to_str()
        .map_err(|error| format!("parser contract version is not UTF-8: {error}"))?
        .to_string();
    validate_contract_version(&actual)?;
    Ok(actual)
}

fn validate_contract_version(actual: &str) -> std::result::Result<(), String> {
    if actual == EXPECTED_CONTRACT_VERSION {
        Ok(())
    } else {
        Err(format!(
            "expected parser contract {EXPECTED_CONTRACT_VERSION}, got {actual}"
        ))
    }
}

fn validate_input(input: &str) -> Result<()> {
    if input.len() > MAX_QUERY_INPUT_BYTES {
        return Err(TsmError::ResourceLimit(format!(
            "query input is {} bytes; limit is {MAX_QUERY_INPUT_BYTES} bytes",
            input.len()
        )));
    }
    if input.as_bytes().contains(&0) {
        return Err(TsmError::InvalidInput(
            "query input contains an interior NUL byte".to_string(),
        ));
    }
    Ok(())
}

fn decode_payload<T>(payload: &[u8]) -> Result<T>
where
    T: DeserializeOwned,
{
    if payload.is_empty() {
        return Err(TsmError::FfiContract(
            "parser returned an empty MessagePack payload".to_string(),
        ));
    }
    if payload.len() > MAX_AST_PAYLOAD_BYTES {
        return Err(TsmError::ResourceLimit(format!(
            "parser AST payload is {} bytes; limit is {MAX_AST_PAYLOAD_BYTES} bytes",
            payload.len()
        )));
    }

    let mut decoder = rmp_serde::Deserializer::new(Cursor::new(payload));
    decoder.set_max_depth(MAX_MESSAGEPACK_DEPTH);
    let value = T::deserialize(&mut decoder).map_err(|error| {
        TsmError::FfiContract(format!("invalid MessagePack parser payload: {error}"))
    })?;
    let consumed = usize::try_from(decoder.position()).map_err(|_| {
        TsmError::FfiContract("MessagePack decoder position overflowed usize".to_string())
    })?;
    if consumed != payload.len() {
        return Err(TsmError::FfiContract(format!(
            "MessagePack parser payload has {} trailing bytes",
            payload.len() - consumed
        )));
    }
    Ok(value)
}

struct OwnedParseResult(CParseResult);

impl OwnedParseResult {
    fn success_payload(&self) -> Result<&[u8]> {
        if !self.0.error_ptr.is_null() || self.0.error_len != 0 {
            return Err(TsmError::FfiContract(
                "successful parser result also contained an error buffer".to_string(),
            ));
        }
        self.slot(
            self.0.buffer_ptr,
            self.0.buffer_len,
            MAX_AST_PAYLOAD_BYTES,
            "AST",
        )
    }

    fn error_diagnostic(&self) -> Result<&str> {
        if !self.0.buffer_ptr.is_null() || self.0.buffer_len != 0 {
            return Err(TsmError::FfiContract(
                "error parser result also contained an AST buffer".to_string(),
            ));
        }
        let bytes = self.slot(
            self.0.error_ptr.cast(),
            self.0.error_len,
            MAX_DIAGNOSTIC_BYTES,
            "diagnostic",
        )?;
        if bytes.is_empty() {
            return Err(TsmError::FfiContract(
                "parser returned an empty error diagnostic".to_string(),
            ));
        }
        std::str::from_utf8(bytes).map_err(|error| {
            TsmError::FfiContract(format!("parser diagnostic is not UTF-8: {error}"))
        })
    }

    fn slot(
        &self,
        pointer: *mut c_void,
        length: c_int,
        maximum: usize,
        name: &str,
    ) -> Result<&[u8]> {
        let length = usize::try_from(length).map_err(|_| {
            TsmError::FfiContract(format!("parser returned a negative {name} buffer length"))
        })?;
        if length == 0 {
            return if pointer.is_null() {
                Ok(&[])
            } else {
                Err(TsmError::FfiContract(format!(
                    "parser returned a non-null {name} buffer with zero length"
                )))
            };
        }
        if pointer.is_null() {
            return Err(TsmError::FfiContract(format!(
                "parser returned a null {name} buffer with length {length}"
            )));
        }
        if length > maximum {
            return Err(TsmError::ResourceLimit(format!(
                "parser {name} buffer is {length} bytes; limit is {maximum} bytes"
            )));
        }
        Ok(unsafe { slice::from_raw_parts(pointer.cast::<u8>(), length) })
    }
}

impl Drop for OwnedParseResult {
    fn drop(&mut self) {
        let buffer = self.0.buffer_ptr;
        let error = self.0.error_ptr.cast::<c_void>();
        if !buffer.is_null() {
            unsafe { alopex_free_buffer(buffer) };
        }
        if !error.is_null() && error != buffer {
            unsafe { alopex_free_buffer(error) };
        }
    }
}

fn diagnostic_to_error(language: ParserLanguage, message: &str) -> TsmError {
    if let Some(defect) = message.strip_prefix(INTERNAL_DEFECT_PREFIX) {
        return TsmError::FfiContract(format!("parser internal defect: {defect}"));
    }
    let (line, column, offset) = parse_diagnostic_position(message).unwrap_or((0, 0, 0));
    TsmError::Parse {
        language: language.name().to_string(),
        message: message.to_string(),
        line,
        column,
        offset,
    }
}

fn parse_diagnostic_position(message: &str) -> Option<(usize, usize, usize)> {
    if let Some(rest) = message.strip_prefix("PromQL parse error at line ") {
        let (line, rest) = rest.split_once(", col ")?;
        let (column, rest) = rest.split_once(", offset ")?;
        let (offset, _) = rest.split_once(" near ")?;
        return Some((
            line.parse().ok()?,
            column.parse().ok()?,
            offset.parse().ok()?,
        ));
    }

    let rest = message.strip_prefix("Parse error at line ")?;
    let (line, rest) = rest.split_once(", col ")?;
    let (column, _) = rest.split_once(':')?;
    Some((line.parse().ok()?, column.parse().ok()?, 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct Sample {
        value: u64,
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct Nested(Vec<Nested>);

    #[test]
    fn input_validation_runs_without_ffi_or_panics() {
        assert!(matches!(
            validate_input(&"x".repeat(MAX_QUERY_INPUT_BYTES + 1)),
            Err(TsmError::ResourceLimit(_))
        ));
        assert!(matches!(
            validate_input("x\0y"),
            Err(TsmError::InvalidInput(_))
        ));
    }

    #[test]
    fn contract_version_mismatch_is_explicit() {
        assert!(validate_contract_version(EXPECTED_CONTRACT_VERSION).is_ok());
        assert_eq!(
            validate_contract_version("9.9.9").unwrap_err(),
            "expected parser contract 0.2.0, got 9.9.9"
        );
    }

    #[test]
    fn malformed_and_trailing_messagepack_are_contract_errors() {
        assert!(matches!(
            decode_payload::<Sample>(&[0xc1]),
            Err(TsmError::FfiContract(_))
        ));

        let mut payload = rmp_serde::to_vec_named(&Sample { value: 7 }).unwrap();
        payload.push(0);
        assert!(matches!(
            decode_payload::<Sample>(&payload),
            Err(TsmError::FfiContract(_))
        ));
    }

    #[test]
    fn messagepack_depth_and_result_buffer_shapes_are_bounded() {
        let mut nested = Nested(Vec::new());
        for _ in 0..600 {
            nested = Nested(vec![nested]);
        }
        let payload = rmp_serde::to_vec_named(&nested).unwrap();
        assert!(matches!(
            decode_payload::<Nested>(&payload),
            Err(TsmError::FfiContract(_))
        ));

        let negative_length = OwnedParseResult(CParseResult {
            kind: 0,
            buffer_ptr: std::ptr::null_mut(),
            buffer_len: -1,
            error_ptr: std::ptr::null_mut(),
            error_len: 0,
        });
        assert!(matches!(
            negative_length.success_payload(),
            Err(TsmError::FfiContract(_))
        ));

        let empty_error = OwnedParseResult(CParseResult {
            kind: 1,
            buffer_ptr: std::ptr::null_mut(),
            buffer_len: 0,
            error_ptr: std::ptr::null_mut(),
            error_len: 0,
        });
        assert!(matches!(
            empty_error.error_diagnostic(),
            Err(TsmError::FfiContract(_))
        ));
    }

    #[test]
    fn parser_diagnostic_positions_are_extracted() {
        assert_eq!(
            parse_diagnostic_position(
                "PromQL parse error at line 2, col 4, offset 9 near '}': expected expression"
            ),
            Some((2, 4, 9))
        );
        assert_eq!(
            parse_diagnostic_position("Parse error at line 3, col 5: expected SELECT"),
            Some((3, 5, 0))
        );
    }
}
