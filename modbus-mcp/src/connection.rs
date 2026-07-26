use chrono::{SecondsFormat, Utc};
use fieldworks_adapter_core::*;
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashMap, time::Instant};
use tokio_modbus::Slave;

// ── Topology config ───────────────────────────────────────────────────────────
//
// Loaded from topology.yaml at connect time. Structurally identical to
// mqtt-mcp's topology config — protocol-agnostic, reused as-is.
//
// Example topology.yaml:
//
//   tags:
//     - tag_id: "holding:100:float32"
//       name: "Pump 01 Flow Rate"
//       description: "Inlet pump volumetric flow"
//       units: "m3/h"
//       data_type: "float"
//       writable: false
//       process_area: "raw_water"
//       equipment_id: "pump_01"
//       normal_range:
//         min: 0.0
//         max: 500.0
//
//   write_permissions:
//     "holding:200":
//       min: 0.0
//       max: 60.0
//       units: "Hz"

#[derive(Debug, Deserialize)]
pub struct TopologyTag {
    pub tag_id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub units: String,
    #[serde(default = "default_data_type")]
    pub data_type: String,
    #[serde(default)]
    pub writable: bool,
    #[serde(default)]
    pub process_area: String,
    #[serde(default)]
    pub equipment_id: String,
    pub normal_range: Option<TopologyRange>,
}

fn default_data_type() -> String {
    "uint16".to_string()
}

#[derive(Debug, Deserialize)]
pub struct TopologyRange {
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct WritePermission {
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub units: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct TopologyConfig {
    #[serde(default)]
    pub tags: Vec<TopologyTag>,
    #[serde(default)]
    pub write_permissions: HashMap<String, WritePermission>,
}

pub fn load_topology() -> TopologyConfig {
    let candidates = ["topology.yaml", "topology.yml", "../topology.yaml"];
    for path in &candidates {
        if let Ok(content) = std::fs::read_to_string(path) {
            match serde_yaml::from_str::<TopologyConfig>(&content) {
                Ok(config) => {
                    tracing::info!("loaded topology from {path}");
                    return config;
                }
                Err(e) => tracing::warn!("failed to parse {path}: {e}"),
            }
        }
    }
    tracing::warn!("no topology.yaml found — discover_tags returns no tags; all writes denied");
    TopologyConfig::default()
}

// ── tag_id parsing ────────────────────────────────────────────────────────────
//
// Modbus has no natural string tag identifier (unlike MQTT topics or OPC-UA
// NodeIds), so tag_id encodes register type + address + optional data type:
//
//   "{register_type}:{address}[:{data_type}]"
//     register_type ∈ {holding, input, coil, discrete}
//     data_type     ∈ {uint16, int16, float32, int32} — holding/input only,
//                      defaults to uint16 if omitted. coil/discrete are
//                      inherently boolean and must not carry a data_type.
//
//   examples: "holding:100", "holding:100:float32", "coil:5"

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Uint16,
    Int16,
    Float32,
    Int32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParsedTag {
    Holding { address: u16, data_type: DataType },
    Input { address: u16, data_type: DataType },
    Coil { address: u16 },
    Discrete { address: u16 },
}

impl ParsedTag {
    pub fn address(&self) -> u16 {
        match self {
            ParsedTag::Holding { address, .. }
            | ParsedTag::Input { address, .. }
            | ParsedTag::Coil { address }
            | ParsedTag::Discrete { address } => *address,
        }
    }

    /// Coils and discrete inputs are boolean-only; holding/input registers
    /// carry the parsed numeric data_type.
    pub fn register_type_name(&self) -> &'static str {
        match self {
            ParsedTag::Holding { .. } => "holding",
            ParsedTag::Input { .. } => "input",
            ParsedTag::Coil { .. } => "coil",
            ParsedTag::Discrete { .. } => "discrete",
        }
    }

    /// Input registers and discrete inputs are read-only in the Modbus spec —
    /// there is no write function code for either. Coils and holding
    /// registers are the only writable register types.
    pub fn is_writable_type(&self) -> bool {
        matches!(self, ParsedTag::Holding { .. } | ParsedTag::Coil { .. })
    }
}

pub fn parse_tag_id(tag_id: &str) -> Result<ParsedTag, AdapterError> {
    let parts: Vec<&str> = tag_id.split(':').collect();
    let bad_tag = |msg: String| AdapterError {
        code: ErrorCode::TagNotFound,
        message: msg,
        tag_id: Some(tag_id.to_string()),
    };

    if parts.len() < 2 || parts.len() > 3 {
        return Err(bad_tag(format!(
            "'{tag_id}' is not a valid tag_id. Expected \"register_type:address[:data_type]\"."
        )));
    }

    let address: u16 = parts[1].parse().map_err(|_| {
        bad_tag(format!(
            "'{}' is not a valid Modbus register address (0-65535).",
            parts[1]
        ))
    })?;

    match parts[0] {
        "coil" | "discrete" => {
            if parts.len() == 3 {
                return Err(bad_tag(format!(
                    "'{}' registers are boolean and must not carry a data_type suffix.",
                    parts[0]
                )));
            }
            if parts[0] == "coil" {
                Ok(ParsedTag::Coil { address })
            } else {
                Ok(ParsedTag::Discrete { address })
            }
        }
        "holding" | "input" => {
            let data_type = match parts.get(2) {
                None => DataType::Uint16,
                Some(&"uint16") => DataType::Uint16,
                Some(&"int16") => DataType::Int16,
                Some(&"float32") => DataType::Float32,
                Some(&"int32") => DataType::Int32,
                Some(other) => {
                    return Err(bad_tag(format!(
                        "'{other}' is not a valid data_type. Expected one of: uint16, int16, float32, int32."
                    )));
                }
            };
            if parts[0] == "holding" {
                Ok(ParsedTag::Holding { address, data_type })
            } else {
                Ok(ParsedTag::Input { address, data_type })
            }
        }
        other => Err(bad_tag(format!(
            "'{other}' is not a valid register type. Expected one of: holding, input, coil, discrete."
        ))),
    }
}

// ── Register ↔ value conversion ───────────────────────────────────────────────
//
// tokio-modbus returns/accepts flat Vec<u16> with no multi-register decoding.
// float32/int32 span 2 consecutive registers, high word first (big-endian
// word order — the common PLC convention; devices using word-swapped/
// little-endian layouts are not supported by this v1).

pub fn registers_to_value(words: &[u16], data_type: DataType) -> Result<TagValue, AdapterError> {
    match data_type {
        DataType::Uint16 => Ok(TagValue::Float(expect_words::<1>(words)?[0] as f64)),
        DataType::Int16 => {
            let w = expect_words::<1>(words)?[0];
            Ok(TagValue::Float((w as i16) as f64))
        }
        DataType::Float32 => {
            let [hi, lo] = expect_words::<2>(words)?;
            Ok(TagValue::Float(
                f32::from_be_bytes(be_bytes_from_words(hi, lo)) as f64,
            ))
        }
        DataType::Int32 => {
            let [hi, lo] = expect_words::<2>(words)?;
            Ok(TagValue::Float(
                i32::from_be_bytes(be_bytes_from_words(hi, lo)) as f64,
            ))
        }
    }
}

pub fn value_to_registers(
    value: &serde_json::Value,
    data_type: DataType,
    tag_id: &str,
) -> Result<Vec<u16>, AdapterError> {
    let invalid = |msg: String| AdapterError {
        code: ErrorCode::InvalidValue,
        message: msg,
        tag_id: Some(tag_id.to_string()),
    };

    let f = value
        .as_f64()
        .ok_or_else(|| invalid("write_tag value must be a number for register writes.".into()))?;

    match data_type {
        DataType::Uint16 => {
            if f.fract() != 0.0 || !(0.0..=u16::MAX as f64).contains(&f) {
                return Err(invalid(format!(
                    "value {f} is out of range for uint16 (0-65535)."
                )));
            }
            Ok(vec![f as u16])
        }
        DataType::Int16 => {
            if f.fract() != 0.0 || !((i16::MIN as f64)..=(i16::MAX as f64)).contains(&f) {
                return Err(invalid(format!(
                    "value {f} is out of range for int16 ({}..={}).",
                    i16::MIN,
                    i16::MAX
                )));
            }
            Ok(vec![(f as i16) as u16])
        }
        DataType::Float32 => {
            let bytes = (f as f32).to_be_bytes();
            Ok(words_from_be_bytes(bytes))
        }
        DataType::Int32 => {
            if f.fract() != 0.0 || !((i32::MIN as f64)..=(i32::MAX as f64)).contains(&f) {
                return Err(invalid(format!("value {f} is out of range for int32.")));
            }
            let bytes = (f as i32).to_be_bytes();
            Ok(words_from_be_bytes(bytes))
        }
    }
}

fn be_bytes_from_words(hi: u16, lo: u16) -> [u8; 4] {
    let h = hi.to_be_bytes();
    let l = lo.to_be_bytes();
    [h[0], h[1], l[0], l[1]]
}

fn words_from_be_bytes(bytes: [u8; 4]) -> Vec<u16> {
    vec![
        u16::from_be_bytes([bytes[0], bytes[1]]),
        u16::from_be_bytes([bytes[2], bytes[3]]),
    ]
}

fn expect_words<const N: usize>(words: &[u16]) -> Result<[u16; N], AdapterError> {
    if words.len() < N {
        return Err(AdapterError {
            code: ErrorCode::ConnectionError,
            message: format!(
                "expected {N} register(s) in device response, got {}",
                words.len()
            ),
            tag_id: None,
        });
    }
    let mut out = [0u16; N];
    out.copy_from_slice(&words[..N]);
    Ok(out)
}

// ── Error mapping ─────────────────────────────────────────────────────────────

pub fn map_transport_error(err: &tokio_modbus::Error, tag_id: &str) -> AdapterError {
    AdapterError {
        code: ErrorCode::ConnectionError,
        message: format!("Modbus transport/protocol error for '{tag_id}': {err}"),
        tag_id: Some(tag_id.to_string()),
    }
}

pub fn map_exception_code(
    code: tokio_modbus::ExceptionCode,
    tag_id: &str,
    is_write: bool,
) -> AdapterError {
    use tokio_modbus::ExceptionCode::*;
    let mapped = match code {
        IllegalDataAddress => ErrorCode::TagNotFound,
        IllegalFunction if is_write => ErrorCode::TagNotWritable,
        IllegalFunction => ErrorCode::ConnectionError,
        IllegalDataValue => ErrorCode::InvalidValue,
        _ => ErrorCode::ConnectionError,
    };
    AdapterError {
        code: mapped,
        message: format!("Modbus device returned exception {code:?} for '{tag_id}'."),
        tag_id: Some(tag_id.to_string()),
    }
}

/// Number of consecutive registers a data_type spans. uint16/int16 are one
/// register; float32/int32 span two (see registers_to_value doc comment).
pub fn register_count(data_type: DataType) -> u16 {
    match data_type {
        DataType::Uint16 | DataType::Int16 => 1,
        DataType::Float32 | DataType::Int32 => 2,
    }
}

pub fn topology_tag_to_descriptor(t: &TopologyTag) -> TagDescriptor {
    TagDescriptor {
        tag_id: t.tag_id.clone(),
        name: t.name.clone(),
        description: t.description.clone(),
        units: t.units.clone(),
        data_type: t.data_type.clone(),
        writable: t.writable,
        process_area: t.process_area.clone(),
        equipment_id: t.equipment_id.clone(),
        normal_range: t.normal_range.as_ref().map(|r| NormalRange {
            min: r.min,
            max: r.max,
        }),
    }
}

pub fn tag_units(topology: &TopologyConfig, tag_id: &str) -> String {
    topology
        .tags
        .iter()
        .find(|t| t.tag_id == tag_id)
        .map(|t| t.units.clone())
        .unwrap_or_default()
}

// ── Active connection ─────────────────────────────────────────────────────────

pub struct ModbusConnection {
    pub ctx: tokio_modbus::client::Context,
    pub host: String,
    pub port: u16,
    pub slave: Slave,
    pub connected_at: Instant,
    pub last_error: Option<String>,
    pub topology: TopologyConfig,
}

// ── Tag tree builder ──────────────────────────────────────────────────────────

/// Group topology tags by register type. Purely topology-derived, no live I/O
/// — Modbus addresses aren't hierarchical like MQTT topics or OPC-UA browse
/// paths, so a flat per-register-type grouping is more useful than a nested tree.
pub fn build_tag_tree(tags: &[TopologyTag]) -> serde_json::Value {
    let mut holding = Vec::new();
    let mut input = Vec::new();
    let mut coil = Vec::new();
    let mut discrete = Vec::new();

    for t in tags {
        let entry = json!({
            "tag_id": t.tag_id,
            "name": t.name,
            "units": t.units,
            "writable": t.writable,
        });
        match parse_tag_id(&t.tag_id) {
            Ok(ParsedTag::Holding { .. }) => holding.push(entry),
            Ok(ParsedTag::Input { .. }) => input.push(entry),
            Ok(ParsedTag::Coil { .. }) => coil.push(entry),
            Ok(ParsedTag::Discrete { .. }) => discrete.push(entry),
            Err(e) => tracing::warn!("skipping malformed topology tag_id: {}", e.message),
        }
    }

    json!({
        "holding": holding,
        "input": input,
        "coil": coil,
        "discrete": discrete,
    })
}

// ── Write validation (reused from mqtt-mcp verbatim) ──────────────────────────

pub fn validate_write(
    perm: &WritePermission,
    value: &serde_json::Value,
    units: &str,
    tag_id: &str,
) -> Result<WriteValue, AdapterError> {
    let write_value = if let Some(f) = value.as_f64() {
        if let Some(min) = perm.min {
            if f < min {
                return Err(AdapterError {
                    code: ErrorCode::InvalidValue,
                    message: format!(
                        "Value {f} is below the configured minimum {min} for '{tag_id}'."
                    ),
                    tag_id: Some(tag_id.to_string()),
                });
            }
        }
        if let Some(max) = perm.max {
            if f > max {
                return Err(AdapterError {
                    code: ErrorCode::InvalidValue,
                    message: format!(
                        "Value {f} exceeds the configured maximum {max} for '{tag_id}'."
                    ),
                    tag_id: Some(tag_id.to_string()),
                });
            }
        }
        WriteValue::Float(f)
    } else if let Some(b) = value.as_bool() {
        WriteValue::Bool(b)
    } else {
        return Err(AdapterError {
            code: ErrorCode::InvalidValue,
            message: "write_tag value must be a number or boolean. String values are not writable."
                .into(),
            tag_id: Some(tag_id.to_string()),
        });
    };

    if let Some(ref configured_units) = perm.units {
        if !configured_units.is_empty() && configured_units != units {
            return Err(AdapterError {
                code: ErrorCode::InvalidValue,
                message: format!(
                    "Units mismatch for '{tag_id}': expected '{configured_units}', got '{units}'."
                ),
                tag_id: Some(tag_id.to_string()),
            });
        }
    }

    Ok(write_value)
}

// ── Audit log (identical to mqtt-mcp) ─────────────────────────────────────────

pub fn log_write_audit(
    tag_id: &str,
    value: &serde_json::Value,
    units: &str,
    operator_id: &str,
    reason: &str,
) {
    use std::io::Write;
    let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let entry = json!({
        "timestamp": timestamp,
        "tag_id": tag_id,
        "value": value,
        "units": units,
        "operator_id": operator_id,
        "reason": reason,
    });
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("write_audit.jsonl")
    {
        Ok(mut f) => {
            let _ = writeln!(f, "{entry}");
        }
        Err(e) => tracing::error!("failed to write audit log: {e}"),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_tag_id ──────────────────────────────────────────────────────────

    #[test]
    fn parse_holding_default_uint16() {
        let t = parse_tag_id("holding:100").unwrap();
        assert_eq!(
            t,
            ParsedTag::Holding {
                address: 100,
                data_type: DataType::Uint16
            }
        );
    }

    #[test]
    fn parse_holding_float32() {
        let t = parse_tag_id("holding:100:float32").unwrap();
        assert_eq!(
            t,
            ParsedTag::Holding {
                address: 100,
                data_type: DataType::Float32
            }
        );
    }

    #[test]
    fn parse_input_int16() {
        let t = parse_tag_id("input:5:int16").unwrap();
        assert_eq!(
            t,
            ParsedTag::Input {
                address: 5,
                data_type: DataType::Int16
            }
        );
    }

    #[test]
    fn parse_input_int32() {
        let t = parse_tag_id("input:5:int32").unwrap();
        assert_eq!(
            t,
            ParsedTag::Input {
                address: 5,
                data_type: DataType::Int32
            }
        );
    }

    #[test]
    fn parse_coil() {
        assert_eq!(
            parse_tag_id("coil:5").unwrap(),
            ParsedTag::Coil { address: 5 }
        );
    }

    #[test]
    fn parse_discrete() {
        assert_eq!(
            parse_tag_id("discrete:3").unwrap(),
            ParsedTag::Discrete { address: 3 }
        );
    }

    #[test]
    fn parse_coil_rejects_data_type_suffix() {
        let err = parse_tag_id("coil:5:uint16").unwrap_err();
        assert_eq!(err.code, ErrorCode::TagNotFound);
        assert!(err.message.contains("boolean"));
    }

    #[test]
    fn parse_discrete_rejects_data_type_suffix() {
        let err = parse_tag_id("discrete:5:uint16").unwrap_err();
        assert_eq!(err.code, ErrorCode::TagNotFound);
    }

    #[test]
    fn parse_unknown_register_type() {
        let err = parse_tag_id("bogus:5").unwrap_err();
        assert_eq!(err.code, ErrorCode::TagNotFound);
        assert!(err.message.contains("register type"));
    }

    #[test]
    fn parse_unknown_data_type() {
        let err = parse_tag_id("holding:100:bogus").unwrap_err();
        assert_eq!(err.code, ErrorCode::TagNotFound);
        assert!(err.message.contains("data_type"));
    }

    #[test]
    fn parse_non_numeric_address() {
        let err = parse_tag_id("holding:abc").unwrap_err();
        assert_eq!(err.code, ErrorCode::TagNotFound);
    }

    #[test]
    fn parse_address_out_of_u16_range() {
        assert!(parse_tag_id("holding:99999").is_err());
    }

    #[test]
    fn parse_missing_address() {
        assert!(parse_tag_id("holding").is_err());
    }

    #[test]
    fn parse_too_many_parts() {
        assert!(parse_tag_id("holding:100:float32:extra").is_err());
    }

    #[test]
    fn parse_empty_string() {
        assert!(parse_tag_id("").is_err());
    }

    #[test]
    fn parsed_tag_is_writable_type() {
        assert!(ParsedTag::Holding {
            address: 0,
            data_type: DataType::Uint16
        }
        .is_writable_type());
        assert!(ParsedTag::Coil { address: 0 }.is_writable_type());
        assert!(!ParsedTag::Input {
            address: 0,
            data_type: DataType::Uint16
        }
        .is_writable_type());
        assert!(!ParsedTag::Discrete { address: 0 }.is_writable_type());
    }

    // ── registers_to_value / value_to_registers round trips ──────────────────

    #[test]
    fn uint16_round_trip() {
        let regs = value_to_registers(&json!(1234.0), DataType::Uint16, "t").unwrap();
        let v = registers_to_value(&regs, DataType::Uint16).unwrap();
        assert!(matches!(v, TagValue::Float(f) if (f - 1234.0).abs() < 1e-9));
    }

    #[test]
    fn int16_round_trip_negative() {
        let regs = value_to_registers(&json!(-42.0), DataType::Int16, "t").unwrap();
        let v = registers_to_value(&regs, DataType::Int16).unwrap();
        assert!(matches!(v, TagValue::Float(f) if (f - (-42.0)).abs() < 1e-9));
    }

    #[test]
    fn float32_round_trip() {
        let regs = value_to_registers(&json!(312.7_f32 as f64), DataType::Float32, "t").unwrap();
        assert_eq!(regs.len(), 2);
        let v = registers_to_value(&regs, DataType::Float32).unwrap();
        assert!(matches!(v, TagValue::Float(f) if (f - 312.7).abs() < 1e-3));
    }

    #[test]
    fn int32_round_trip_negative() {
        let regs = value_to_registers(&json!(-100000.0), DataType::Int32, "t").unwrap();
        assert_eq!(regs.len(), 2);
        let v = registers_to_value(&regs, DataType::Int32).unwrap();
        assert!(matches!(v, TagValue::Float(f) if (f - (-100000.0)).abs() < 1e-9));
    }

    #[test]
    fn float32_word_order_is_high_word_first() {
        // 1.0f32 = 0x3F800000 → hi word 0x3F80, lo word 0x0000
        let regs = value_to_registers(&json!(1.0), DataType::Float32, "t").unwrap();
        assert_eq!(regs, vec![0x3F80, 0x0000]);
    }

    #[test]
    fn uint16_out_of_range_rejected() {
        let err = value_to_registers(&json!(70000.0), DataType::Uint16, "t").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidValue);
    }

    #[test]
    fn uint16_negative_rejected() {
        let err = value_to_registers(&json!(-1.0), DataType::Uint16, "t").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidValue);
    }

    #[test]
    fn int16_out_of_range_rejected() {
        let err = value_to_registers(&json!(40000.0), DataType::Int16, "t").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidValue);
    }

    #[test]
    fn non_numeric_value_rejected() {
        let err = value_to_registers(&json!("bad"), DataType::Uint16, "t").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidValue);
    }

    #[test]
    fn fractional_uint16_rejected() {
        let err = value_to_registers(&json!(1.5), DataType::Uint16, "t").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidValue);
    }

    #[test]
    fn registers_to_value_short_response_errors() {
        let err = registers_to_value(&[], DataType::Uint16).unwrap_err();
        assert_eq!(err.code, ErrorCode::ConnectionError);
        let err = registers_to_value(&[1], DataType::Float32).unwrap_err();
        assert_eq!(err.code, ErrorCode::ConnectionError);
    }

    // ── error mapping ─────────────────────────────────────────────────────────

    #[test]
    fn exception_illegal_data_address_is_tag_not_found() {
        let err = map_exception_code(tokio_modbus::ExceptionCode::IllegalDataAddress, "t", false);
        assert_eq!(err.code, ErrorCode::TagNotFound);
    }

    #[test]
    fn exception_illegal_function_on_write_is_tag_not_writable() {
        let err = map_exception_code(tokio_modbus::ExceptionCode::IllegalFunction, "t", true);
        assert_eq!(err.code, ErrorCode::TagNotWritable);
    }

    #[test]
    fn exception_illegal_function_on_read_is_connection_error() {
        let err = map_exception_code(tokio_modbus::ExceptionCode::IllegalFunction, "t", false);
        assert_eq!(err.code, ErrorCode::ConnectionError);
    }

    #[test]
    fn exception_illegal_data_value_is_invalid_value() {
        let err = map_exception_code(tokio_modbus::ExceptionCode::IllegalDataValue, "t", true);
        assert_eq!(err.code, ErrorCode::InvalidValue);
    }

    #[test]
    fn exception_device_busy_is_connection_error() {
        let err = map_exception_code(tokio_modbus::ExceptionCode::ServerDeviceBusy, "t", false);
        assert_eq!(err.code, ErrorCode::ConnectionError);
    }

    #[test]
    fn exception_error_includes_tag_id() {
        let err = map_exception_code(
            tokio_modbus::ExceptionCode::IllegalDataAddress,
            "holding:5",
            false,
        );
        assert_eq!(err.tag_id.as_deref(), Some("holding:5"));
    }

    // ── build_tag_tree ────────────────────────────────────────────────────────

    fn tag(tag_id: &str) -> TopologyTag {
        TopologyTag {
            tag_id: tag_id.to_string(),
            name: tag_id.to_string(),
            description: String::new(),
            units: String::new(),
            data_type: "uint16".to_string(),
            writable: false,
            process_area: String::new(),
            equipment_id: String::new(),
            normal_range: None,
        }
    }

    #[test]
    fn tag_tree_groups_by_register_type() {
        let tags = vec![
            tag("holding:100"),
            tag("input:5"),
            tag("coil:1"),
            tag("discrete:2"),
        ];
        let tree = build_tag_tree(&tags);
        assert_eq!(tree["holding"].as_array().unwrap().len(), 1);
        assert_eq!(tree["input"].as_array().unwrap().len(), 1);
        assert_eq!(tree["coil"].as_array().unwrap().len(), 1);
        assert_eq!(tree["discrete"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn tag_tree_skips_malformed_tag_id() {
        let tags = vec![tag("holding:100"), tag("not-a-valid-tag")];
        let tree = build_tag_tree(&tags);
        assert_eq!(tree["holding"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn tag_tree_empty_input() {
        let tree = build_tag_tree(&[]);
        assert!(tree["holding"].as_array().unwrap().is_empty());
        assert!(tree["coil"].as_array().unwrap().is_empty());
    }

    // ── validate_write ────────────────────────────────────────────────────────

    fn perm(min: Option<f64>, max: Option<f64>, units: Option<&str>) -> WritePermission {
        WritePermission {
            min,
            max,
            units: units.map(String::from),
        }
    }

    #[test]
    fn validate_write_float_ok() {
        let p = perm(Some(0.0), Some(100.0), Some("Hz"));
        let result = validate_write(&p, &json!(50.0), "Hz", "tag");
        assert!(matches!(result, Ok(WriteValue::Float(f)) if (f - 50.0).abs() < 1e-9));
    }

    #[test]
    fn validate_write_bool_ok() {
        let p = perm(None, None, None);
        let result = validate_write(&p, &json!(true), "", "tag");
        assert!(matches!(result, Ok(WriteValue::Bool(true))));
    }

    #[test]
    fn validate_write_below_min() {
        let p = perm(Some(10.0), Some(100.0), None);
        let err = validate_write(&p, &json!(5.0), "", "tag").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidValue);
        assert!(err.message.contains("minimum"));
    }

    #[test]
    fn validate_write_above_max() {
        let p = perm(Some(0.0), Some(60.0), None);
        let err = validate_write(&p, &json!(61.0), "", "tag").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidValue);
        assert!(err.message.contains("maximum"));
    }

    #[test]
    fn validate_write_units_mismatch() {
        let p = perm(None, None, Some("Hz"));
        let err = validate_write(&p, &json!(50.0), "rpm", "tag").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidValue);
    }

    #[test]
    fn validate_write_string_rejected() {
        let p = perm(None, None, None);
        let err = validate_write(&p, &json!("bad"), "", "tag").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidValue);
    }

    // ── register_count / tag_units ────────────────────────────────────────────

    #[test]
    fn register_count_matches_data_type_width() {
        assert_eq!(register_count(DataType::Uint16), 1);
        assert_eq!(register_count(DataType::Int16), 1);
        assert_eq!(register_count(DataType::Float32), 2);
        assert_eq!(register_count(DataType::Int32), 2);
    }

    #[test]
    fn tag_units_finds_matching_tag() {
        let config = TopologyConfig {
            tags: vec![TopologyTag {
                tag_id: "holding:100".to_string(),
                name: "Flow".to_string(),
                description: String::new(),
                units: "m3/h".to_string(),
                data_type: "uint16".to_string(),
                writable: false,
                process_area: String::new(),
                equipment_id: String::new(),
                normal_range: None,
            }],
            write_permissions: HashMap::new(),
        };
        assert_eq!(tag_units(&config, "holding:100"), "m3/h");
        assert_eq!(tag_units(&config, "holding:999"), "");
    }

    // ── load_topology (fixture) ───────────────────────────────────────────────

    #[test]
    fn load_topology_returns_default_when_no_file() {
        let config = load_topology();
        let _ = config.tags.len();
    }
}
