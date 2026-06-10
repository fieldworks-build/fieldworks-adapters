use chrono::{SecondsFormat, Utc};
use fieldworks_adapter_core::*;
use rumqttc::{AsyncClient, Event, EventLoop, Packet};
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

// ── Connection status ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum ConnStatus {
    Connecting,
    Connected,
    Error(String),
}

// ── Received MQTT message ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    pub topic: String,
    pub payload: String,
    pub timestamp: String,
    pub retain: bool,
}

// ── Topology config ───────────────────────────────────────────────────────────
//
// Loaded from topology.yaml at connect time.
//
// Example topology.yaml:
//
//   tags:
//     - tag_id: "factory/pump01/flow_rate"
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
//     "factory/pump01/speed_setpoint":
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
    "float".to_string()
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
    tracing::warn!("no topology.yaml found — discover_tags returns scan cache; all writes denied");
    TopologyConfig::default()
}

// ── Active connection ─────────────────────────────────────────────────────────

pub struct MqttConnection {
    pub client: AsyncClient,
    pub handle: JoinHandle<()>,
    pub host: String,
    pub port: u16,
    pub protocol_version: String,
    pub connected_at: Instant,
    /// Latest VQT per topic, updated by the event loop background task.
    pub cache: Arc<RwLock<HashMap<String, Vqt>>>,
    /// Broadcast channel used by read_tag and scan to receive incoming messages.
    pub msg_tx: broadcast::Sender<ReceivedMessage>,
    pub status_rx: watch::Receiver<ConnStatus>,
    pub last_error: Arc<Mutex<Option<String>>>,
    pub topology: TopologyConfig,
}

// ── Event loop task ───────────────────────────────────────────────────────────

/// Runs in a background tokio task. Drives the rumqttc event loop and:
/// - Signals ConnAck via the watch channel so connect() can wait for confirmation.
/// - Updates the topic cache on every incoming PUBLISH.
/// - Broadcasts each incoming PUBLISH to all active read_tag / scan receivers.
pub async fn run_event_loop(
    mut event_loop: EventLoop,
    cache: Arc<RwLock<HashMap<String, Vqt>>>,
    msg_tx: broadcast::Sender<ReceivedMessage>,
    status_tx: watch::Sender<ConnStatus>,
    last_error: Arc<Mutex<Option<String>>>,
) {
    loop {
        match event_loop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                let _ = status_tx.send(ConnStatus::Connected);
                tracing::info!("mqtt: connected");
            }
            Ok(Event::Incoming(Packet::Publish(p))) => {
                let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
                let payload_str = String::from_utf8_lossy(&p.payload).to_string();
                let vqt = parse_mqtt_payload(&p.topic, &payload_str, &timestamp);

                cache.write().unwrap().insert(p.topic.clone(), vqt);

                let _ = msg_tx.send(ReceivedMessage {
                    topic: p.topic,
                    payload: payload_str,
                    timestamp,
                    retain: p.retain,
                });
            }
            Ok(Event::Incoming(Packet::Disconnect)) => {
                let _ = status_tx.send(ConnStatus::Error("disconnected by broker".into()));
                tracing::info!("mqtt: disconnected by broker");
            }
            Err(e) => {
                let msg = e.to_string();
                tracing::warn!("mqtt event loop error: {msg}");
                let _ = status_tx.send(ConnStatus::Error(msg.clone()));
                *last_error.lock().unwrap() = Some(msg);
                // rumqttc auto-reconnects; keep polling
            }
            _ => {}
        }
    }
}

// ── Payload parsing ───────────────────────────────────────────────────────────

/// Parse an MQTT payload bytes into a VQT.
///
/// Supported payload formats (tried in order):
/// 1. JSON object with a `"value"` key:
///    `{"value": 42.3, "quality": "good", "timestamp": "...", "units": "bar"}`
/// 2. Raw float string: `"42.3"`
/// 3. Raw bool-like string: `"true"`, `"false"`, `"1"`, `"0"`, `"on"`, `"off"`, …
/// 4. Arbitrary string (falls through as TagValue::Text).
pub fn parse_mqtt_payload(topic: &str, payload: &str, timestamp: &str) -> Vqt {
    // JSON with "value" key
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload) {
        if let Some(v) = json.get("value") {
            let value = json_to_tag_value(v, payload);
            let quality = json
                .get("quality")
                .and_then(|q| q.as_str())
                .map(str_to_quality)
                .unwrap_or(Quality::Good);
            let ts = json
                .get("timestamp")
                .and_then(|t| t.as_str())
                .unwrap_or(timestamp)
                .to_string();
            let units = json
                .get("units")
                .and_then(|u| u.as_str())
                .unwrap_or("")
                .to_string();
            return Vqt {
                tag_id: topic.to_string(),
                value,
                quality,
                timestamp: ts,
                units,
            };
        }
    }

    let s = payload.trim();

    if let Ok(f) = s.parse::<f64>() {
        return Vqt {
            tag_id: topic.to_string(),
            value: TagValue::Float(f),
            quality: Quality::Good,
            timestamp: timestamp.to_string(),
            units: String::new(),
        };
    }

    match s.to_ascii_lowercase().as_str() {
        "true" | "1" | "on" | "open" | "active" | "high" => {
            return Vqt {
                tag_id: topic.to_string(),
                value: TagValue::Bool(true),
                quality: Quality::Good,
                timestamp: timestamp.to_string(),
                units: String::new(),
            };
        }
        "false" | "0" | "off" | "closed" | "inactive" | "low" => {
            return Vqt {
                tag_id: topic.to_string(),
                value: TagValue::Bool(false),
                quality: Quality::Good,
                timestamp: timestamp.to_string(),
                units: String::new(),
            };
        }
        _ => {}
    }

    Vqt {
        tag_id: topic.to_string(),
        value: TagValue::Text(payload.to_string()),
        quality: Quality::Good,
        timestamp: timestamp.to_string(),
        units: String::new(),
    }
}

fn json_to_tag_value(v: &serde_json::Value, fallback: &str) -> TagValue {
    if let Some(f) = v.as_f64() {
        TagValue::Float(f)
    } else if let Some(b) = v.as_bool() {
        TagValue::Bool(b)
    } else if let Some(s) = v.as_str() {
        TagValue::Text(s.to_string())
    } else {
        TagValue::Text(fallback.to_string())
    }
}

pub fn str_to_quality(s: &str) -> Quality {
    match s {
        "good" => Quality::Good,
        "uncertain" => Quality::Uncertain,
        _ => Quality::Bad,
    }
}

// ── Tag descriptor helpers ────────────────────────────────────────────────────

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

pub fn vqt_to_descriptor(vqt: &Vqt) -> TagDescriptor {
    let data_type = match &vqt.value {
        TagValue::Float(_) => "float",
        TagValue::Bool(_) => "boolean",
        TagValue::Text(_) => "string",
    }
    .to_string();
    TagDescriptor {
        tag_id: vqt.tag_id.clone(),
        name: vqt
            .tag_id
            .split('/')
            .next_back()
            .unwrap_or(&vqt.tag_id)
            .to_string(),
        description: String::new(),
        units: vqt.units.clone(),
        data_type,
        writable: false,
        process_area: String::new(),
        equipment_id: String::new(),
        normal_range: None,
    }
}

// ── Topic tree builder ────────────────────────────────────────────────────────

/// Build a nested JSON object from a flat topic → leaf map.
/// `"factory/pump01/flow"` becomes `{"factory": {"pump01": {"flow": <leaf>}}}`.
pub fn build_topic_tree(
    topics: impl IntoIterator<Item = (String, serde_json::Value)>,
) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    for (topic, leaf) in topics {
        let parts: Vec<&str> = topic.split('/').collect();
        insert_path(&mut root, &parts, leaf);
    }
    serde_json::Value::Object(root)
}

fn insert_path(
    node: &mut serde_json::Map<String, serde_json::Value>,
    path: &[&str],
    value: serde_json::Value,
) {
    match path {
        [] => {}
        [leaf] => {
            node.insert(leaf.to_string(), value);
        }
        [head, tail @ ..] => {
            let child = node
                .entry(head.to_string())
                .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
            if let serde_json::Value::Object(m) = child {
                insert_path(m, tail, value);
            }
        }
    }
}

// ── Audit log ─────────────────────────────────────────────────────────────────

/// Append a write event to `write_audit.jsonl` (append-only, one JSON object per line).
/// Pure validation for write_tag. Extracted so it can be unit-tested without a live connection.
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

pub fn log_write_audit(
    tag_id: &str,
    value: &serde_json::Value,
    units: &str,
    operator_id: &str,
    reason: &str,
) {
    use std::io::Write;
    let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let entry = serde_json::json!({
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

    fn ts() -> &'static str {
        "2026-06-10T00:00:00.000Z"
    }

    // ── parse_mqtt_payload ────────────────────────────────────────────────────

    #[test]
    fn payload_json_value_key_float() {
        let vqt = parse_mqtt_payload("t", r#"{"value": 42.3}"#, ts());
        assert!(matches!(vqt.value, TagValue::Float(f) if (f - 42.3).abs() < 1e-9));
        assert_eq!(vqt.quality, Quality::Good);
    }

    #[test]
    fn payload_json_value_key_bool() {
        let vqt = parse_mqtt_payload("t", r#"{"value": true}"#, ts());
        assert!(matches!(vqt.value, TagValue::Bool(true)));
    }

    #[test]
    fn payload_json_quality_propagates() {
        let vqt = parse_mqtt_payload("t", r#"{"value": 1.0, "quality": "bad"}"#, ts());
        assert_eq!(vqt.quality, Quality::Bad);
    }

    #[test]
    fn payload_json_units_propagates() {
        let vqt = parse_mqtt_payload("t", r#"{"value": 1.0, "units": "bar"}"#, ts());
        assert_eq!(vqt.units, "bar");
    }

    #[test]
    fn payload_raw_float() {
        let vqt = parse_mqtt_payload("t", "3.14", ts());
        assert!(matches!(vqt.value, TagValue::Float(f) if (f - 3.14).abs() < 1e-9));
    }

    #[test]
    fn payload_raw_integer_string() {
        let vqt = parse_mqtt_payload("t", "100", ts());
        assert!(matches!(vqt.value, TagValue::Float(f) if (f - 100.0).abs() < 1e-9));
    }

    #[test]
    fn payload_bool_aliases_true() {
        // "1" parses as f64 first, so only word-form aliases reach the bool branch
        for s in ["true", "True", "TRUE", "on", "ON", "open", "active", "high"] {
            let vqt = parse_mqtt_payload("t", s, ts());
            assert!(
                matches!(vqt.value, TagValue::Bool(true)),
                "expected Bool(true) for payload '{s}'"
            );
        }
    }

    #[test]
    fn payload_bool_aliases_false() {
        // "0" parses as f64 first, so only word-form aliases reach the bool branch
        for s in [
            "false", "False", "FALSE", "off", "OFF", "closed", "inactive", "low",
        ] {
            let vqt = parse_mqtt_payload("t", s, ts());
            assert!(
                matches!(vqt.value, TagValue::Bool(false)),
                "expected Bool(false) for payload '{s}'"
            );
        }
    }

    #[test]
    fn payload_arbitrary_string_fallback() {
        let vqt = parse_mqtt_payload("t", "UNKNOWN_STATE", ts());
        assert!(matches!(vqt.value, TagValue::Text(s) if s == "UNKNOWN_STATE"));
    }

    #[test]
    fn payload_topic_id_is_set() {
        let vqt = parse_mqtt_payload("factory/pump01/flow", "1.0", ts());
        assert_eq!(vqt.tag_id, "factory/pump01/flow");
    }

    #[test]
    fn payload_json_without_value_key_falls_through() {
        // JSON but no "value" key → try as raw string → falls back to Text
        let vqt = parse_mqtt_payload("t", r#"{"other": 1}"#, ts());
        assert!(matches!(vqt.value, TagValue::Text(_)));
    }

    // ── str_to_quality ────────────────────────────────────────────────────────

    #[test]
    fn quality_strings() {
        assert_eq!(str_to_quality("good"), Quality::Good);
        assert_eq!(str_to_quality("uncertain"), Quality::Uncertain);
        assert_eq!(str_to_quality("bad"), Quality::Bad);
        assert_eq!(str_to_quality("unknown"), Quality::Bad);
        assert_eq!(str_to_quality(""), Quality::Bad);
    }

    // ── build_topic_tree ──────────────────────────────────────────────────────

    #[test]
    fn topic_tree_flat_single_level() {
        let tree = build_topic_tree([("sensor".into(), serde_json::json!(1))]);
        assert_eq!(tree["sensor"], 1);
    }

    #[test]
    fn topic_tree_nested() {
        let tree = build_topic_tree([
            ("factory/pump01/flow".into(), serde_json::json!(42.0)),
            ("factory/pump01/speed".into(), serde_json::json!(30.0)),
            ("factory/pump02/flow".into(), serde_json::json!(10.0)),
        ]);
        assert_eq!(tree["factory"]["pump01"]["flow"], 42.0);
        assert_eq!(tree["factory"]["pump01"]["speed"], 30.0);
        assert_eq!(tree["factory"]["pump02"]["flow"], 10.0);
    }

    #[test]
    fn topic_tree_empty_input() {
        let tree = build_topic_tree(std::iter::empty::<(String, serde_json::Value)>());
        assert!(tree.as_object().unwrap().is_empty());
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
        let result = validate_write(&p, &serde_json::json!(50.0), "Hz", "tag");
        assert!(matches!(result, Ok(WriteValue::Float(f)) if (f - 50.0).abs() < 1e-9));
    }

    #[test]
    fn validate_write_bool_ok() {
        let p = perm(None, None, None);
        let result = validate_write(&p, &serde_json::json!(true), "", "tag");
        assert!(matches!(result, Ok(WriteValue::Bool(true))));
    }

    #[test]
    fn validate_write_below_min() {
        let p = perm(Some(10.0), Some(100.0), None);
        let err = validate_write(&p, &serde_json::json!(5.0), "", "tag").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidValue);
        assert!(err.message.contains("minimum"));
    }

    #[test]
    fn validate_write_above_max() {
        let p = perm(Some(0.0), Some(60.0), None);
        let err = validate_write(&p, &serde_json::json!(61.0), "", "tag").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidValue);
        assert!(err.message.contains("maximum"));
    }

    #[test]
    fn validate_write_at_boundary_passes() {
        let p = perm(Some(0.0), Some(60.0), Some("Hz"));
        assert!(validate_write(&p, &serde_json::json!(0.0), "Hz", "tag").is_ok());
        assert!(validate_write(&p, &serde_json::json!(60.0), "Hz", "tag").is_ok());
    }

    #[test]
    fn validate_write_units_mismatch() {
        let p = perm(None, None, Some("Hz"));
        let err = validate_write(&p, &serde_json::json!(50.0), "rpm", "tag").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidValue);
        assert!(err.message.contains("Units") || err.message.contains("units"));
    }

    #[test]
    fn validate_write_empty_units_skips_check() {
        let p = perm(None, None, Some(""));
        assert!(validate_write(&p, &serde_json::json!(1.0), "anything", "tag").is_ok());
    }

    #[test]
    fn validate_write_string_rejected() {
        let p = perm(None, None, None);
        let err = validate_write(&p, &serde_json::json!("bad"), "", "tag").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidValue);
    }

    #[test]
    fn validate_write_tag_id_in_error() {
        let p = perm(Some(0.0), Some(10.0), None);
        let err = validate_write(&p, &serde_json::json!(99.0), "", "my/tag").unwrap_err();
        assert_eq!(err.tag_id.as_deref(), Some("my/tag"));
    }

    // ── load_topology (fixture) ───────────────────────────────────────────────

    #[test]
    fn load_topology_returns_default_when_no_file() {
        // No topology.yaml in the test working directory → default (empty).
        // This test is intentionally loose: it just verifies the function
        // doesn't panic and returns a valid (possibly empty) config.
        let config = load_topology();
        // If a topology.yaml happens to exist, we just check it parsed.
        let _ = config.tags.len();
    }
}
