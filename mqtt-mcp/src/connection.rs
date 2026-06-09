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
        match std::fs::read_to_string(path) {
            Ok(content) => match serde_yaml::from_str::<TopologyConfig>(&content) {
                Ok(config) => {
                    tracing::info!("loaded topology from {path}");
                    return config;
                }
                Err(e) => tracing::warn!("failed to parse {path}: {e}"),
            },
            Err(_) => {}
        }
    }
    tracing::warn!(
        "no topology.yaml found — discover_tags returns scan cache; all writes denied"
    );
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
            return Vqt { tag_id: topic.to_string(), value, quality, timestamp: ts, units };
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
        normal_range: t
            .normal_range
            .as_ref()
            .map(|r| NormalRange { min: r.min, max: r.max }),
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
        name: vqt.tag_id.split('/').last().unwrap_or(&vqt.tag_id).to_string(),
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

