use serde::{Deserialize, Serialize};

// ── Core data types ──────────────────────────────────────────────────────────

/// Quality states per the FieldWorks VQT spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Quality {
    Good,
    Uncertain,
    Bad,
}

/// Tag value union. Numeric process values are f64; boolean states are bool;
/// string descriptors are String. Serializes without a type wrapper (untagged).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TagValue {
    Float(f64),
    Bool(bool),
    Text(String),
}

/// Write value union. The spec restricts writes to number | boolean — strings
/// are not writable regardless of tag data type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WriteValue {
    Float(f64),
    Bool(bool),
}

// ── VQT envelope ─────────────────────────────────────────────────────────────

/// The primary response envelope for all data operations.
/// Every tool that returns tag data returns this shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vqt {
    pub tag_id: String,
    pub value: TagValue,
    pub quality: Quality,
    /// ISO 8601 UTC. Always UTC, never local time. Millisecond precision.
    pub timestamp: String,
    /// Engineering units string (e.g. "degC", "bar", "m3/h"). Empty if unitless.
    pub units: String,
}

/// Confirmation returned by write_tag. Extends VQT with audit fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteConfirmation {
    pub tag_id: String,
    pub value_written: WriteValue,
    pub quality: Quality,
    pub timestamp: String,
    pub units: String,
    pub operator_id: String,
}

// ── Error types ───────────────────────────────────────────────────────────────

/// Required error codes. All seven are mandatory for conformance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    TagNotFound,
    TagNotWritable,
    ConnectionError,
    Timeout,
    InvalidValue,
    PermissionDenied,
    HistoryUnavailable,
}

/// Standard error envelope. Raw protocol errors must never surface to the
/// agent layer — translate everything into this shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterError {
    pub code: ErrorCode,
    /// Human-readable. Will surface to operators. No protocol addresses or codes.
    pub message: String,
    pub tag_id: Option<String>,
}

pub type AdapterResult<T> = Result<T, AdapterError>;

// ── Tag descriptor ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalRange {
    pub min: f64,
    pub max: f64,
}

/// Full tag descriptor returned by discover_tags.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagDescriptor {
    pub tag_id: String,
    pub name: String,
    pub description: String,
    pub units: String,
    pub data_type: String,
    pub writable: bool,
    pub process_area: String,
    pub equipment_id: String,
    pub normal_range: Option<NormalRange>,
}

// ── Per-tool response types ───────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectResponse {
    pub connected: bool,
    pub server_name: String,
    pub protocol_version: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisconnectResponse {
    pub disconnected: bool,
    pub timestamp: String,
}

/// Response from get_server_info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfoResponse {
    pub server_name: String,
    pub protocol: String,
    pub protocol_version: String,
    pub connected: bool,
    pub connection_state: String,
    /// All nine required tool names are always listed. Protocol extensions appear here too.
    pub capabilities: Vec<String>,
    pub uptime_seconds: u64,
    pub last_error: Option<String>,
}

/// A single entry returned by scan (MQTT) or browse (OPC-UA).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanEntry {
    pub topic: String,
    pub last_value: Option<TagValue>,
    pub quality: Quality,
    pub timestamp: Option<String>,
}

// ── Conformance trait ─────────────────────────────────────────────────────────

/// The nine required tools as a Rust trait.
///
/// Method names follow MQTT conventions because mqtt-mcp is the reference
/// implementation. OPC-UA adapters rename:
///   - `scan`          → `browse`         (different params: node_id, depth)
///   - `get_topic_tree` → `get_node_tree`  (different params: root_node, depth)
///
/// The spec will be updated to acknowledge these as the two protocol-specific
/// slots in an otherwise identical nine-tool surface.
///
/// This trait uses `async fn in trait` (stable since Rust 1.75). It is not
/// object-safe — do not use with `dyn FieldworksAdapter`.
#[allow(async_fn_in_trait)]
pub trait FieldworksAdapter {
    // Lifecycle ────────────────────────────────────────────────────────────────

    async fn connect(
        &self,
        host: String,
        port: u16,
        timeout_ms: Option<u32>,
        options: Option<serde_json::Value>,
    ) -> AdapterResult<ConnectResponse>;

    async fn disconnect(&self, reason: Option<String>) -> AdapterResult<DisconnectResponse>;

    // Discovery ────────────────────────────────────────────────────────────────

    async fn discover_tags(
        &self,
        process_area: Option<String>,
        equipment_id: Option<String>,
        include_metadata: Option<bool>,
    ) -> AdapterResult<Vec<TagDescriptor>>;

    /// Protocol-native incremental address-space exploration.
    /// MQTT: subscribes to wildcard, collects topics over `scan_duration_ms`.
    /// OPC-UA: rename to `browse`; replace params with `node_id` + `depth`.
    async fn scan(
        &self,
        topic_pattern: Option<String>,
        scan_duration_ms: Option<u32>,
    ) -> AdapterResult<Vec<ScanEntry>>;

    /// Full address-space dump. Intentionally expensive — use for topology
    /// onboarding and periodic refresh only, never at query runtime.
    /// OPC-UA: rename to `get_node_tree`; replace `topic_prefix` with `root_node`.
    async fn get_topic_tree(
        &self,
        topic_prefix: Option<String>,
        include_values: Option<bool>,
    ) -> AdapterResult<serde_json::Value>;

    // Data ─────────────────────────────────────────────────────────────────────

    async fn read_tag(&self, tag_id: String) -> AdapterResult<Vqt>;

    async fn read_tag_history(
        &self,
        tag_id: String,
        start_time: String,
        end_time: String,
        max_points: Option<u32>,
        quality_filter: Option<String>,
    ) -> AdapterResult<Vec<Vqt>>;

    async fn write_tag(
        &self,
        tag_id: String,
        value: WriteValue,
        units: String,
        operator_id: String,
        reason: String,
    ) -> AdapterResult<WriteConfirmation>;

    // Server ───────────────────────────────────────────────────────────────────

    async fn get_server_info(&self) -> AdapterResult<ServerInfoResponse>;
}
