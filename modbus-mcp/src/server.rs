#![allow(dead_code)]

use crate::connection::{
    build_tag_tree, load_topology, log_write_audit, map_exception_code, map_transport_error,
    parse_tag_id, register_count, registers_to_value, tag_units, topology_tag_to_descriptor,
    validate_write, value_to_registers, ModbusConnection, ParsedTag,
};
use chrono::{SecondsFormat, Utc};
use fieldworks_adapter_core::*;
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Implementation, ProtocolVersion, ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler,
};
use serde_json::json;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::lookup_host;
use tokio_modbus::client::{tcp, Client, Reader, Writer};
use tokio_modbus::Slave;

// ── Parameter structs ─────────────────────────────────────────────────────────

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct ConnectParams {
    #[schemars(description = "Hostname or IP address of the Modbus TCP device or gateway.")]
    host: String,
    #[schemars(description = "Port number. Standard Modbus TCP port is 502.")]
    port: u16,
    #[schemars(description = "Connection timeout in milliseconds. Default 5000.")]
    timeout_ms: Option<u32>,
    #[schemars(description = "Protocol-specific options object. Supported keys: \
        slave_id (integer, 0-255, default 1) — the default unit/slave identifier \
        used for all reads and writes on this connection.")]
    options: Option<serde_json::Value>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct DisconnectParams {
    #[schemars(description = "Human-readable reason for disconnection. Logged to audit trail.")]
    reason: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct DiscoverTagsParams {
    #[schemars(
        description = "Filter to tags belonging to this process area. Must match a process_area in topology.yaml."
    )]
    process_area: Option<String>,
    #[schemars(description = "Filter to tags belonging to this equipment instance.")]
    equipment_id: Option<String>,
    #[schemars(
        description = "Include extended metadata (description, normal range). Default true."
    )]
    include_metadata: Option<bool>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct ScanParams {
    #[schemars(description = "Register type to scan: holding, input, coil, or discrete.")]
    register_type: String,
    #[schemars(description = "Starting register/coil address (0-65535).")]
    start_address: u16,
    #[schemars(
        description = "Number of consecutive addresses to read. Max 125 for holding/input registers, 2000 for coil/discrete (Modbus protocol limits)."
    )]
    count: u16,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct GetTopicTreeParams {
    #[schemars(
        description = "Unused for Modbus — the tree is always the full topology.yaml, grouped by register type. No live I/O is performed."
    )]
    topic_prefix: Option<String>,
    #[schemars(description = "Unused for Modbus — no live I/O is performed by this tool.")]
    include_values: Option<bool>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct ReadTagParams {
    #[schemars(
        description = "Tag identifier: \"register_type:address[:data_type]\", e.g. \"holding:100\", \"holding:100:float32\", \"coil:5\"."
    )]
    tag_id: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct ReadTagHistoryParams {
    #[schemars(description = "Tag identifier.")]
    tag_id: String,
    #[schemars(description = "Window start. ISO 8601 UTC. Example: 2024-01-15T08:00:00.000Z")]
    start_time: String,
    #[schemars(description = "Window end. ISO 8601 UTC. Example: 2024-01-15T09:00:00.000Z")]
    end_time: String,
    #[schemars(
        description = "Maximum data points to return. Adapter may downsample to fit. Default 1000."
    )]
    max_points: Option<u32>,
    #[schemars(
        description = "Quality filter: good or good_or_uncertain. Default: no filter (all quality levels returned)."
    )]
    quality_filter: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct WriteTagParams {
    #[schemars(
        description = "Tag identifier. Must be a holding register or coil (input registers and discrete inputs are read-only in Modbus), and must appear in topology write_permissions."
    )]
    tag_id: String,
    #[schemars(
        description = "Value to write: number for holding registers, boolean for coils. \
        Must be within the configured engineering range and within the data type's numeric range."
    )]
    value: serde_json::Value,
    #[schemars(
        description = "Engineering units of the provided value. Validated against tag configuration."
    )]
    units: String,
    #[schemars(
        description = "Identifier of the operator who approved this write. Logged to audit trail."
    )]
    operator_id: String,
    #[schemars(description = "Human-readable reason for the write. Logged to audit trail.")]
    reason: String,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const DEFAULT_TIMEOUT_MS: u64 = 5000;

fn fw_error(err: AdapterError) -> CallToolResult {
    CallToolResult::structured_error(json!({ "error": err }))
}

fn not_connected(tag_id: Option<&str>) -> CallToolResult {
    fw_error(AdapterError {
        code: ErrorCode::ConnectionError,
        message: "Not connected to a Modbus device. Call connect first.".into(),
        tag_id: tag_id.map(str::to_string),
    })
}

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

const CAPABILITIES: &[&str] = &[
    "connect",
    "disconnect",
    "discover_tags",
    "scan",
    "get_topic_tree",
    "read_tag",
    "read_tag_history",
    "write_tag",
    "get_server_info",
];

/// Records the failure on the connection (visible via get_server_info) and,
/// for connection-level failures, drops the connection entirely — tokio-modbus
/// has no auto-reconnect, so the AI must call connect again.
fn record_and_maybe_clear(inner: &mut Option<ModbusConnection>, err: &AdapterError) {
    if let Some(c) = inner.as_mut() {
        c.last_error = Some(err.message.clone());
    }
    if err.code == ErrorCode::ConnectionError {
        *inner = None;
    }
}

/// Wrap a Modbus call in a timeout, since tokio-modbus has no native one.
async fn timed<T>(
    fut: impl std::future::Future<Output = T>,
    timeout_ms: u64,
    tag_id: &str,
) -> Result<T, AdapterError> {
    tokio::time::timeout(Duration::from_millis(timeout_ms), fut)
        .await
        .map_err(|_| AdapterError {
            code: ErrorCode::Timeout,
            message: format!("Modbus operation for '{tag_id}' timed out after {timeout_ms}ms."),
            tag_id: Some(tag_id.to_string()),
        })
}

/// Flatten tokio-modbus's nested Result<Result<T, ExceptionCode>, Error> for a read.
fn map_read_result<T>(
    result: Result<Result<T, tokio_modbus::ExceptionCode>, tokio_modbus::Error>,
    tag_id: &str,
) -> Result<T, AdapterError> {
    match result {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(exc)) => Err(map_exception_code(exc, tag_id, false)),
        Err(e) => Err(map_transport_error(&e, tag_id)),
    }
}

/// Same as map_read_result, but maps IllegalFunction to TAG_NOT_WRITABLE.
fn map_write_result<T>(
    result: Result<Result<T, tokio_modbus::ExceptionCode>, tokio_modbus::Error>,
    tag_id: &str,
) -> Result<T, AdapterError> {
    match result {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(exc)) => Err(map_exception_code(exc, tag_id, true)),
        Err(e) => Err(map_transport_error(&e, tag_id)),
    }
}

/// Read `parsed`'s tag from the device. Requires the connection mutex held by the caller.
async fn read_parsed_tag(
    conn: &mut ModbusConnection,
    parsed: ParsedTag,
    tag_id: &str,
) -> Result<TagValue, AdapterError> {
    match parsed {
        ParsedTag::Holding { address, data_type } => {
            let raw = timed(
                conn.ctx
                    .read_holding_registers(address, register_count(data_type)),
                DEFAULT_TIMEOUT_MS,
                tag_id,
            )
            .await?;
            let words = map_read_result(raw, tag_id)?;
            registers_to_value(&words, data_type)
        }
        ParsedTag::Input { address, data_type } => {
            let raw = timed(
                conn.ctx
                    .read_input_registers(address, register_count(data_type)),
                DEFAULT_TIMEOUT_MS,
                tag_id,
            )
            .await?;
            let words = map_read_result(raw, tag_id)?;
            registers_to_value(&words, data_type)
        }
        ParsedTag::Coil { address } => {
            let raw = timed(conn.ctx.read_coils(address, 1), DEFAULT_TIMEOUT_MS, tag_id).await?;
            let bits = map_read_result(raw, tag_id)?;
            bits.first()
                .map(|&b| TagValue::Bool(b))
                .ok_or_else(|| AdapterError {
                    code: ErrorCode::ConnectionError,
                    message: "expected 1 coil in device response, got 0".into(),
                    tag_id: Some(tag_id.to_string()),
                })
        }
        ParsedTag::Discrete { address } => {
            let raw = timed(
                conn.ctx.read_discrete_inputs(address, 1),
                DEFAULT_TIMEOUT_MS,
                tag_id,
            )
            .await?;
            let bits = map_read_result(raw, tag_id)?;
            bits.first()
                .map(|&b| TagValue::Bool(b))
                .ok_or_else(|| AdapterError {
                    code: ErrorCode::ConnectionError,
                    message: "expected 1 discrete input in device response, got 0".into(),
                    tag_id: Some(tag_id.to_string()),
                })
        }
    }
}

// ── Server struct ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ModbusMcpServer {
    inner: std::sync::Arc<tokio::sync::Mutex<Option<ModbusConnection>>>,
    tool_router: ToolRouter<ModbusMcpServer>,
}

impl ModbusMcpServer {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            tool_router: Self::tool_router(),
        }
    }
}

// ── Tool implementations ──────────────────────────────────────────────────────

#[tool_router]
impl ModbusMcpServer {
    // ── connect ───────────────────────────────────────────────────────────────

    #[tool(
        description = "Establish a connection to a Modbus TCP device or gateway. If already connected, the existing connection is replaced."
    )]
    async fn connect(
        &self,
        Parameters(ConnectParams {
            host,
            port,
            timeout_ms,
            options,
        }): Parameters<ConnectParams>,
    ) -> Result<CallToolResult, McpError> {
        let mut inner = self.inner.lock().await;

        if let Some(mut old) = inner.take() {
            let _ = old.ctx.disconnect().await;
        }

        let slave_id = options
            .as_ref()
            .and_then(|o| o.get("slave_id"))
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as u8;

        let timeout_dur = Duration::from_millis(timeout_ms.unwrap_or(5000) as u64);

        let addrs: Vec<SocketAddr> = match lookup_host((host.as_str(), port)).await {
            Ok(a) => a.collect(),
            Err(e) => {
                return Ok(fw_error(AdapterError {
                    code: ErrorCode::ConnectionError,
                    message: format!("DNS resolution failed for {host}:{port} — {e}"),
                    tag_id: None,
                }));
            }
        };
        if addrs.is_empty() {
            return Ok(fw_error(AdapterError {
                code: ErrorCode::ConnectionError,
                message: format!("No addresses found for {host}:{port}"),
                tag_id: None,
            }));
        }

        // A hostname can resolve to multiple addresses (e.g. "localhost" to
        // both ::1 and 127.0.0.1) — try each in turn rather than assuming
        // the first one is reachable. The whole attempt, across every
        // address, is bounded by the single timeout below.
        let connect_attempt = async {
            let mut last_err = None;
            for addr in &addrs {
                match tcp::connect_slave(*addr, Slave(slave_id)).await {
                    Ok(ctx) => return Ok(ctx),
                    Err(e) => last_err = Some(e),
                }
            }
            Err(last_err.expect("addrs is non-empty, so at least one attempt ran"))
        };

        let connect_result = tokio::time::timeout(timeout_dur, connect_attempt).await;

        let ctx = match connect_result {
            Err(_) => {
                return Ok(fw_error(AdapterError {
                    code: ErrorCode::Timeout,
                    message: format!(
                        "Connection to {host}:{port} timed out after {}ms",
                        timeout_ms.unwrap_or(5000)
                    ),
                    tag_id: None,
                }));
            }
            Ok(Err(e)) => {
                return Ok(fw_error(AdapterError {
                    code: ErrorCode::ConnectionError,
                    message: format!(
                        "Failed to connect to {host}:{port} (tried {} address(es)) — {e}",
                        addrs.len()
                    ),
                    tag_id: None,
                }));
            }
            Ok(Ok(ctx)) => ctx,
        };

        let topology = load_topology();

        *inner = Some(ModbusConnection {
            ctx,
            host: host.clone(),
            port,
            slave: Slave(slave_id),
            connected_at: Instant::now(),
            last_error: None,
            topology,
        });

        Ok(CallToolResult::structured(json!({
            "connected": true,
            "server_name": format!("{host}:{port}"),
            "protocol_version": "Modbus Application Protocol V1.1b3",
            "timestamp": now_iso(),
        })))
    }

    // ── disconnect ────────────────────────────────────────────────────────────

    #[tool(description = "Cleanly terminate the connection to the Modbus device.")]
    async fn disconnect(
        &self,
        Parameters(DisconnectParams { reason }): Parameters<DisconnectParams>,
    ) -> Result<CallToolResult, McpError> {
        let mut inner = self.inner.lock().await;
        let ts = now_iso();

        match inner.take() {
            None => Ok(CallToolResult::structured(json!({
                "disconnected": true,
                "timestamp": ts,
            }))),
            Some(mut conn) => {
                if let Some(r) = &reason {
                    tracing::info!("disconnect requested: {r}");
                }
                let _ = conn.ctx.disconnect().await;
                Ok(CallToolResult::structured(json!({
                    "disconnected": true,
                    "timestamp": ts,
                })))
            }
        }
    }

    // ── discover_tags ─────────────────────────────────────────────────────────

    #[tool(
        description = "Enumerate available tags with metadata from topology.yaml. Modbus has no protocol-native tag discovery — without a topology.yaml, this returns an empty list; use scan to probe live address ranges instead."
    )]
    async fn discover_tags(
        &self,
        Parameters(DiscoverTagsParams {
            process_area,
            equipment_id,
            include_metadata: _,
        }): Parameters<DiscoverTagsParams>,
    ) -> Result<CallToolResult, McpError> {
        let inner = self.inner.lock().await;
        match inner.as_ref() {
            None => Ok(not_connected(None)),
            Some(conn) => {
                let mut tags: Vec<TagDescriptor> = conn
                    .topology
                    .tags
                    .iter()
                    .map(topology_tag_to_descriptor)
                    .collect();

                if let Some(ref pa) = process_area {
                    tags.retain(|t| &t.process_area == pa);
                }
                if let Some(ref eq) = equipment_id {
                    tags.retain(|t| &t.equipment_id == eq);
                }

                Ok(CallToolResult::structured(json!({ "tags": tags })))
            }
        }
    }

    // ── scan ──────────────────────────────────────────────────────────────────

    #[tool(
        description = "Actively read a contiguous range of a given register type from the live device. \
        Unlike MQTT's passive scan, this is a direct request/response Modbus read — returns immediately \
        with raw per-address values (no float32/int32 decoding; each entry is one raw register or bit)."
    )]
    async fn scan(
        &self,
        Parameters(ScanParams {
            register_type,
            start_address,
            count,
        }): Parameters<ScanParams>,
    ) -> Result<CallToolResult, McpError> {
        let rt = register_type.to_ascii_lowercase();
        let max_count: u16 = if rt == "coil" || rt == "discrete" {
            2000
        } else {
            125
        };
        if count == 0 || count > max_count {
            return Ok(fw_error(AdapterError {
                code: ErrorCode::InvalidValue,
                message: format!("count must be between 1 and {max_count} for '{rt}'."),
                tag_id: None,
            }));
        }

        let mut inner = self.inner.lock().await;
        let conn = match inner.as_mut() {
            None => return Ok(not_connected(None)),
            Some(c) => c,
        };

        let ts = now_iso();
        let scan_tag_id = format!("{rt}:{start_address}");

        let entries_result: Result<Vec<serde_json::Value>, AdapterError> = match rt.as_str() {
            "holding" => timed(
                conn.ctx.read_holding_registers(start_address, count),
                DEFAULT_TIMEOUT_MS,
                &scan_tag_id,
            )
            .await
            .and_then(|raw| map_read_result(raw, &scan_tag_id))
            .map(|words| {
                words
                    .into_iter()
                    .enumerate()
                    .map(|(i, w)| {
                        json!({
                            "tag_id": format!("holding:{}", start_address as u32 + i as u32),
                            "value": w,
                            "quality": "good",
                            "timestamp": ts,
                        })
                    })
                    .collect()
            }),
            "input" => timed(
                conn.ctx.read_input_registers(start_address, count),
                DEFAULT_TIMEOUT_MS,
                &scan_tag_id,
            )
            .await
            .and_then(|raw| map_read_result(raw, &scan_tag_id))
            .map(|words| {
                words
                    .into_iter()
                    .enumerate()
                    .map(|(i, w)| {
                        json!({
                            "tag_id": format!("input:{}", start_address as u32 + i as u32),
                            "value": w,
                            "quality": "good",
                            "timestamp": ts,
                        })
                    })
                    .collect()
            }),
            "coil" => timed(
                conn.ctx.read_coils(start_address, count),
                DEFAULT_TIMEOUT_MS,
                &scan_tag_id,
            )
            .await
            .and_then(|raw| map_read_result(raw, &scan_tag_id))
            .map(|bits| {
                bits.into_iter()
                    .enumerate()
                    .map(|(i, b)| {
                        json!({
                            "tag_id": format!("coil:{}", start_address as u32 + i as u32),
                            "value": b,
                            "quality": "good",
                            "timestamp": ts,
                        })
                    })
                    .collect()
            }),
            "discrete" => timed(
                conn.ctx.read_discrete_inputs(start_address, count),
                DEFAULT_TIMEOUT_MS,
                &scan_tag_id,
            )
            .await
            .and_then(|raw| map_read_result(raw, &scan_tag_id))
            .map(|bits| {
                bits.into_iter()
                    .enumerate()
                    .map(|(i, b)| {
                        json!({
                            "tag_id": format!("discrete:{}", start_address as u32 + i as u32),
                            "value": b,
                            "quality": "good",
                            "timestamp": ts,
                        })
                    })
                    .collect()
            }),
            other => {
                return Ok(fw_error(AdapterError {
                    code: ErrorCode::InvalidValue,
                    message: format!(
                        "'{other}' is not a valid register_type. Expected one of: holding, input, coil, discrete."
                    ),
                    tag_id: None,
                }));
            }
        };

        match entries_result {
            Ok(entries) => Ok(CallToolResult::structured(json!({ "entries": entries }))),
            Err(e) => {
                record_and_maybe_clear(&mut inner, &e);
                Ok(fw_error(e))
            }
        }
    }

    // ── get_topic_tree ────────────────────────────────────────────────────────

    #[tool(
        description = "Return the topology.yaml tag set grouped by register type (holding/input/coil/discrete). \
        Purely config-derived — no live I/O. Use scan for live address-space exploration."
    )]
    async fn get_topic_tree(
        &self,
        Parameters(GetTopicTreeParams { .. }): Parameters<GetTopicTreeParams>,
    ) -> Result<CallToolResult, McpError> {
        let inner = self.inner.lock().await;
        match inner.as_ref() {
            None => Ok(not_connected(None)),
            Some(conn) => Ok(CallToolResult::structured(build_tag_tree(
                &conn.topology.tags,
            ))),
        }
    }

    // ── read_tag ──────────────────────────────────────────────────────────────

    #[tool(
        description = "Read the current value of a single tag. tag_id format: \"register_type:address[:data_type]\", \
        e.g. \"holding:100\", \"holding:100:float32\", \"coil:5\". Returns a VQT envelope."
    )]
    async fn read_tag(
        &self,
        Parameters(ReadTagParams { tag_id }): Parameters<ReadTagParams>,
    ) -> Result<CallToolResult, McpError> {
        let parsed = match parse_tag_id(&tag_id) {
            Ok(p) => p,
            Err(e) => return Ok(fw_error(e)),
        };

        let mut inner = self.inner.lock().await;
        let conn = match inner.as_mut() {
            None => return Ok(not_connected(Some(&tag_id))),
            Some(c) => c,
        };

        let ts = now_iso();
        let units = tag_units(&conn.topology, &tag_id);
        let result = read_parsed_tag(conn, parsed, &tag_id).await;

        match result {
            Ok(value) => {
                let vqt = Vqt {
                    tag_id: tag_id.clone(),
                    value,
                    quality: Quality::Good,
                    timestamp: ts,
                    units,
                };
                Ok(CallToolResult::structured(
                    serde_json::to_value(&vqt).unwrap(),
                ))
            }
            Err(e) => {
                record_and_maybe_clear(&mut inner, &e);
                Ok(fw_error(e))
            }
        }
    }

    // ── read_tag_history ──────────────────────────────────────────────────────

    #[tool(description = "Read a time-series for a tag over a defined window. \
        Modbus has no native history. This adapter returns HISTORY_UNAVAILABLE. \
        Integrate a Modbus-polling historian (e.g. an InfluxDB bridge) and extend \
        this method to support historical queries.")]
    async fn read_tag_history(
        &self,
        Parameters(ReadTagHistoryParams { tag_id, .. }): Parameters<ReadTagHistoryParams>,
    ) -> Result<CallToolResult, McpError> {
        Ok(fw_error(AdapterError {
            code: ErrorCode::HistoryUnavailable,
            message: "Modbus does not support native historical data. \
                      Bridge to a time-series store to enable read_tag_history."
                .into(),
            tag_id: Some(tag_id),
        }))
    }

    // ── write_tag ─────────────────────────────────────────────────────────────

    #[tool(
        description = "Write a value to a holding register or coil. Input registers and discrete inputs \
        are read-only in Modbus and always return TAG_NOT_WRITABLE. Gated by topology write_permissions — \
        any tag not explicitly listed returns TAG_NOT_WRITABLE. Requires operator_id and reason; both are \
        logged to write_audit.jsonl."
    )]
    async fn write_tag(
        &self,
        Parameters(WriteTagParams {
            tag_id,
            value,
            units,
            operator_id,
            reason,
        }): Parameters<WriteTagParams>,
    ) -> Result<CallToolResult, McpError> {
        let parsed = match parse_tag_id(&tag_id) {
            Ok(p) => p,
            Err(e) => return Ok(fw_error(e)),
        };

        if !parsed.is_writable_type() {
            return Ok(fw_error(AdapterError {
                code: ErrorCode::TagNotWritable,
                message: format!(
                    "'{tag_id}' is a {} register, which is read-only in Modbus.",
                    parsed.register_type_name()
                ),
                tag_id: Some(tag_id),
            }));
        }

        let mut inner = self.inner.lock().await;
        let conn = match inner.as_mut() {
            None => return Ok(not_connected(Some(&tag_id))),
            Some(c) => c,
        };

        let perm = match conn.topology.write_permissions.get(&tag_id).cloned() {
            None => {
                return Ok(fw_error(AdapterError {
                    code: ErrorCode::TagNotWritable,
                    message: format!(
                        "Tag '{tag_id}' is not in topology write_permissions. \
                         Add it to topology.yaml to allow writes."
                    ),
                    tag_id: Some(tag_id),
                }));
            }
            Some(p) => p,
        };

        let write_value = match validate_write(&perm, &value, &units, &tag_id) {
            Ok(v) => v,
            Err(e) => return Ok(fw_error(e)),
        };

        let ts = now_iso();
        let address = parsed.address();

        let raw_result: Result<(), AdapterError> = match parsed {
            ParsedTag::Coil { .. } => {
                let b = match write_value {
                    WriteValue::Bool(b) => b,
                    WriteValue::Float(_) => {
                        return Ok(fw_error(AdapterError {
                            code: ErrorCode::InvalidValue,
                            message: "coil writes require a boolean value.".into(),
                            tag_id: Some(tag_id.clone()),
                        }));
                    }
                };
                timed(
                    conn.ctx.write_single_coil(address, b),
                    DEFAULT_TIMEOUT_MS,
                    &tag_id,
                )
                .await
                .and_then(|r| map_write_result(r, &tag_id))
            }
            ParsedTag::Holding { data_type, .. } => {
                let f = match write_value {
                    WriteValue::Float(f) => f,
                    WriteValue::Bool(b) => {
                        if b {
                            1.0
                        } else {
                            0.0
                        }
                    }
                };
                let regs = match value_to_registers(&json!(f), data_type, &tag_id) {
                    Ok(r) => r,
                    Err(e) => return Ok(fw_error(e)),
                };
                let raw = if regs.len() == 1 {
                    timed(
                        conn.ctx.write_single_register(address, regs[0]),
                        DEFAULT_TIMEOUT_MS,
                        &tag_id,
                    )
                    .await
                } else {
                    timed(
                        conn.ctx.write_multiple_registers(address, &regs),
                        DEFAULT_TIMEOUT_MS,
                        &tag_id,
                    )
                    .await
                };
                raw.and_then(|r| map_write_result(r, &tag_id))
            }
            ParsedTag::Input { .. } | ParsedTag::Discrete { .. } => {
                unreachable!("is_writable_type already rejected Input/Discrete")
            }
        };

        match raw_result {
            Ok(()) => {
                log_write_audit(&tag_id, &value, &units, &operator_id, &reason);
                Ok(CallToolResult::structured(json!({
                    "tag_id": tag_id,
                    "value_written": write_value,
                    "quality": Quality::Good,
                    "timestamp": ts,
                    "units": units,
                    "operator_id": operator_id,
                })))
            }
            Err(e) => {
                record_and_maybe_clear(&mut inner, &e);
                Ok(fw_error(e))
            }
        }
    }

    // ── get_server_info ───────────────────────────────────────────────────────

    #[tool(
        description = "Return metadata about this adapter and its current connection state. \
        Called by Cascade on initialization to discover available protocol surfaces and health."
    )]
    async fn get_server_info(&self) -> Result<CallToolResult, McpError> {
        let inner = self.inner.lock().await;
        match inner.as_ref() {
            None => Ok(CallToolResult::structured(json!({
                "server_name": "modbus-mcp",
                "protocol": "Modbus TCP",
                "protocol_version": "unknown",
                "connected": false,
                "connection_state": "disconnected",
                "capabilities": CAPABILITIES,
                "uptime_seconds": 0,
                "last_error": null,
            }))),
            Some(conn) => {
                let uptime = conn.connected_at.elapsed().as_secs();
                Ok(CallToolResult::structured(json!({
                    "server_name": format!("modbus-mcp@{}:{}", conn.host, conn.port),
                    "protocol": "Modbus TCP",
                    "protocol_version": "Modbus Application Protocol V1.1b3",
                    "connected": true,
                    "connection_state": "connected",
                    "slave_id": conn.slave.0,
                    "capabilities": CAPABILITIES,
                    "uptime_seconds": uptime,
                    "last_error": conn.last_error,
                })))
            }
        }
    }
}

// ── ServerHandler ─────────────────────────────────────────────────────────────

#[tool_handler]
impl ServerHandler for ModbusMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
            .with_protocol_version(ProtocolVersion::V_2026_07_28)
            .with_instructions(
                "FieldWorks Modbus TCP adapter. Exposes Modbus holding/input registers and \
                 coil/discrete-input data through the standard nine-tool FieldWorks interface. \
                 Call connect before any data operations."
                    .to_string(),
            )
    }
}
