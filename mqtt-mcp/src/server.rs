#![allow(dead_code)]

use crate::connection::{
    self, build_topic_tree, load_topology, log_write_audit, parse_mqtt_payload,
    topology_tag_to_descriptor, vqt_to_descriptor, ConnStatus, MqttConnection,
};
use chrono::{SecondsFormat, Utc};
use fieldworks_adapter_core::*;
use rmcp::{
    ErrorData as McpError, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{
        CallToolResult, Implementation, ProtocolVersion, ServerCapabilities, ServerInfo,
    },
    schemars,
    tool, tool_handler, tool_router,
};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, watch};

// ── Parameter structs ─────────────────────────────────────────────────────────

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct ConnectParams {
    #[schemars(description = "Hostname or IP address of the MQTT broker.")]
    host: String,
    #[schemars(description = "Port number. Typically 1883 (plain) or 8883 (TLS).")]
    port: u16,
    #[schemars(description = "Connection timeout in milliseconds. Default 5000.")]
    timeout_ms: Option<u32>,
    #[schemars(
        description = "Protocol-specific options object. Supported keys: \
        client_id (string), username (string), password (string), \
        keep_alive_secs (integer), clean_session (bool)."
    )]
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
    #[schemars(
        description = "MQTT wildcard subscription pattern. Supports + (single level) and # (multi-level). Default: #"
    )]
    topic_pattern: Option<String>,
    #[schemars(description = "Observation window in milliseconds. Default 5000.")]
    scan_duration_ms: Option<u32>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct GetTopicTreeParams {
    #[schemars(
        description = "Topic prefix to scope the tree (e.g. 'factory/pump01'). Default: empty (full tree)."
    )]
    topic_prefix: Option<String>,
    #[schemars(
        description = "Include current VQT values for leaf nodes. Significantly increases response size. Default false."
    )]
    include_values: Option<bool>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct ReadTagParams {
    #[schemars(description = "Tag identifier (MQTT topic path) as returned by discover_tags or scan.")]
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
    #[schemars(description = "Tag identifier. Must appear in topology write_permissions.")]
    tag_id: String,
    #[schemars(
        description = "Value to write: number or boolean. Strings are not writable. \
        Must be within the configured engineering range."
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

fn fw_error(err: AdapterError) -> CallToolResult {
    CallToolResult::structured_error(json!({ "error": err }))
}

fn not_connected(tag_id: Option<&str>) -> CallToolResult {
    fw_error(AdapterError {
        code: ErrorCode::ConnectionError,
        message: "Not connected to an MQTT broker. Call connect first.".into(),
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

// ── Server struct ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct MqttMcpServer {
    inner: Arc<tokio::sync::Mutex<Option<MqttConnection>>>,
    tool_router: ToolRouter<MqttMcpServer>,
}

impl MqttMcpServer {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(None)),
            tool_router: Self::tool_router(),
        }
    }
}

// ── Tool implementations ──────────────────────────────────────────────────────

#[tool_router]
impl MqttMcpServer {
    // ── connect ───────────────────────────────────────────────────────────────

    #[tool(description = "Establish connection to the MQTT broker. If already connected, the existing connection is replaced.")]
    async fn connect(
        &self,
        Parameters(ConnectParams { host, port, timeout_ms, options }): Parameters<ConnectParams>,
    ) -> Result<CallToolResult, McpError> {
        let mut inner = self.inner.lock().await;

        // Tear down any existing connection first.
        if let Some(old) = inner.take() {
            let _ = old.client.disconnect().await;
            old.handle.abort();
        }

        // Build MqttOptions from params + options blob.
        let client_id = opt_str(&options, "client_id").unwrap_or("fieldworks-mqtt-mcp");
        let mut mqtt_opts = MqttOptions::new(client_id, &host, port);

        let keep_alive = options
            .as_ref()
            .and_then(|o| o.get("keep_alive_secs"))
            .and_then(|v| v.as_u64())
            .unwrap_or(60);
        mqtt_opts.set_keep_alive(Duration::from_secs(keep_alive));

        let clean_session = options
            .as_ref()
            .and_then(|o| o.get("clean_session"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        mqtt_opts.set_clean_session(clean_session);

        if let (Some(u), Some(p)) = (opt_str(&options, "username"), opt_str(&options, "password"))
        {
            mqtt_opts.set_credentials(u, p);
        }

        let (client, event_loop) = AsyncClient::new(mqtt_opts, 64);
        let cache = Arc::new(RwLock::new(HashMap::new()));
        let (msg_tx, _) = broadcast::channel(1024);
        let (status_tx, status_rx) = watch::channel(ConnStatus::Connecting);
        let last_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

        let handle = tokio::spawn(connection::run_event_loop(
            event_loop,
            cache.clone(),
            msg_tx.clone(),
            status_tx,
            last_error.clone(),
        ));

        // Wait for ConnAck or error within timeout.
        let timeout_dur = Duration::from_millis(timeout_ms.unwrap_or(5000) as u64);
        let mut wait_rx = status_rx.clone();

        let result = tokio::time::timeout(timeout_dur, async {
            loop {
                match &*wait_rx.borrow_and_update() {
                    ConnStatus::Connected => return Ok(()),
                    ConnStatus::Error(e) => return Err(e.clone()),
                    ConnStatus::Connecting => {}
                }
                if wait_rx.changed().await.is_err() {
                    return Err("connection channel closed".into());
                }
            }
        })
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                handle.abort();
                return Ok(fw_error(AdapterError {
                    code: ErrorCode::ConnectionError,
                    message: format!("Failed to connect to {host}:{port} — {e}"),
                    tag_id: None,
                }));
            }
            Err(_) => {
                handle.abort();
                return Ok(fw_error(AdapterError {
                    code: ErrorCode::Timeout,
                    message: format!(
                        "Connection to {host}:{port} timed out after {}ms",
                        timeout_ms.unwrap_or(5000)
                    ),
                    tag_id: None,
                }));
            }
        }

        let topology = load_topology();

        *inner = Some(MqttConnection {
            client,
            handle,
            host: host.clone(),
            port,
            protocol_version: "3.1.1".into(),
            connected_at: Instant::now(),
            cache,
            msg_tx,
            status_rx,
            last_error,
            topology,
        });

        Ok(CallToolResult::structured(json!({
            "connected": true,
            "server_name": format!("{host}:{port}"),
            "protocol_version": "3.1.1",
            "timestamp": now_iso(),
        })))
    }

    // ── disconnect ────────────────────────────────────────────────────────────

    #[tool(
        description = "Cleanly terminate the connection to the MQTT broker. \
        Sends MQTT DISCONNECT before closing."
    )]
    async fn disconnect(
        &self,
        Parameters(DisconnectParams { reason }): Parameters<DisconnectParams>,
    ) -> Result<CallToolResult, McpError> {
        let mut inner = self.inner.lock().await;
        let ts = now_iso();

        match inner.take() {
            None => {
                // Already disconnected — return success, it's idempotent.
                Ok(CallToolResult::structured(json!({
                    "disconnected": true,
                    "timestamp": ts,
                })))
            }
            Some(conn) => {
                if let Some(r) = &reason {
                    tracing::info!("disconnect requested: {r}");
                }
                let _ = conn.client.disconnect().await;
                // Give the event loop ~50ms to flush the DISCONNECT packet.
                tokio::time::sleep(Duration::from_millis(50)).await;
                conn.handle.abort();

                Ok(CallToolResult::structured(json!({
                    "disconnected": true,
                    "timestamp": ts,
                })))
            }
        }
    }

    // ── discover_tags ─────────────────────────────────────────────────────────

    #[tool(
        description = "Enumerate available tags with metadata. Returns topology.yaml tags when present, \
        falling back to the in-memory topic cache populated by scan and read_tag. \
        Supports optional filtering by process_area or equipment_id."
    )]
    async fn discover_tags(
        &self,
        Parameters(DiscoverTagsParams { process_area, equipment_id, include_metadata: _ }): Parameters<
            DiscoverTagsParams,
        >,
    ) -> Result<CallToolResult, McpError> {
        let inner = self.inner.lock().await;
        match inner.as_ref() {
            None => return Ok(not_connected(None)),
            Some(conn) => {
                let mut tags: Vec<TagDescriptor> = if conn.topology.tags.is_empty() {
                    // No topology loaded — return the scan cache as thin descriptors.
                    conn.cache
                        .read()
                        .unwrap()
                        .values()
                        .map(vqt_to_descriptor)
                        .collect()
                } else {
                    conn.topology.tags.iter().map(topology_tag_to_descriptor).collect()
                };

                // Apply filters.
                if let Some(ref pa) = process_area {
                    tags.retain(|t| &t.process_area == pa);
                }
                if let Some(ref eq) = equipment_id {
                    tags.retain(|t| &t.equipment_id == eq);
                }

                Ok(CallToolResult::structured(serde_json::to_value(&tags).unwrap_or(json!([]))))
            }
        }
    }

    // ── scan ──────────────────────────────────────────────────────────────────

    #[tool(
        description = "Discover active MQTT topics by subscribing to a wildcard pattern and \
        collecting published messages over a defined observation window. \
        Returns the set of topics seen during the window with their last VQT."
    )]
    async fn scan(
        &self,
        Parameters(ScanParams { topic_pattern, scan_duration_ms }): Parameters<ScanParams>,
    ) -> Result<CallToolResult, McpError> {
        let (client, msg_tx) = {
            let inner = self.inner.lock().await;
            match inner.as_ref() {
                None => return Ok(not_connected(None)),
                Some(c) => (c.client.clone(), c.msg_tx.clone()),
            }
        };

        let pattern = topic_pattern.as_deref().unwrap_or("#").to_string();
        let duration = Duration::from_millis(scan_duration_ms.unwrap_or(5000) as u64);

        // Subscribe to broadcast BEFORE the MQTT subscribe so we don't miss
        // the first flush of retained messages.
        let mut rx = msg_tx.subscribe();

        if let Err(e) = client.subscribe(&pattern, QoS::AtMostOnce).await {
            return Ok(fw_error(AdapterError {
                code: ErrorCode::ConnectionError,
                message: format!("Failed to subscribe to '{pattern}': {e}"),
                tag_id: None,
            }));
        }

        let mut seen: HashMap<String, serde_json::Value> = HashMap::new();

        let _ = tokio::time::timeout(duration, async {
            loop {
                match rx.recv().await {
                    Ok(msg) => {
                        let entry = json!({
                            "topic": msg.topic,
                            "last_value": parse_mqtt_payload(&msg.topic, &msg.payload, &msg.timestamp).value,
                            "quality": "good",
                            "timestamp": msg.timestamp,
                            "retain": msg.retain,
                        });
                        seen.insert(msg.topic, entry);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("scan receiver lagged, missed {n} messages");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
        .await;

        let _ = client.unsubscribe(&pattern).await;

        let entries: Vec<serde_json::Value> = seen.into_values().collect();
        Ok(CallToolResult::structured(json!(entries)))
    }

    // ── get_topic_tree ────────────────────────────────────────────────────────

    #[tool(
        description = "Return the complete MQTT topic address space as a nested hierarchical object. \
        Intentionally expensive — scans for the full duration. \
        Use for topology onboarding only, not at query runtime."
    )]
    async fn get_topic_tree(
        &self,
        Parameters(GetTopicTreeParams { topic_prefix, include_values }): Parameters<GetTopicTreeParams>,
    ) -> Result<CallToolResult, McpError> {
        let (client, msg_tx) = {
            let inner = self.inner.lock().await;
            match inner.as_ref() {
                None => return Ok(not_connected(None)),
                Some(c) => (c.client.clone(), c.msg_tx.clone()),
            }
        };

        let prefix = topic_prefix.as_deref().unwrap_or("").to_string();
        let wildcard = if prefix.is_empty() {
            "#".to_string()
        } else {
            format!("{prefix}/#")
        };
        let include_vals = include_values.unwrap_or(false);

        // Use a longer window than scan — the tree is a full snapshot.
        let duration = Duration::from_millis(10_000);

        let mut rx = msg_tx.subscribe();

        if let Err(e) = client.subscribe(&wildcard, QoS::AtMostOnce).await {
            return Ok(fw_error(AdapterError {
                code: ErrorCode::ConnectionError,
                message: format!("Failed to subscribe to '{wildcard}': {e}"),
                tag_id: None,
            }));
        }

        let mut seen: HashMap<String, serde_json::Value> = HashMap::new();

        let _ = tokio::time::timeout(duration, async {
            loop {
                match rx.recv().await {
                    Ok(msg) => {
                        let leaf = if include_vals {
                            let vqt =
                                parse_mqtt_payload(&msg.topic, &msg.payload, &msg.timestamp);
                            json!({
                                "tag_id": vqt.tag_id,
                                "value": vqt.value,
                                "quality": vqt.quality,
                                "timestamp": vqt.timestamp,
                                "units": vqt.units,
                            })
                        } else {
                            json!({ "tag_id": msg.topic })
                        };
                        seen.insert(msg.topic, leaf);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("get_topic_tree receiver lagged, missed {n} messages");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
        .await;

        let _ = client.unsubscribe(&wildcard).await;

        let tree = build_topic_tree(seen);
        Ok(CallToolResult::structured(tree))
    }

    // ── read_tag ──────────────────────────────────────────────────────────────

    #[tool(
        description = "Read the current value of a single tag by its MQTT topic path. \
        Subscribes to the topic, waits for a message (retained or live) within a 10-second \
        timeout, then unsubscribes. Returns a VQT envelope. Quality is always present."
    )]
    async fn read_tag(
        &self,
        Parameters(ReadTagParams { tag_id }): Parameters<ReadTagParams>,
    ) -> Result<CallToolResult, McpError> {
        let (client, cache, msg_tx) = {
            let inner = self.inner.lock().await;
            match inner.as_ref() {
                None => return Ok(not_connected(Some(&tag_id))),
                Some(c) => (c.client.clone(), c.cache.clone(), c.msg_tx.clone()),
            }
        };

        // Subscribe to broadcast BEFORE the MQTT subscribe so we receive
        // the retained message that the broker pushes immediately after SUBSCRIBE.
        let mut rx = msg_tx.subscribe();

        if let Err(e) = client.subscribe(&tag_id, QoS::AtMostOnce).await {
            return Ok(fw_error(AdapterError {
                code: ErrorCode::ConnectionError,
                message: format!("Failed to subscribe to '{tag_id}': {e}"),
                tag_id: Some(tag_id),
            }));
        }

        let topic = tag_id.clone();
        let result = tokio::time::timeout(Duration::from_secs(10), async move {
            loop {
                match rx.recv().await {
                    Ok(msg) if msg.topic == topic => return Some(msg),
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        })
        .await;

        let _ = client.unsubscribe(&tag_id).await;

        match result {
            Ok(Some(msg)) => {
                let vqt = parse_mqtt_payload(&msg.topic, &msg.payload, &msg.timestamp);
                Ok(CallToolResult::structured(serde_json::to_value(&vqt).unwrap()))
            }
            Ok(None) | Err(_) => {
                // Timeout: check the cache as a last resort (may have a value from a prior scan).
                if let Some(cached) = cache.read().unwrap().get(&tag_id) {
                    let mut vqt = cached.clone();
                    vqt.quality = Quality::Uncertain; // stale
                    return Ok(CallToolResult::structured(serde_json::to_value(&vqt).unwrap()));
                }
                Ok(fw_error(AdapterError {
                    code: ErrorCode::Timeout,
                    message: format!(
                        "No message received for tag '{tag_id}' within 10 seconds. \
                        The topic may not exist or no publisher is active."
                    ),
                    tag_id: Some(tag_id),
                }))
            }
        }
    }

    // ── read_tag_history ──────────────────────────────────────────────────────

    #[tool(
        description = "Read a time-series for a tag over a defined window. \
        MQTT v3.1.1 does not provide native history. This adapter returns \
        HISTORY_UNAVAILABLE. Integrate a MQTT-backed historian (e.g. InfluxDB bridge) \
        and extend this method to support historical queries."
    )]
    async fn read_tag_history(
        &self,
        Parameters(ReadTagHistoryParams { tag_id, .. }): Parameters<ReadTagHistoryParams>,
    ) -> Result<CallToolResult, McpError> {
        Ok(fw_error(AdapterError {
            code: ErrorCode::HistoryUnavailable,
            message: "MQTT v3.1.1 does not support native historical data. \
                      Bridge to a time-series store to enable read_tag_history."
                .into(),
            tag_id: Some(tag_id),
        }))
    }

    // ── write_tag ─────────────────────────────────────────────────────────────

    #[tool(
        description = "Write a value to an MQTT topic. Gated by topology write_permissions — \
        any tag not explicitly listed in write_permissions returns TAG_NOT_WRITABLE. \
        Requires operator_id and reason; both are logged to write_audit.jsonl."
    )]
    async fn write_tag(
        &self,
        Parameters(WriteTagParams { tag_id, value, units, operator_id, reason }): Parameters<
            WriteTagParams,
        >,
    ) -> Result<CallToolResult, McpError> {
        let (client, topology_perm) = {
            let inner = self.inner.lock().await;
            match inner.as_ref() {
                None => return Ok(not_connected(Some(&tag_id))),
                Some(c) => {
                    let perm = c.topology.write_permissions.get(&tag_id).cloned();
                    (c.client.clone(), perm)
                }
            }
        };

        // Enforce write permissions.
        let perm = match topology_perm {
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

        // Validate value type: must be number or boolean per spec.
        let write_value = if let Some(f) = value.as_f64() {
            // Range check against topology limits.
            if let Some(min) = perm.min {
                if f < min {
                    return Ok(fw_error(AdapterError {
                        code: ErrorCode::InvalidValue,
                        message: format!(
                            "Value {f} is below the configured minimum {min} for '{tag_id}'."
                        ),
                        tag_id: Some(tag_id),
                    }));
                }
            }
            if let Some(max) = perm.max {
                if f > max {
                    return Ok(fw_error(AdapterError {
                        code: ErrorCode::InvalidValue,
                        message: format!(
                            "Value {f} exceeds the configured maximum {max} for '{tag_id}'."
                        ),
                        tag_id: Some(tag_id),
                    }));
                }
            }
            WriteValue::Float(f)
        } else if let Some(b) = value.as_bool() {
            WriteValue::Bool(b)
        } else {
            return Ok(fw_error(AdapterError {
                code: ErrorCode::InvalidValue,
                message: "write_tag value must be a number or boolean. String values are not writable.".into(),
                tag_id: Some(tag_id),
            }));
        };

        // Units check.
        if let Some(ref configured_units) = perm.units {
            if !configured_units.is_empty() && configured_units != &units {
                return Ok(fw_error(AdapterError {
                    code: ErrorCode::InvalidValue,
                    message: format!(
                        "Units mismatch for '{tag_id}': expected '{configured_units}', got '{units}'."
                    ),
                    tag_id: Some(tag_id),
                }));
            }
        }

        let timestamp = now_iso();

        // Publish to the MQTT topic. Payload is a JSON object so downstream
        // subscribers (and this adapter's own parse_mqtt_payload) can decode it.
        let payload = json!({
            "value": value,
            "quality": "good",
            "timestamp": timestamp,
            "units": units,
        })
        .to_string();

        if let Err(e) = client
            .publish(&tag_id, QoS::AtLeastOnce, false, payload.as_bytes())
            .await
        {
            return Ok(fw_error(AdapterError {
                code: ErrorCode::ConnectionError,
                message: format!("Publish to '{tag_id}' failed: {e}"),
                tag_id: Some(tag_id),
            }));
        }

        // Append audit log entry — mandatory per conformance requirements.
        log_write_audit(&tag_id, &value, &units, &operator_id, &reason);

        Ok(CallToolResult::structured(json!({
            "tag_id": tag_id,
            "value_written": write_value,
            "quality": Quality::Good,
            "timestamp": timestamp,
            "units": units,
            "operator_id": operator_id,
        })))
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
                "server_name": "mqtt-mcp",
                "protocol": "MQTT",
                "protocol_version": "unknown",
                "connected": false,
                "connection_state": "disconnected",
                "capabilities": CAPABILITIES,
                "uptime_seconds": 0,
                "last_error": null,
            }))),
            Some(conn) => {
                let status = conn.status_rx.borrow().clone();
                let last_error = conn.last_error.lock().unwrap().clone();
                let uptime = conn.connected_at.elapsed().as_secs();

                let (connected, conn_state) = match &status {
                    ConnStatus::Connected => (true, "connected".to_string()),
                    ConnStatus::Connecting => (false, "connecting".to_string()),
                    ConnStatus::Error(e) => (false, format!("error: {e}")),
                };

                Ok(CallToolResult::structured(json!({
                    "server_name": format!("mqtt-mcp@{}:{}", conn.host, conn.port),
                    "protocol": "MQTT",
                    "protocol_version": conn.protocol_version,
                    "connected": connected,
                    "connection_state": conn_state,
                    "capabilities": CAPABILITIES,
                    "uptime_seconds": uptime,
                    "last_error": last_error,
                })))
            }
        }
    }
}

// ── ServerHandler ─────────────────────────────────────────────────────────────

#[tool_handler]
impl ServerHandler for MqttMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
            .with_protocol_version(ProtocolVersion::V_2024_11_05)
            .with_instructions(
                "FieldWorks MQTT adapter. Exposes MQTT process data through the standard \
                 nine-tool FieldWorks interface. Call connect before any data operations."
                    .to_string(),
            )
    }
}

// ── Option helpers ────────────────────────────────────────────────────────────

fn opt_str<'a>(options: &'a Option<serde_json::Value>, key: &str) -> Option<&'a str> {
    options.as_ref()?.get(key)?.as_str()
}
