#![allow(dead_code)]

use crate::connection::{
    browse_flat, browse_tree, data_value_to_vqt, establish_session, extract_history_data,
    is_node_id_unknown, load_topology, log_write_audit, make_history_read_details,
    make_history_value_id, make_opc_write_value, make_read_value_id, map_write_status_code,
    now_iso, parse_node_id, topology_tag_to_descriptor, validate_write, OpcUaConnection,
};
use async_opcua::{
    client::IdentityToken,
    types::{DateTime, MessageSecurityMode, NodeId, StatusCode, TimestampsToReturn},
};
use chrono::{SecondsFormat, Utc};
use fieldworks_adapter_core::*;
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Implementation, ProtocolVersion, ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

// ── Shared helpers ────────────────────────────────────────────────────────────

fn fw_error(err: AdapterError) -> CallToolResult {
    CallToolResult::structured_error(json!({ "error": err }))
}

fn not_connected(tag_id: Option<&str>) -> CallToolResult {
    fw_error(AdapterError {
        code: ErrorCode::ConnectionError,
        message: "not connected — call connect first".into(),
        tag_id: tag_id.map(String::from),
    })
}

fn opc_err(sc: StatusCode, tag_id: Option<&str>, ctx: &str) -> CallToolResult {
    fw_error(AdapterError {
        code: ErrorCode::ConnectionError,
        message: format!("{ctx}: {sc}"),
        tag_id: tag_id.map(String::from),
    })
}

fn opt_str<'a>(options: &'a Option<serde_json::Value>, key: &str) -> Option<&'a str> {
    options.as_ref()?.get(key)?.as_str()
}

const CAPABILITIES: &[&str] = &[
    "connect",
    "disconnect",
    "discover_tags",
    "browse",
    "get_node_tree",
    "read_tag",
    "read_tag_history",
    "write_tag",
    "get_server_info",
];

// ── Parameter structs ─────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ConnectParams {
    #[schemars(
        description = "OPC-UA endpoint URL or hostname. If it starts with opc.tcp:// it is used as-is; otherwise opc.tcp://<host>:<port> is constructed."
    )]
    host: String,
    #[schemars(
        description = "Port number. Default 4840. Ignored if host is a full opc.tcp:// URL."
    )]
    port: Option<u16>,
    #[schemars(description = "Connection and session timeout in milliseconds. Default 10000.")]
    timeout_ms: Option<u32>,
    /// Optional keys: security_mode (None|Sign|SignAndEncrypt), security_policy (None|Basic256Sha256|Aes128Sha256RsaOaep), username, password
    options: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct DisconnectParams {
    #[schemars(description = "Optional reason for disconnect (informational only).")]
    reason: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct DiscoverTagsParams {
    #[schemars(description = "Filter by process area. Omit to return all.")]
    process_area: Option<String>,
    #[schemars(description = "Filter by equipment ID. Omit to return all.")]
    equipment_id: Option<String>,
    include_metadata: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct BrowseParams {
    #[schemars(description = "Starting NodeId. Default: Objects folder (ns=0;i=85).")]
    node_id: Option<String>,
    #[schemars(description = "How many levels deep to recurse. Default 1, max 5.")]
    depth: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct GetNodeTreeParams {
    #[schemars(description = "Root NodeId for the tree dump. Default: Root folder (ns=0;i=84).")]
    root_node: Option<String>,
    #[schemars(description = "Maximum depth. Default 3, max 6.")]
    depth: Option<u32>,
    #[schemars(description = "Read current value for Variable nodes. Default false.")]
    include_values: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ReadTagParams {
    #[schemars(description = "OPC-UA NodeId string. Example: ns=2;s=Pump01.FlowRate")]
    tag_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ReadTagHistoryParams {
    #[schemars(description = "OPC-UA NodeId string.")]
    tag_id: String,
    #[schemars(
        description = "Start of history window. ISO 8601 UTC. Example: 2026-06-09T00:00:00Z"
    )]
    start_time: String,
    #[schemars(description = "End of history window. ISO 8601 UTC.")]
    end_time: String,
    #[schemars(description = "Maximum number of data points to return. Default 1000.")]
    max_points: Option<u32>,
    #[schemars(description = "Filter by quality: good | uncertain | bad. Omit for all.")]
    quality_filter: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct WriteTagParams {
    #[schemars(description = "OPC-UA NodeId string.")]
    tag_id: String,
    #[schemars(description = "Value to write. Number or boolean. Strings not accepted.")]
    value: serde_json::Value,
    #[schemars(
        description = "Engineering units. Must match the write_permissions entry if one is configured."
    )]
    units: String,
    #[schemars(description = "Operator identity for the audit log.")]
    operator_id: String,
    #[schemars(description = "Reason for the write, for the audit log.")]
    reason: String,
}

// ── Server struct ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct OpcUaMcpServer {
    inner: Arc<tokio::sync::Mutex<Option<OpcUaConnection>>>,
    tool_router: ToolRouter<OpcUaMcpServer>,
}

#[tool_router]
impl OpcUaMcpServer {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(None)),
            tool_router: Self::tool_router(),
        }
    }

    // ── connect ───────────────────────────────────────────────────────────────

    #[tool(
        description = "Establish an OPC-UA session. Supports SecurityMode None/Sign/SignAndEncrypt and Anonymous or Username/Password identity. Tag IDs are OPC-UA NodeId strings (e.g. ns=2;s=Pump01.FlowRate)."
    )]
    async fn connect(
        &self,
        Parameters(p): Parameters<ConnectParams>,
    ) -> Result<CallToolResult, McpError> {
        let endpoint_url = if p.host.starts_with("opc.tcp://") {
            p.host.clone()
        } else {
            format!("opc.tcp://{}:{}", p.host, p.port.unwrap_or(4840))
        };
        let timeout_ms = p.timeout_ms.unwrap_or(10_000);

        let security_mode = match opt_str(&p.options, "security_mode").unwrap_or("None") {
            "Sign" => MessageSecurityMode::Sign,
            "SignAndEncrypt" => MessageSecurityMode::SignAndEncrypt,
            _ => MessageSecurityMode::None,
        };
        let security_policy = opt_str(&p.options, "security_policy")
            .unwrap_or("None")
            .to_string();
        let identity = match opt_str(&p.options, "username") {
            Some(u) => {
                let pw = opt_str(&p.options, "password").unwrap_or("");
                IdentityToken::new_user_name(u, pw)
            }
            None => IdentityToken::Anonymous,
        };

        // Tear down any existing session.
        {
            let mut guard = self.inner.lock().await;
            if let Some(old) = guard.take() {
                let _ = old.session.disconnect().await;
                old.handle.abort();
            }
        }

        let (session, handle) = match establish_session(
            &endpoint_url,
            security_mode,
            &security_policy,
            identity,
            timeout_ms,
        )
        .await
        {
            Ok(pair) => pair,
            Err(e) => return Ok(fw_error(e)),
        };

        session.wait_for_connection().await;

        let topology = load_topology();
        let connected_at = std::time::Instant::now();

        let resp = ConnectResponse {
            connected: true,
            server_name: endpoint_url.clone(),
            protocol_version: "1.05".into(),
            timestamp: now_iso(),
        };

        let mut guard = self.inner.lock().await;
        *guard = Some(OpcUaConnection {
            session,
            handle,
            endpoint_url,
            server_name: resp.server_name.clone(),
            protocol_version: resp.protocol_version.clone(),
            connected_at,
            topology,
        });

        Ok(CallToolResult::structured(
            serde_json::to_value(resp).unwrap(),
        ))
    }

    // ── disconnect ────────────────────────────────────────────────────────────

    #[tool(
        description = "Close the OPC-UA session cleanly, sending a CloseSession request to the server."
    )]
    async fn disconnect(
        &self,
        Parameters(_p): Parameters<DisconnectParams>,
    ) -> Result<CallToolResult, McpError> {
        let conn = self.inner.lock().await.take();
        if let Some(c) = conn {
            let _ = c.session.disconnect().await;
            c.handle.abort();
        }
        Ok(CallToolResult::structured(
            serde_json::to_value(DisconnectResponse {
                disconnected: true,
                timestamp: now_iso(),
            })
            .unwrap(),
        ))
    }

    // ── discover_tags ─────────────────────────────────────────────────────────

    #[tool(
        description = "Return tag descriptors from topology.yaml. Returns an empty list if no topology is loaded — run browse or get_node_tree to explore the address space."
    )]
    async fn discover_tags(
        &self,
        Parameters(p): Parameters<DiscoverTagsParams>,
    ) -> Result<CallToolResult, McpError> {
        let guard = self.inner.lock().await;
        if guard.is_none() {
            return Ok(not_connected(None));
        }
        let topology = &guard.as_ref().unwrap().topology;

        let tags: Vec<TagDescriptor> = topology
            .tags
            .iter()
            .filter(|t| {
                p.process_area.as_ref().is_none_or(|f| &t.process_area == f)
                    && p.equipment_id.as_ref().is_none_or(|f| &t.equipment_id == f)
            })
            .map(topology_tag_to_descriptor)
            .collect();

        Ok(CallToolResult::structured(json!({ "tags": tags })))
    }

    // ── browse ────────────────────────────────────────────────────────────────

    #[tool(
        description = "Browse the OPC-UA address space from a starting node. Returns child nodes with their NodeId, browse name, display name, and node class. Use discover_tags for tag metadata."
    )]
    async fn browse(
        &self,
        Parameters(p): Parameters<BrowseParams>,
    ) -> Result<CallToolResult, McpError> {
        let session = {
            let guard = self.inner.lock().await;
            match guard.as_ref() {
                Some(c) => c.session.clone(),
                None => return Ok(not_connected(None)),
            }
        };

        let start = match p.node_id.as_deref() {
            Some(s) => match parse_node_id(s) {
                Ok(n) => n,
                Err(e) => return Ok(fw_error(e)),
            },
            None => NodeId::objects_folder_id(),
        };
        let depth = p.depth.unwrap_or(1).min(5);

        let entries = match browse_flat(&session, start, depth).await {
            Ok(e) => e,
            Err(e) => return Ok(fw_error(e)),
        };

        let result: Vec<serde_json::Value> = entries
            .into_iter()
            .map(|e| {
                json!({
                    "node_id": e.node_id,
                    "browse_name": e.browse_name,
                    "display_name": e.display_name,
                    "node_class": e.node_class,
                })
            })
            .collect();

        Ok(CallToolResult::structured(json!({ "entries": result })))
    }

    // ── get_node_tree ─────────────────────────────────────────────────────────

    #[tool(
        description = "Return a nested view of the OPC-UA address space. Intentionally slow — use only for topology onboarding. Hard cap: 1000 nodes. Use browse for incremental exploration."
    )]
    async fn get_node_tree(
        &self,
        Parameters(p): Parameters<GetNodeTreeParams>,
    ) -> Result<CallToolResult, McpError> {
        let session = {
            let guard = self.inner.lock().await;
            match guard.as_ref() {
                Some(c) => c.session.clone(),
                None => return Ok(not_connected(None)),
            }
        };

        let root = match p.root_node.as_deref() {
            Some(s) => match parse_node_id(s) {
                Ok(n) => n,
                Err(e) => return Ok(fw_error(e)),
            },
            None => NodeId::root_folder_id(),
        };
        let depth = p.depth.unwrap_or(3).min(6);
        let include_values = p.include_values.unwrap_or(false);

        let mut count = 0usize;
        let tree = browse_tree(&session, root, depth, include_values, &mut count).await;
        let truncated = count >= 1000;

        Ok(CallToolResult::structured(json!({
            "tree": tree,
            "node_count": count,
            "truncated": truncated,
        })))
    }

    // ── read_tag ──────────────────────────────────────────────────────────────

    #[tool(
        description = "Read the current value of an OPC-UA Variable node by NodeId. Returns a VQT envelope with status code mapped to quality (Good/Uncertain/Bad)."
    )]
    async fn read_tag(
        &self,
        Parameters(p): Parameters<ReadTagParams>,
    ) -> Result<CallToolResult, McpError> {
        let (session, units) = {
            let guard = self.inner.lock().await;
            match guard.as_ref() {
                Some(c) => {
                    let units = c
                        .topology
                        .tags
                        .iter()
                        .find(|t| t.tag_id == p.tag_id)
                        .map(|t| t.units.clone())
                        .unwrap_or_default();
                    (c.session.clone(), units)
                }
                None => return Ok(not_connected(Some(&p.tag_id))),
            }
        };

        let node_id = match parse_node_id(&p.tag_id) {
            Ok(n) => n,
            Err(e) => return Ok(fw_error(e)),
        };

        let mut dvs = match session
            .read(
                &[make_read_value_id(node_id)],
                TimestampsToReturn::Both,
                0.0,
            )
            .await
        {
            Ok(v) => v,
            Err(sc) => return Ok(opc_err(sc, Some(&p.tag_id), "read_tag")),
        };

        match dvs.pop() {
            // BadNodeIdUnknown means the NodeId doesn't refer to anything in
            // the server's address space — that's TAG_NOT_FOUND, not a VQT
            // with quality=bad. Every other Bad status (device failure,
            // sensor fault, comm loss downstream of the OPC-UA server, etc.)
            // is a real reading the server is telling us not to trust, which
            // is exactly what VQT's Bad quality exists to represent — those
            // still return a VQT, not an error.
            Some(dv) if is_node_id_unknown(dv.status) => Ok(fw_error(AdapterError {
                code: ErrorCode::TagNotFound,
                message: format!(
                    "NodeId '{}' does not exist on the server (BadNodeIdUnknown)",
                    p.tag_id
                ),
                tag_id: Some(p.tag_id),
            })),
            Some(dv) => Ok(CallToolResult::structured(
                serde_json::to_value(data_value_to_vqt(&p.tag_id, &dv, &units)).unwrap(),
            )),
            None => Ok(fw_error(AdapterError {
                code: ErrorCode::TagNotFound,
                message: format!("no result returned for {}", p.tag_id),
                tag_id: Some(p.tag_id),
            })),
        }
    }

    // ── read_tag_history ──────────────────────────────────────────────────────

    #[tool(
        description = "Read historical values for an OPC-UA node that exposes HistoricalAccess. Returns HISTORY_UNAVAILABLE if the server does not support history for this node."
    )]
    async fn read_tag_history(
        &self,
        Parameters(p): Parameters<ReadTagHistoryParams>,
    ) -> Result<CallToolResult, McpError> {
        let (session, units) = {
            let guard = self.inner.lock().await;
            match guard.as_ref() {
                Some(c) => {
                    let units = c
                        .topology
                        .tags
                        .iter()
                        .find(|t| t.tag_id == p.tag_id)
                        .map(|t| t.units.clone())
                        .unwrap_or_default();
                    (c.session.clone(), units)
                }
                None => return Ok(not_connected(Some(&p.tag_id))),
            }
        };

        let node_id = match parse_node_id(&p.tag_id) {
            Ok(n) => n,
            Err(e) => return Ok(fw_error(e)),
        };

        let start = match DateTime::parse_from_rfc3339(&p.start_time) {
            Ok(dt) => dt,
            Err(_) => {
                return Ok(fw_error(AdapterError {
                    code: ErrorCode::InvalidValue,
                    message: format!("'{}' is not a valid ISO 8601 timestamp", p.start_time),
                    tag_id: Some(p.tag_id),
                }))
            }
        };
        let end = match DateTime::parse_from_rfc3339(&p.end_time) {
            Ok(dt) => dt,
            Err(_) => {
                return Ok(fw_error(AdapterError {
                    code: ErrorCode::InvalidValue,
                    message: format!("'{}' is not a valid ISO 8601 timestamp", p.end_time),
                    tag_id: Some(p.tag_id),
                }))
            }
        };
        let max_points = p.max_points.unwrap_or(1000);

        let history_results = match session
            .history_read(
                make_history_read_details(start, end, max_points),
                TimestampsToReturn::Source,
                false,
                &[make_history_value_id(node_id)],
            )
            .await
        {
            Ok(r) => r,
            Err(sc) => {
                return Ok(fw_error(AdapterError {
                    code: ErrorCode::HistoryUnavailable,
                    message: format!("history_read failed: {sc}"),
                    tag_id: Some(p.tag_id),
                }));
            }
        };

        let mut vqts: Vec<Vqt> = Vec::new();

        for hr in history_results {
            if hr.status_code.is_bad() {
                return Ok(fw_error(AdapterError {
                    code: ErrorCode::HistoryUnavailable,
                    message: format!("server returned: {}", hr.status_code),
                    tag_id: Some(p.tag_id.clone()),
                }));
            }

            for dv in extract_history_data(hr.history_data) {
                let vqt = data_value_to_vqt(&p.tag_id, &dv, &units);

                if let Some(ref filter) = p.quality_filter {
                    let keep = match filter.as_str() {
                        "good" => matches!(vqt.quality, Quality::Good),
                        "uncertain" => matches!(vqt.quality, Quality::Uncertain),
                        "bad" => matches!(vqt.quality, Quality::Bad),
                        _ => true,
                    };
                    if !keep {
                        continue;
                    }
                }

                vqts.push(vqt);
            }
        }

        Ok(CallToolResult::structured(json!({ "values": vqts })))
    }

    // ── write_tag ─────────────────────────────────────────────────────────────

    #[tool(
        description = "Write a value to an OPC-UA Variable node. Gated by topology write_permissions. Requires operator_id and reason for audit trail."
    )]
    async fn write_tag(
        &self,
        Parameters(p): Parameters<WriteTagParams>,
    ) -> Result<CallToolResult, McpError> {
        let (session, perm) = {
            let guard = self.inner.lock().await;
            match guard.as_ref() {
                Some(c) => {
                    let perm = c.topology.write_permissions.get(&p.tag_id).cloned();
                    (c.session.clone(), perm)
                }
                None => return Ok(not_connected(Some(&p.tag_id))),
            }
        };

        let perm = match perm {
            Some(p) => p,
            None => {
                return Ok(fw_error(AdapterError {
                    code: ErrorCode::TagNotWritable,
                    message: format!(
                    "'{}' is not in write_permissions — add it to topology.yaml to enable writes",
                    p.tag_id
                ),
                    tag_id: Some(p.tag_id),
                }))
            }
        };

        let (variant, write_value) = match validate_write(&perm, &p.value, &p.units, &p.tag_id) {
            Ok(pair) => pair,
            Err(e) => return Ok(fw_error(e)),
        };

        let node_id = match parse_node_id(&p.tag_id) {
            Ok(n) => n,
            Err(e) => return Ok(fw_error(e)),
        };

        let status_codes = match session
            .write(&[make_opc_write_value(node_id, variant)])
            .await
        {
            Ok(v) => v,
            Err(sc) => return Ok(opc_err(sc, Some(&p.tag_id), "write_tag")),
        };

        if let Some(sc) = status_codes.first() {
            if sc.is_bad() {
                return Ok(fw_error(AdapterError {
                    code: map_write_status_code(*sc),
                    message: format!("write rejected by server: {sc}"),
                    tag_id: Some(p.tag_id),
                }));
            }
        }

        log_write_audit(&p.tag_id, &p.value, &p.units, &p.operator_id, &p.reason);

        let quality = status_codes
            .first()
            .map(|sc| {
                if sc.is_good() {
                    Quality::Good
                } else if sc.is_uncertain() {
                    Quality::Uncertain
                } else {
                    Quality::Bad
                }
            })
            .unwrap_or(Quality::Good);

        Ok(CallToolResult::structured(
            serde_json::to_value(WriteConfirmation {
                tag_id: p.tag_id,
                value_written: write_value,
                quality,
                timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
                units: p.units,
                operator_id: p.operator_id,
            })
            .unwrap(),
        ))
    }

    // ── get_server_info ───────────────────────────────────────────────────────

    #[tool(
        description = "Return OPC-UA server metadata, connection state, and supported capabilities."
    )]
    async fn get_server_info(&self) -> Result<CallToolResult, McpError> {
        let guard = self.inner.lock().await;
        let (connected, conn_state, server_name, protocol_version, uptime, last_error) =
            match guard.as_ref() {
                Some(c) => {
                    let uptime = c.connected_at.elapsed().as_secs();
                    (
                        true,
                        "connected".to_string(),
                        c.server_name.clone(),
                        c.protocol_version.clone(),
                        uptime,
                        None,
                    )
                }
                None => (
                    false,
                    "disconnected".to_string(),
                    String::new(),
                    String::new(),
                    0u64,
                    None,
                ),
            };

        Ok(CallToolResult::structured(
            serde_json::to_value(ServerInfoResponse {
                server_name,
                protocol: "OPC-UA".into(),
                protocol_version,
                connected,
                connection_state: conn_state,
                capabilities: CAPABILITIES.iter().map(|s| s.to_string()).collect(),
                uptime_seconds: uptime,
                last_error,
            })
            .unwrap(),
        ))
    }
}

// ── ServerHandler ─────────────────────────────────────────────────────────────

#[tool_handler]
impl ServerHandler for OpcUaMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
            .with_protocol_version(ProtocolVersion::V_2024_11_05)
            .with_instructions(
                "FieldWorks OPC-UA adapter. Exposes OPC-UA process data through the standard \
                 nine-tool FieldWorks interface. Call connect before any data operations. \
                 Tag IDs are OPC-UA NodeId strings (e.g. ns=2;s=Pump01.FlowRate). \
                 Use browse or get_node_tree to discover available nodes."
                    .to_string(),
            )
    }
}
