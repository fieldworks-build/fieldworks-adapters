use async_opcua::{
    client::{ClientBuilder, HistoryReadAction, IdentityToken, Session},
    types::{
        AttributeId, BrowseDescription, BrowseDirection, DataValue, DateTime, EndpointDescription,
        HistoryData, HistoryReadValueId, MessageSecurityMode, NodeClass, NodeId, QualifiedName,
        ReadRawModifiedDetails, ReadValueId, StatusCode, TimestampsToReturn, UserTokenPolicy,
        Variant, WriteValue as OpcWriteValue,
    },
};
use chrono::{SecondsFormat, Utc};
use fieldworks_adapter_core::*;
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc, time::Instant};
use tokio::task::JoinHandle;

// ── Topology config ───────────────────────────────────────────────────────────
// Same schema as mqtt-mcp; tag_id values are OPC-UA NodeId strings (ns=N;s=...).

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
    tracing::warn!("no topology.yaml found — discover_tags returns empty list; all writes denied");
    TopologyConfig::default()
}

// ── Active connection ─────────────────────────────────────────────────────────

#[allow(dead_code)]
pub struct OpcUaConnection {
    pub session: Arc<Session>,
    pub handle: JoinHandle<StatusCode>,
    pub endpoint_url: String,
    pub server_name: String,
    pub protocol_version: String,
    pub connected_at: Instant,
    pub topology: TopologyConfig,
}

// ── Session establishment ─────────────────────────────────────────────────────

pub async fn establish_session(
    endpoint_url: &str,
    security_mode: MessageSecurityMode,
    security_policy: &str,
    identity: IdentityToken,
    timeout_ms: u32,
) -> Result<(Arc<Session>, JoinHandle<StatusCode>), AdapterError> {
    let trust_server_certs = security_mode == MessageSecurityMode::None;

    let mut client = ClientBuilder::new()
        .application_name("fieldworks-opcua-mcp")
        .application_uri("urn:fieldworks:opcua-mcp")
        .trust_server_certs(trust_server_certs)
        .create_sample_keypair(security_mode != MessageSecurityMode::None)
        .session_retry_limit(0)
        .session_timeout(timeout_ms)
        .client()
        .map_err(|e| AdapterError {
            code: ErrorCode::ConnectionError,
            message: format!("failed to build OPC-UA client: {e:?}"),
            tag_id: None,
        })?;

    let endpoint: EndpointDescription = (
        endpoint_url,
        security_policy,
        security_mode,
        UserTokenPolicy::anonymous(),
    )
        .into();

    let (session, event_loop) = client
        .connect_to_matching_endpoint(endpoint, identity)
        .await
        .map_err(|e| AdapterError {
            code: ErrorCode::ConnectionError,
            message: format!("failed to connect to OPC-UA server: {e}"),
            tag_id: None,
        })?;

    let handle = event_loop.spawn();
    Ok((session, handle))
}

// ── Type conversions ──────────────────────────────────────────────────────────

pub fn data_value_to_vqt(node_id_str: &str, dv: &DataValue, units: &str) -> Vqt {
    let quality = status_code_to_quality(dv.status);
    let timestamp = dv
        .source_timestamp
        .as_ref()
        .or(dv.server_timestamp.as_ref())
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(now_iso);
    let value = dv
        .value
        .as_ref()
        .map(variant_to_tag_value)
        .unwrap_or(TagValue::Text("null".into()));
    Vqt {
        tag_id: node_id_str.to_string(),
        value,
        quality,
        timestamp,
        units: units.to_string(),
    }
}

pub fn status_code_to_quality(code: Option<StatusCode>) -> Quality {
    match code {
        None => Quality::Good,
        Some(c) if c.is_good() => Quality::Good,
        Some(c) if c.is_uncertain() => Quality::Uncertain,
        _ => Quality::Bad,
    }
}

pub fn variant_to_tag_value(v: &Variant) -> TagValue {
    match v {
        Variant::Boolean(b) => TagValue::Bool(*b),
        Variant::SByte(b) => TagValue::Float(*b as f64),
        Variant::Byte(b) => TagValue::Float(*b as f64),
        Variant::Int16(i) => TagValue::Float(*i as f64),
        Variant::UInt16(i) => TagValue::Float(*i as f64),
        Variant::Int32(i) => TagValue::Float(*i as f64),
        Variant::UInt32(i) => TagValue::Float(*i as f64),
        Variant::Int64(i) => TagValue::Float(*i as f64),
        Variant::UInt64(i) => TagValue::Float(*i as f64),
        Variant::Float(f) => TagValue::Float(*f as f64),
        Variant::Double(f) => TagValue::Float(*f),
        Variant::String(s) => TagValue::Text(s.to_string()),
        other => TagValue::Text(format!("{other:?}")),
    }
}

pub fn parse_node_id(s: &str) -> Result<NodeId, AdapterError> {
    s.parse::<NodeId>().map_err(|_| AdapterError {
        code: ErrorCode::TagNotFound,
        message: format!(
            "'{s}' is not a valid OPC-UA NodeId. Expected format: ns=<ns>;s=<id> or ns=<ns>;i=<id>"
        ),
        tag_id: Some(s.to_string()),
    })
}

pub fn make_opc_write_value(node_id: NodeId, variant: Variant) -> OpcWriteValue {
    OpcWriteValue {
        node_id,
        attribute_id: AttributeId::Value as u32,
        value: DataValue::new_now(variant),
        ..Default::default()
    }
}

pub fn make_read_value_id(node_id: NodeId) -> ReadValueId {
    ReadValueId {
        node_id,
        attribute_id: AttributeId::Value as u32,
        ..Default::default()
    }
}

// ── Browse helpers ────────────────────────────────────────────────────────────

pub struct BrowseEntry {
    pub node_id: String,
    pub browse_name: String,
    pub display_name: String,
    pub node_class: String,
}

/// Iterative breadth-first browse. Hard cap: 1000 nodes total.
pub async fn browse_flat(
    session: &Session,
    start: NodeId,
    max_depth: u32,
) -> Result<Vec<BrowseEntry>, AdapterError> {
    let mut results: Vec<BrowseEntry> = Vec::new();
    let mut queue: Vec<(NodeId, u32)> = vec![(start, 0)];
    let mut idx = 0;

    while idx < queue.len() {
        if results.len() >= 1000 {
            break;
        }
        let (node, depth) = queue[idx].clone();
        idx += 1;

        let browse_results = session
            .browse(
                &[BrowseDescription {
                    node_id: node,
                    browse_direction: BrowseDirection::Forward,
                    reference_type_id: NodeId::new(0, 33u32), // HierarchicalReferences
                    include_subtypes: true,
                    node_class_mask: 0,
                    result_mask: 0x3f,
                }],
                500,
                None,
            )
            .await
            .map_err(|sc| AdapterError {
                code: ErrorCode::ConnectionError,
                message: format!("browse failed: {sc}"),
                tag_id: None,
            })?;

        for br in browse_results {
            for rr in br.references.unwrap_or_default() {
                let node_id_str = rr.node_id.node_id.to_string();
                results.push(BrowseEntry {
                    node_id: node_id_str.clone(),
                    browse_name: rr.browse_name.name.to_string(),
                    display_name: rr.display_name.text.to_string(),
                    node_class: format!("{:?}", rr.node_class),
                });
                if depth + 1 < max_depth && results.len() < 1000 {
                    queue.push((rr.node_id.node_id, depth + 1));
                }
            }
        }
    }

    Ok(results)
}

/// Recursive tree builder — returns nested serde_json::Value. Hard cap: 1000 nodes.
pub async fn browse_tree(
    session: &Session,
    root: NodeId,
    max_depth: u32,
    include_values: bool,
    count: &mut usize,
) -> serde_json::Value {
    if *count >= 1000 || max_depth == 0 {
        return serde_json::Value::Object(serde_json::Map::new());
    }

    let browse_results = match session
        .browse(
            &[BrowseDescription {
                node_id: root,
                browse_direction: BrowseDirection::Forward,
                reference_type_id: NodeId::new(0, 33u32),
                include_subtypes: true,
                node_class_mask: 0,
                result_mask: 0x3f,
            }],
            500,
            None,
        )
        .await
    {
        Ok(r) => r,
        Err(_) => return serde_json::Value::Object(serde_json::Map::new()),
    };

    let mut children = serde_json::Map::new();

    for br in browse_results {
        for rr in br.references.unwrap_or_default() {
            if *count >= 1000 {
                break;
            }
            *count += 1;

            let node_id_str = rr.node_id.node_id.to_string();
            let display = rr.display_name.text.to_string();
            let is_variable = rr.node_class == NodeClass::Variable;

            let mut entry = serde_json::json!({
                "node_id": node_id_str,
                "node_class": format!("{:?}", rr.node_class),
            });

            if include_values && is_variable {
                if let Ok(dvs) = session
                    .read(
                        &[ReadValueId {
                            node_id: rr.node_id.node_id.clone(),
                            attribute_id: AttributeId::Value as u32,
                            ..Default::default()
                        }],
                        TimestampsToReturn::Source,
                        0.0,
                    )
                    .await
                {
                    if let Some(dv) = dvs.into_iter().next() {
                        if let Some(v) = &dv.value {
                            entry["value"] = serde_json::to_value(variant_to_tag_value(v))
                                .unwrap_or(serde_json::Value::Null);
                        }
                    }
                }
            }

            let subtree = Box::pin(browse_tree(
                session,
                rr.node_id.node_id.clone(),
                max_depth - 1,
                include_values,
                count,
            ))
            .await;
            if !subtree.as_object().map(|m| m.is_empty()).unwrap_or(false) {
                entry["children"] = subtree;
            }

            children.insert(display, entry);
        }
    }

    serde_json::Value::Object(children)
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

// ── History helpers ───────────────────────────────────────────────────────────

pub fn make_history_read_details(
    start_time: DateTime,
    end_time: DateTime,
    max_points: u32,
) -> HistoryReadAction {
    HistoryReadAction::ReadRawModifiedDetails(ReadRawModifiedDetails {
        is_read_modified: false,
        start_time,
        end_time,
        num_values_per_node: max_points,
        return_bounds: false,
    })
}

pub fn make_history_value_id(node_id: NodeId) -> HistoryReadValueId {
    HistoryReadValueId {
        node_id,
        index_range: Default::default(),
        data_encoding: QualifiedName::null(),
        continuation_point: Default::default(),
    }
}

pub fn extract_history_data(obj: async_opcua::types::ExtensionObject) -> Vec<DataValue> {
    obj.into_inner_as::<HistoryData>()
        .and_then(|hd| hd.data_values)
        .unwrap_or_default()
}

// ── Audit log ─────────────────────────────────────────────────────────────────

/// Pure validation for write_tag. Extracted so it can be unit-tested without a live connection.
pub fn validate_write(
    perm: &WritePermission,
    value: &serde_json::Value,
    units: &str,
    tag_id: &str,
) -> Result<(Variant, WriteValue), AdapterError> {
    let (variant, write_value) = match value {
        serde_json::Value::Number(n) => {
            let f = n.as_f64().ok_or_else(|| AdapterError {
                code: ErrorCode::InvalidValue,
                message: "value is not a valid number".into(),
                tag_id: Some(tag_id.to_string()),
            })?;
            if let Some(min) = perm.min {
                if f < min {
                    return Err(AdapterError {
                        code: ErrorCode::InvalidValue,
                        message: format!("value {f} is below minimum {min}"),
                        tag_id: Some(tag_id.to_string()),
                    });
                }
            }
            if let Some(max) = perm.max {
                if f > max {
                    return Err(AdapterError {
                        code: ErrorCode::InvalidValue,
                        message: format!("value {f} exceeds maximum {max}"),
                        tag_id: Some(tag_id.to_string()),
                    });
                }
            }
            (Variant::Double(f), WriteValue::Float(f))
        }
        serde_json::Value::Bool(b) => (Variant::Boolean(*b), WriteValue::Bool(*b)),
        _ => {
            return Err(AdapterError {
                code: ErrorCode::InvalidValue,
                message: "value must be a number or boolean — strings are not writable".into(),
                tag_id: Some(tag_id.to_string()),
            })
        }
    };

    if let Some(ref expected_units) = perm.units {
        if !expected_units.is_empty() && units != expected_units.as_str() {
            return Err(AdapterError {
                code: ErrorCode::InvalidValue,
                message: format!("units mismatch: expected '{expected_units}', got '{units}'"),
                tag_id: Some(tag_id.to_string()),
            });
        }
    }

    Ok((variant, write_value))
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

// ── Misc ──────────────────────────────────────────────────────────────────────

pub fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use async_opcua::types::UAString;

    fn perm(min: Option<f64>, max: Option<f64>, units: Option<&str>) -> WritePermission {
        WritePermission {
            min,
            max,
            units: units.map(String::from),
        }
    }

    // ── parse_node_id ─────────────────────────────────────────────────────────

    #[test]
    fn parse_node_id_string_form() {
        assert!(parse_node_id("ns=2;s=Pump01.FlowRate").is_ok());
    }

    #[test]
    fn parse_node_id_numeric_form() {
        assert!(parse_node_id("ns=0;i=85").is_ok());
    }

    #[test]
    fn parse_node_id_invalid_returns_tag_not_found() {
        let err = parse_node_id("not-a-valid-nodeid").unwrap_err();
        assert_eq!(err.code, ErrorCode::TagNotFound);
        assert_eq!(err.tag_id.as_deref(), Some("not-a-valid-nodeid"));
    }

    // ── variant_to_tag_value ──────────────────────────────────────────────────

    #[test]
    fn variant_double_to_float() {
        assert!(matches!(
            variant_to_tag_value(&Variant::Double(42.5)),
            TagValue::Float(f) if (f - 42.5).abs() < 1e-9
        ));
    }

    #[test]
    fn variant_float_to_float() {
        assert!(matches!(
            variant_to_tag_value(&Variant::Float(3.14)),
            TagValue::Float(f) if (f - 3.14_f32 as f64).abs() < 1e-5
        ));
    }

    #[test]
    fn variant_int32_to_float() {
        assert!(matches!(
            variant_to_tag_value(&Variant::Int32(100)),
            TagValue::Float(f) if (f - 100.0).abs() < 1e-9
        ));
    }

    #[test]
    fn variant_uint32_to_float() {
        assert!(matches!(
            variant_to_tag_value(&Variant::UInt32(7)),
            TagValue::Float(f) if (f - 7.0).abs() < 1e-9
        ));
    }

    #[test]
    fn variant_int64_to_float() {
        assert!(matches!(
            variant_to_tag_value(&Variant::Int64(999)),
            TagValue::Float(f) if (f - 999.0).abs() < 1e-9
        ));
    }

    #[test]
    fn variant_bool_to_bool() {
        assert!(matches!(
            variant_to_tag_value(&Variant::Boolean(true)),
            TagValue::Bool(true)
        ));
        assert!(matches!(
            variant_to_tag_value(&Variant::Boolean(false)),
            TagValue::Bool(false)
        ));
    }

    #[test]
    fn variant_string_to_text() {
        let v = Variant::String(UAString::from("hello"));
        assert!(matches!(variant_to_tag_value(&v), TagValue::Text(s) if s == "hello"));
    }

    // ── status_code_to_quality ────────────────────────────────────────────────

    #[test]
    fn quality_none_is_good() {
        assert_eq!(status_code_to_quality(None), Quality::Good);
    }

    #[test]
    fn quality_good_status() {
        assert_eq!(
            status_code_to_quality(Some(StatusCode::Good)),
            Quality::Good
        );
    }

    #[test]
    fn quality_uncertain_status() {
        assert_eq!(
            status_code_to_quality(Some(StatusCode::UncertainSubNormal)),
            Quality::Uncertain
        );
    }

    #[test]
    fn quality_bad_status() {
        assert_eq!(
            status_code_to_quality(Some(StatusCode::BadNodeIdInvalid)),
            Quality::Bad
        );
    }

    // ── data_value_to_vqt ────────────────────────────────────────────────────

    #[test]
    fn dv_to_vqt_basic_shape() {
        let dv = DataValue::new_now(Variant::Double(312.7));
        let vqt = data_value_to_vqt("ns=2;s=Pump01.Flow", &dv, "m3/h");
        assert_eq!(vqt.tag_id, "ns=2;s=Pump01.Flow");
        assert_eq!(vqt.units, "m3/h");
        assert_eq!(vqt.quality, Quality::Good);
        assert!(matches!(vqt.value, TagValue::Float(f) if (f - 312.7).abs() < 1e-9));
    }

    #[test]
    fn dv_to_vqt_null_value_falls_back() {
        let dv = DataValue {
            value: None,
            status: None,
            source_timestamp: None,
            source_picoseconds: None,
            server_timestamp: None,
            server_picoseconds: None,
        };
        let vqt = data_value_to_vqt("ns=2;s=X", &dv, "");
        assert!(matches!(&vqt.value, TagValue::Text(s) if s == "null"));
    }

    #[test]
    fn dv_to_vqt_bad_status_propagates() {
        let dv = DataValue::new_now_status(Variant::Double(0.0), StatusCode::BadNodeIdInvalid);
        let vqt = data_value_to_vqt("ns=2;s=X", &dv, "");
        assert_eq!(vqt.quality, Quality::Bad);
    }

    // ── validate_write ────────────────────────────────────────────────────────

    #[test]
    fn validate_write_float_ok() {
        let p = perm(Some(0.0), Some(100.0), Some("Hz"));
        let (variant, wv) = validate_write(&p, &serde_json::json!(50.0), "Hz", "tag").unwrap();
        assert!(matches!(variant, Variant::Double(f) if (f - 50.0).abs() < 1e-9));
        assert!(matches!(wv, WriteValue::Float(f) if (f - 50.0).abs() < 1e-9));
    }

    #[test]
    fn validate_write_bool_ok() {
        let p = perm(None, None, None);
        let (variant, wv) = validate_write(&p, &serde_json::json!(true), "", "tag").unwrap();
        assert!(matches!(variant, Variant::Boolean(true)));
        assert!(matches!(wv, WriteValue::Bool(true)));
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
        let p = perm(Some(0.0), Some(100.0), Some("Hz"));
        assert!(validate_write(&p, &serde_json::json!(0.0), "Hz", "tag").is_ok());
        assert!(validate_write(&p, &serde_json::json!(100.0), "Hz", "tag").is_ok());
    }

    #[test]
    fn validate_write_units_mismatch() {
        let p = perm(None, None, Some("Hz"));
        let err = validate_write(&p, &serde_json::json!(50.0), "rpm", "tag").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidValue);
        assert!(err.message.contains("units") || err.message.contains("Units"));
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
        let err = validate_write(&p, &serde_json::json!(99.0), "", "ns=2;s=MyTag").unwrap_err();
        assert_eq!(err.tag_id.as_deref(), Some("ns=2;s=MyTag"));
    }
}
