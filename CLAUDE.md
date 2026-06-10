# fieldworks-adapters

Rust workspace: the FieldWorks Protocol Adapter Layer. Each crate is an MCP server exposing a **nine-tool surface** so AI agents get uniform, structured access to industrial process data regardless of underlying protocol.

This is a framework component — no waterworks-specific imports, no hardcoded plant topology. Plant-agnostic by design.

## Building

```bash
# Build everything
cargo build

# Build one adapter
cargo build -p mqtt-mcp
cargo build -p opcua-mcp
```

Requires **Rust 1.75+** (stable async fn in traits).

## Testing

```bash
# Unit tests — no infrastructure required (61 tests, always run these)
cargo test --workspace

# Integration tests — need a live broker
MQTT_TEST_HOST=localhost cargo test -p mqtt-mcp
OPCUA_TEST_HOST=opc.tcp://localhost:4840 cargo test -p opcua-mcp
```

CI runs unit-only suite on every push. Don't break `cargo test --workspace`.

## Workspace layout

| Crate | Status |
|---|---|
| `fieldworks-adapter-core` | Complete — shared types, VQT envelope, `FieldworksAdapter` trait |
| `mqtt-mcp` | Complete — MQTT v3.1.1/v5.0, 25 unit tests |
| `opcua-mcp` | Complete — OPC-UA via async-opcua 0.18, 26 unit tests |
| `modbus-mcp` | Stub |
| `dnp3-mcp` | Stub |
| `ethernetip-mcp` | Stub |
| `aveva-mcp` | Stub |

## The nine required tools

Every conformant adapter exposes exactly these. The two protocol-specific slots are `scan`/`browse` and `get_topic_tree`/`get_node_tree`.

| Tool | Notes |
|---|---|
| `connect` | Establish connection |
| `disconnect` | Clean close |
| `discover_tags` | Metadata from topology.yaml or scan cache |
| `scan` / `browse` † | Protocol-native address-space exploration |
| `get_topic_tree` / `get_node_tree` † | Full address-space dump for topology onboarding |
| `read_tag` | Current value with VQT envelope |
| `read_tag_history` | Historian data; return `HISTORY_UNAVAILABLE` if unsupported |
| `write_tag` | Setpoint write with operator attribution + audit log |
| `get_server_info` | Metadata, connection state, capability list |

## VQT envelope

All data reads return this shape. Never return raw values without it.

```json
{
  "tag_id": "factory/pump01/flow_rate",
  "value": 312.7,
  "quality": "good",
  "timestamp": "2026-06-09T17:50:00.000Z",
  "units": "m3/h"
}
```

- Quality: `good` | `uncertain` | `bad`
- Timestamps: always UTC ISO 8601, millisecond precision
- Units: engineering units string, empty string if unitless

## Error codes

Seven error codes defined in `fieldworks-adapter-core`. Never return raw protocol errors — always map to one of the seven. See `AdapterError` in `fieldworks-adapter-core/src/lib.rs`.

## Conformance checklist

An adapter is conformant when it:
1. Exposes all nine required tools
2. Returns VQT envelopes from all data reads
3. Returns one of the seven `ErrorCode` values on failure
4. Returns `HISTORY_UNAVAILABLE` from `read_tag_history` if protocol doesn't support it
5. Appends to `write_audit.jsonl` on every successful write
6. Lists supported tools in `capabilities` field of `get_server_info`

## topology.yaml (optional, per adapter)

Place next to binary or one directory up. Enables rich tag metadata and write-permission enforcement. Without it, `discover_tags` falls back to scan cache and all writes are denied.

```yaml
tags:
  - tag_id: "factory/pump01/flow_rate"
    name: "Pump 01 Flow Rate"
    units: "m3/h"
    data_type: "float"
    writable: false
    process_area: "raw_water"
    equipment_id: "pump_01"
    normal_range:
      min: 0.0
      max: 500.0

write_permissions:
  "factory/pump01/speed_setpoint":
    min: 0.0
    max: 60.0
    units: "Hz"
```

## Adding a stub adapter

Pattern from existing stubs (`modbus-mcp`, `dnp3-mcp`, etc.):
1. Add crate to `Cargo.toml` workspace `members`
2. `Cargo.toml` dependency: `fieldworks-adapter-core = { path = "../fieldworks-adapter-core" }`
3. `src/main.rs`: implement `FieldworksAdapter` trait, return `NOT_IMPLEMENTED` or `DISCONNECTED` from all tools until real implementation lands
4. Keep it compiling — a stub that doesn't build breaks `cargo test --workspace`

## Key decisions — don't relitigate

- **Nine tools, not more**: the tool surface is fixed by the conformance spec. Protocol-specific tools fill the two reserved slots; anything else is out of spec.
- **VQT everywhere**: every data read returns a VQT. No raw value returns, no protocol-specific response shapes.
- **Error code mapping**: raw protocol errors (MQTT disconnect, OPC-UA status codes) are always mapped to the seven framework error codes before returning. Callers never see protocol internals.
- **Write audit log is mandatory**: `write_audit.jsonl` append on every successful write is part of conformance, not optional.
- **No waterworks imports**: this crate is plant-agnostic. Any waterworks-specific behavior lives in waterworks-ai, not here.
- **Stubs must compile**: implement `FieldworksAdapter` returning stubs rather than leaving `todo!()` panics that break the workspace build.

## Architecture notes — reviewed and accepted

**`FieldworksAdapter` trait is a specification document, not compiler enforcement.** The trait in `fieldworks-adapter-core` defines the nine-tool surface but no adapter `impl`s it. The rmcp macro system (`#[tool_router]`, `#[tool]`) generates tool dispatch through its own machinery and doesn't compose with a hand-written trait impl at the same level. Conformance is enforced by convention and code review. Don't add `impl FieldworksAdapter for MqttServer` blocks — they'd be dead code.

**`thiserror` is absent by design.** `AdapterError` is a hand-rolled struct. Adding `thiserror` would only replace boilerplate with different boilerplate — nothing propagates errors with `?` from external crates into `AdapterError`, so the derive benefit doesn't apply here.

**`unwrap()` calls in production code are all provably infallible.** Three patterns exist:
- `serde_json::to_value(&T).unwrap()` — on our own `Serialize` types; cannot fail
- `Mutex::lock().unwrap()` / `RwLock::write().unwrap()` — standard Rust practice; poisoning only occurs on thread panic
- `guard.as_ref().unwrap()` after an explicit `is_none()` early return — proven `Some` by control flow

Don't flag these as bugs or suggest replacing them with `expect()` or `?` propagation.
