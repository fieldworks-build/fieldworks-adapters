# mqtt-mcp

MCP server exposing MQTT as a [FieldWorks](https://github.com/fieldworks-build/) nine-tool industrial protocol adapter.

Connects to any MQTT v3.1.1 or v5.0 broker and gives AI agents structured, uniform access to tag data — the same nine-tool MCP surface as every other FieldWorks adapter (OPC-UA, Modbus, ...), so agents can connect, discover tags, read values, write setpoints, and inspect server state with identical tool calls regardless of protocol.

## Install / build

```bash
cargo install mqtt-mcp
# or, from the workspace:
cargo build -p mqtt-mcp
```

## Run

```bash
RUST_LOG=info mqtt-mcp
```

The server speaks MCP over stdio — wire it up in your MCP client config.

## The nine tools

| Tool | Description |
|------|-------------|
| `connect` | Establish a connection to the broker |
| `disconnect` | Cleanly close the connection |
| `discover_tags` | Return tagged metadata from topology.yaml or scan cache |
| `scan` | Wildcard subscribe + timed collect |
| `get_topic_tree` | Full topic tree dump via a 10s wildcard scan |
| `read_tag` | Read current value with VQT envelope |
| `read_tag_history` | Always returns `HISTORY_UNAVAILABLE` — MQTT has no native history |
| `write_tag` | Publish a setpoint with operator attribution and audit log entry |
| `get_server_info` | Server metadata, connection state, capability list |

## Optional topology.yaml

Place a `topology.yaml` next to the binary (or one directory up) to enable rich tag metadata and write-permission enforcement. Without it, `discover_tags` falls back to the live scan cache and all writes are denied.

```yaml
tags:
  - tag_id: "factory/pump01/flow_rate"
    name: "Pump 01 Flow Rate"
    description: "Inlet pump volumetric flow"
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

## Write audit log

Every successful `write_tag` call appends a JSON line to `write_audit.jsonl` in the working directory:

```json
{"timestamp":"2026-06-09T17:50:00.000Z","tag_id":"factory/pump01/speed_setpoint","value":45.0,"units":"Hz","operator_id":"ops-team","reason":"demand response adjustment"}
```

## Testing

25 pure-logic unit tests require no broker:

```bash
cargo test -p mqtt-mcp
```

Integration tests against a live broker are gated behind an env var:

```bash
MQTT_TEST_HOST=localhost cargo test -p mqtt-mcp
```

## Related

- [fieldworks-adapters](https://github.com/fieldworks-build/fieldworks-adapters) — workspace source and the full adapter conformance spec
- [fieldworks-adapter-core](https://crates.io/crates/fieldworks-adapter-core) — shared types this crate is built on

## License

Apache-2.0
