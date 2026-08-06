# opcua-mcp

MCP server exposing OPC-UA as a [FieldWorks](https://github.com/fieldworks-build/) nine-tool industrial protocol adapter, built on [async-opcua](https://crates.io/crates/async-opcua) 0.18.

Gives AI agents structured, uniform access to OPC-UA server data — the same nine-tool MCP surface as every other FieldWorks adapter (MQTT, Modbus, ...), so agents can connect, browse, read, write, and inspect server state with identical tool calls regardless of protocol.

## Install / build

```bash
cargo install opcua-mcp
# or, from the workspace:
cargo build -p opcua-mcp
```

## Run

```bash
RUST_LOG=info opcua-mcp
```

The server speaks MCP over stdio — wire it up in your MCP client config.

## Connect

```json
{ "host": "opc.tcp://localhost:4840" }
```

Or with security:

```json
{
  "host": "192.168.1.10",
  "port": 4840,
  "options": {
    "security_mode": "SignAndEncrypt",
    "security_policy": "Basic256Sha256",
    "username": "operator",
    "password": "secret"
  }
}
```

## Tag IDs are OPC-UA NodeId strings

```
ns=2;s=Pump01.FlowRate        ← string identifier
ns=2;i=1001                   ← numeric identifier
ns=0;i=85                     ← Objects folder (browse start)
```

Use `browse` or `get_node_tree` to discover NodeIds on an unfamiliar server. Use `discover_tags` if a `topology.yaml` is present.

## The nine tools

Two are protocol-specific:

| Tool | Description |
|------|-------------|
| `connect` | Establish a connection to the server |
| `disconnect` | Cleanly close the connection |
| `discover_tags` | Return tagged metadata from topology.yaml or scan cache |
| `browse` | Browse child nodes from a starting NodeId, configurable depth (default 1, max 5) |
| `get_node_tree` | Recursive nested address space dump from a root NodeId (default depth 3, hard cap 1000 nodes) |
| `read_tag` | Read current value with VQT envelope |
| `read_tag_history` | Fully implemented via OPC-UA HistoricalAccess; returns `HISTORY_UNAVAILABLE` if the server doesn't expose history for the node |
| `write_tag` | Write a setpoint with operator attribution and audit log entry |
| `get_server_info` | Server metadata, connection state, capability list |

## Optional topology.yaml

Same schema as mqtt-mcp, with NodeId strings as `tag_id` values:

```yaml
tags:
  - tag_id: "ns=2;s=Pump01.FlowRate"
    name: "Pump 01 Flow Rate"
    units: "m3/h"
    data_type: "float"
    writable: false
    process_area: "raw_water"
    equipment_id: "pump_01"

write_permissions:
  "ns=2;s=Pump01.SpeedSetpoint":
    min: 0.0
    max: 60.0
    units: "Hz"
```

## Local testing without hardware

`sim/` ships a fixture OPC-UA server (asyncua-based):

```bash
pip install -r sim/requirements.txt
python sim/simulator.py --port 4860
```

`sim/topology.yaml` matches the simulator's fixture nodes (`Pump01.FlowRate` = 312.7, historized with a few seed points; `Pump01.Running` = true; `Pump01.SpeedSetpoint` writable 0-60) — copy it next to the binary to exercise `discover_tags`, `write_tag`, and `read_tag_history` against known values. Connect with `--host 127.0.0.1`, not `localhost` (see known limitation below).

## Known limitation

`async-opcua`'s TCP transport only tries the first DNS-resolved address for a hostname and doesn't fall back — on Linux, `localhost` resolves to `::1` first, so connecting by that name fails unless something is actually listening on IPv6. This lives inside the `async-opcua` crate itself, not opcua-mcp's code; use a literal IP when in doubt.

## Security note

When using `Sign` or `SignAndEncrypt`, the adapter auto-generates a self-signed certificate in `./pki/`. The server must trust this certificate before the connection will succeed. For `SecurityMode::None`, certificate validation is skipped automatically.

## Testing

32 pure-logic unit tests require no server:

```bash
cargo test -p opcua-mcp
```

Integration tests against a live server are gated behind an env var:

```bash
OPCUA_TEST_HOST=opc.tcp://localhost:4840 cargo test -p opcua-mcp
```

## Related

- [fieldworks-adapters](https://github.com/fieldworks-build/fieldworks-adapters) — workspace source and the full adapter conformance spec
- [fieldworks-adapter-core](https://crates.io/crates/fieldworks-adapter-core) — shared types this crate is built on

## License

Apache-2.0
