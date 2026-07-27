# fieldworks-adapters

Rust workspace implementing the FieldWorks Protocol Adapter Layer — a set of MCP servers that give AI agents uniform, structured access to industrial process data across heterogeneous protocols.

Each adapter exposes the same nine-tool MCP surface so agents can connect, discover tags, read values, write setpoints, and inspect server state using identical tool calls regardless of the underlying protocol.

## Workspace layout

```
fieldworks-adapters/
├── fieldworks-adapter-core/   # Shared types, VQT envelope, conformance trait
├── mqtt-mcp/                  # MQTT v3.1.1 / v5.0 adapter (complete)
├── opcua-mcp/                 # OPC-UA adapter (complete)
├── modbus-mcp/                # Modbus TCP adapter (stub)
├── dnp3-mcp/                  # DNP3 adapter (stub)
├── ethernetip-mcp/            # EtherNet/IP adapter (stub)
└── aveva-mcp/                 # AVEVA PI / System Platform adapter (stub)
```

## The nine required tools

Every conformant adapter exposes these tools. Two slots are protocol-specific (marked †).

| Tool | Description |
|------|-------------|
| `connect` | Establish a connection to the server |
| `disconnect` | Cleanly close the connection |
| `discover_tags` | Return tagged metadata from topology or scan cache |
| `scan` / `browse` † | Protocol-native address-space exploration (MQTT: wildcard subscribe + timed collect; OPC-UA: `browse` with depth limit) |
| `get_topic_tree` / `get_node_tree` † | Full address-space dump for topology onboarding (MQTT: 10s wildcard scan; OPC-UA: `get_node_tree` recursive browse) |
| `read_tag` | Read current value with VQT envelope |
| `read_tag_history` | Read historian data (OPC-UA: HistoricalAccess; MQTT: returns `HISTORY_UNAVAILABLE`) |
| `write_tag` | Write a setpoint with operator attribution and audit log entry |
| `get_server_info` | Server metadata, connection state, and capability list |

## VQT envelope

All data reads return a VQT — Value, Quality, Timestamp:

```json
{
  "tag_id": "factory/pump01/flow_rate",
  "value": 312.7,
  "quality": "good",
  "timestamp": "2026-06-09T17:50:00.000Z",
  "units": "m3/h"
}
```

Quality is `good`, `uncertain`, or `bad`. Timestamps are always UTC ISO 8601 at millisecond precision.

## mqtt-mcp

The reference implementation. Connects to any MQTT v3.1.1 or v5.0 broker.

**Build and run:**

```bash
cargo build -p mqtt-mcp
RUST_LOG=info ./target/debug/mqtt-mcp
```

The server speaks MCP over stdio — wire it up in your MCP client config.

**Optional topology.yaml:**

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

**Write audit log:**

Every successful `write_tag` call appends a JSON line to `write_audit.jsonl` in the working directory:

```json
{"timestamp":"2026-06-09T17:50:00.000Z","tag_id":"factory/pump01/speed_setpoint","value":45.0,"units":"Hz","operator_id":"ops-team","reason":"demand response adjustment"}
```

## opcua-mcp

OPC-UA client adapter using [async-opcua](https://crates.io/crates/async-opcua) 0.18.

**Build and run:**

```bash
cargo build -p opcua-mcp
RUST_LOG=info ./target/debug/opcua-mcp
```

**Connect:**

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

**Tag IDs are OPC-UA NodeId strings:**

```
ns=2;s=Pump01.FlowRate        ← string identifier
ns=2;i=1001                   ← numeric identifier
ns=0;i=85                     ← Objects folder (browse start)
```

Use `browse` or `get_node_tree` to discover NodeIds on an unfamiliar server. Use `discover_tags` if a `topology.yaml` is present.

**Protocol-specific tools:**

| Tool | Description |
|------|-------------|
| `browse` | Browse child nodes from a starting NodeId with configurable depth (default 1, max 5) |
| `get_node_tree` | Recursive nested address space dump from a root NodeId (default depth 3, hard cap 1000 nodes) |

**`read_tag_history`** is fully implemented via OPC-UA HistoricalAccess. Returns `HISTORY_UNAVAILABLE` if the server does not expose history for the requested node.

**Optional topology.yaml** — same schema as mqtt-mcp, with NodeId strings as `tag_id` values:

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

**Security note:** When using `Sign` or `SignAndEncrypt`, the adapter auto-generates a self-signed certificate in `./pki/`. The server must trust this certificate before the connection will succeed. For `SecurityMode::None`, certificate validation is skipped automatically.

## modbus-mcp

Modbus TCP client adapter using [tokio-modbus](https://crates.io/crates/tokio-modbus) 0.17.

**Build and run:**

```bash
cargo build -p modbus-mcp
RUST_LOG=info ./target/debug/modbus-mcp
```

**Tag IDs encode register type, address, and (for registers) an optional data type:**

```
holding:100          ← holding register, uint16 (default when omitted)
holding:100:float32  ← holding register, float32 (spans 2 registers, big-endian word order)
input:5              ← input register (read-only)
coil:5               ← coil
discrete:3           ← discrete input (read-only)
```

Input registers and discrete inputs have no write function code in the Modbus spec — `write_tag` against either always returns `TAG_NOT_WRITABLE`, independent of `topology.yaml`.

**Protocol-specific tools:**

| Tool | Description |
|------|-------------|
| `scan` | Active read of a contiguous address range for a given register type (max 125 for holding/input, 2000 for coil/discrete — Modbus protocol limits) |
| `get_topic_tree` | `topology.yaml` tags grouped by register type. Purely config-derived, no live I/O |

**Optional topology.yaml** — same schema as mqtt-mcp, with `register_type:address[:data_type]` strings as `tag_id` values:

```yaml
tags:
  - tag_id: "holding:100:float32"
    name: "Pump 01 Flow Rate"
    units: "m3/h"
    data_type: "float32"
    writable: false
    process_area: "raw_water"
    equipment_id: "pump_01"

write_permissions:
  "holding:210":
    min: 0.0
    max: 100.0
    units: "Hz"
```

**Local testing:** `modbus-mcp/sim/` ships a fixture Modbus TCP simulator (pymodbus-based) for exercising the adapter without real hardware:

```bash
pip install -r modbus-mcp/sim/requirements.txt
python modbus-mcp/sim/simulator.py --port 5502
```

`modbus-mcp/sim/topology.yaml` matches the simulator's fixture values (`holding:100` = 1234, `holding:200:float32` = 312.7, `holding:210` writable 0-100, `input:5` = 777, `coil:5`/`discrete:3` = true) — copy it next to the binary to exercise `discover_tags` and `write_tag` against known values. This is also what `.github/workflows/test.yml`'s conformance job stands up for CI.

**No auto-reconnect** (tokio-modbus doesn't provide one) — a connection-level failure drops the held connection; call `connect` again. Every call is wrapped in a timeout since tokio-modbus has none natively.

## fieldworks-adapter-core

Shared library crate. Contains:

- `Vqt`, `TagValue`, `WriteValue`, `WriteConfirmation` — the data envelope types
- `ErrorCode`, `AdapterError`, `AdapterResult<T>` — the seven required error codes
- `TagDescriptor`, `NormalRange`, `ScanEntry` — discovery response types
- `ConnectResponse`, `DisconnectResponse`, `ServerInfoResponse` — lifecycle response types
- `FieldworksAdapter` trait — the nine-tool async interface

## Testing

```bash
# Run all unit tests (no infrastructure required)
cargo test --workspace
```

**106 pure-logic unit tests** run without any broker or server:

| Crate | Tests | What's covered |
|-------|-------|----------------|
| `fieldworks-adapter-core` | 10 | Quality/TagValue/ErrorCode/Vqt/WriteValue serialization — snake_case, SCREAMING_SNAKE_CASE, untagged |
| `mqtt-mcp` | 25 | `parse_mqtt_payload` (JSON paths, bool aliases, raw numeric, string fallback), `str_to_quality`, `build_topic_tree`, `validate_write` (range, units, type checks) |
| `opcua-mcp` | 26 | `parse_node_id`, `variant_to_tag_value` (all numeric arms, bool, string), `status_code_to_quality`, `data_value_to_vqt`, `validate_write` |
| `modbus-mcp` | 45 | `parse_tag_id`, `registers_to_value`/`value_to_registers` (round trips incl. float32/int32 endianness), `map_exception_code`, `build_tag_tree`, `validate_write` |

**Integration tests** (connection-dependent) are gated by environment variables. Set `MQTT_TEST_HOST` or `OPCUA_TEST_HOST` to run them against a live broker:

```bash
MQTT_TEST_HOST=localhost cargo test -p mqtt-mcp
OPCUA_TEST_HOST=opc.tcp://localhost:4840 cargo test -p opcua-mcp
```

**modbus-mcp** doesn't have a Rust-level integration test — its connection-dependent behavior is exercised through the `fieldworks test-adapter` conformance CLI against the fixture simulator in `modbus-mcp/sim/` (see the modbus-mcp section above). CI runs this the same way it does for mqtt-mcp's live Mosquitto broker.

CI runs the unit-only suite on every push via `.github/workflows/test.yml`; the conformance job additionally runs live connection-dependent checks for mqtt-mcp and modbus-mcp.

## Building

Requires Rust 1.75+ (stable async fn in traits).

```bash
# Build everything
cargo build

# Build one adapter
cargo build -p mqtt-mcp
```

## Conformance

An adapter is conformant when it:

1. Exposes all nine required tools
2. Returns VQT envelopes from all data reads
3. Returns one of the seven `ErrorCode` values on failure (never raw protocol errors)
4. Returns `HISTORY_UNAVAILABLE` from `read_tag_history` if the protocol or server does not support it
5. Appends to `write_audit.jsonl` on every successful write
6. Lists all supported tools in the `capabilities` field of `get_server_info`

## Related

- [FieldWorks Framework](https://github.com/fieldworks-build/) — coming soon
- [Waterworks AI](https://github.com/smslavin/waterworks-ai) — reference industrial AI deployment using this adapter stack
