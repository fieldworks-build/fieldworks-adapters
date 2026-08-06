# fieldworks-adapter-core

Shared types, VQT envelope, and error taxonomy for [FieldWorks](https://github.com/fieldworks-build/) industrial protocol adapters.

This crate is the common dependency for FieldWorks Protocol Adapter Layer MCP servers ([mqtt-mcp](https://crates.io/crates/mqtt-mcp), [opcua-mcp](https://crates.io/crates/opcua-mcp), and others) — it defines the shared vocabulary that lets agents talk to any conformant adapter identically, regardless of the underlying protocol.

## What's in it

- `Vqt`, `TagValue`, `WriteValue`, `WriteConfirmation` — the data envelope types
- `ErrorCode`, `AdapterError`, `AdapterResult<T>` — the seven required error codes
- `TagDescriptor`, `NormalRange`, `ScanEntry` — discovery response types
- `ConnectResponse`, `DisconnectResponse`, `ServerInfoResponse` — lifecycle response types
- `FieldworksAdapter` trait — the nine-tool async interface every adapter implements

## The VQT envelope

All data reads across FieldWorks adapters return a VQT — Value, Quality, Timestamp:

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

## Usage

```toml
[dependencies]
fieldworks-adapter-core = "1.0"
```

This crate is a library only — it has no adapter behavior of its own. See [mqtt-mcp](https://crates.io/crates/mqtt-mcp) or [opcua-mcp](https://crates.io/crates/opcua-mcp) for runnable MCP servers built on it.

## Related

- [fieldworks-adapters](https://github.com/fieldworks-build/fieldworks-adapters) — workspace source, adapter conformance spec, and the full nine-tool contract
- [FieldWorks Framework](https://github.com/fieldworks-build/) — the broader industrial AI framework

## License

Apache-2.0
