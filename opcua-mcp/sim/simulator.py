"""OPC-UA simulator for local development and CI conformance testing.

Not a general-purpose OPC-UA simulator — a fixed fixture for exercising
opcua-mcp specifically. Pairs with topology.yaml in this directory: every
tag_id declared there resolves to a real node below.

Usage:
    pip install -r requirements.txt
    python simulator.py [--host 127.0.0.1] [--port 4860]

Fixture nodes (namespace index is whatever register_namespace assigns —
printed on startup, expected to be 2 for a fresh server):
    ns=2;s=Pump01.FlowRate        Double  312.7   read-only, historized
    ns=2;s=Pump01.Running         Boolean true    read-only
    ns=2;s=Pump01.SpeedSetpoint   Double  0.0     writable (0-60, matches
                                                   topology.yaml's
                                                   write_permissions entry)

FlowRate is historized (asyncua's historize_node_data_change) with a few
seed data points written at startup, so read_tag_history has something real
to return instead of an empty set.
"""

import argparse
import asyncio
from datetime import timedelta

from asyncua import Server
from asyncua.common.node import Node


async def build_server(host: str, port: int) -> tuple[Server, Node]:
    server = Server()
    await server.init()
    # No path suffix: opcua-mcp's connect() builds a bare opc.tcp://host:port
    # when given a plain hostname, and the client's endpoint matching (via
    # GetEndpoints discovery) requires this to equal the server's advertised
    # EndpointUrl exactly — a path here would make every connection attempt
    # fail with BadTcpEndpointUrlInvalid.
    server.set_endpoint(f"opc.tcp://{host}:{port}")
    idx = await server.register_namespace("http://fieldworks.example/opcua-sim")
    print(f"registered namespace index: {idx}", flush=True)

    objects = server.get_objects_node()
    pump = await objects.add_object(idx, "Pump01")

    flow = await pump.add_variable(f"ns={idx};s=Pump01.FlowRate", "FlowRate", 312.7)
    running = await pump.add_variable(f"ns={idx};s=Pump01.Running", "Running", True)
    setpoint = await pump.add_variable(
        f"ns={idx};s=Pump01.SpeedSetpoint", "SpeedSetpoint", 0.0
    )
    await setpoint.set_writable()

    await server.historize_node_data_change(flow, period=timedelta(days=1), count=1000)

    return server, flow


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4860)
    args = parser.parse_args()

    server, flow = await build_server(args.host, args.port)

    async with server:
        # Seed a few historical data points so read_tag_history has real
        # data to find, not just an empty (but valid) result.
        for value in (310.1, 311.4, 312.7):
            await flow.write_value(value)
            await asyncio.sleep(0.2)

        print(
            f"opcua-mcp simulator listening on opc.tcp://{args.host}:{args.port}",
            flush=True,
        )
        while True:
            await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
