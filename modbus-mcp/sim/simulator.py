"""Modbus TCP simulator for local development and CI conformance testing.

Not a general-purpose Modbus simulator — a fixed fixture for exercising
modbus-mcp specifically. Pairs with topology.yaml in this directory: every
tag_id declared there resolves to a real value below.

Usage:
    pip install -r requirements.txt
    python simulator.py [--host 127.0.0.1] [--port 5502]

Fixture values (all addresses are the raw wire-protocol address, 0-based):
    holding:100          uint16  1234
    holding:200:float32  float32 312.7   (big-endian word order, hi word first)
    holding:210          uint16  1       (writable — matches topology.yaml's
                                          write_permissions entry, range 0-100)
    input:5              uint16  777
    coil:5               bool    true
    discrete:3           bool    true

Everything else reads as zero/false — useful for exercising scan over a
range that includes both fixture and blank addresses.
"""

import argparse
import asyncio
import struct

from pymodbus.datastore import (
    ModbusDeviceContext,
    ModbusSequentialDataBlock,
    ModbusServerContext,
)
from pymodbus.server import StartAsyncTcpServer


def build_context() -> ModbusServerContext:
    # ModbusSequentialDataBlock's start-address parameter is consumed as a
    # legacy 1-based reference and converted back to 0-based internally —
    # confirmed empirically against pymodbus's own client. Pass start=1;
    # array index k is then served at wire/protocol address k directly, no
    # manual offset needed.
    hi, lo = struct.unpack(">HH", struct.pack(">f", 312.7))

    holding = [0] * 300
    holding[100] = 1234
    holding[200] = hi
    holding[201] = lo
    holding[210] = 1

    input_regs = [0] * 20
    input_regs[5] = 777

    coils = [False] * 20
    coils[5] = True

    discrete = [False] * 20
    discrete[3] = True

    device = ModbusDeviceContext(
        di=ModbusSequentialDataBlock(1, discrete),
        co=ModbusSequentialDataBlock(1, coils),
        hr=ModbusSequentialDataBlock(1, holding),
        ir=ModbusSequentialDataBlock(1, input_regs),
    )
    return ModbusServerContext(devices=device, single=True)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5502)
    args = parser.parse_args()

    context = build_context()
    print(f"modbus-mcp simulator listening on {args.host}:{args.port}", flush=True)
    await StartAsyncTcpServer(context=context, address=(args.host, args.port))


if __name__ == "__main__":
    asyncio.run(main())
