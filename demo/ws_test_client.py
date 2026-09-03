#!/usr/bin/env python3
"""
WebSocket test client for grpcgi — simulates what the Envoy WASM WS filter does.

Opens a grpcgi.v1.WebSocketBridge.Connect bidi-streaming gRPC call, sends a
few frames, and prints the echoed responses.

Usage:
    python ws_test_client.py [PATH] [MESSAGE...]
    python ws_test_client.py /ws/ "hello" "world"
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

import grpc.aio
from grpcgi._proto.grpcgi.v1 import websocket_pb2, websocket_pb2_grpc


async def ws_session(
    path: str = "/ws/",
    messages: list[str] | None = None,
    host: str = "localhost",
    port: int = 50051,
) -> None:
    if messages is None:
        messages = ["Hello grpcgi!", "WebSocket over gRPC works."]

    # HTTP upgrade metadata is passed as gRPC initial metadata.
    # The WASM filter sets these from the browser's HTTP upgrade request.
    metadata = [
        ("grpcgi-path",      path),
        ("grpcgi-scheme",    "ws"),
        ("grpcgi-authority", host),
    ]

    async with grpc.aio.insecure_channel(f"{host}:{port}") as channel:
        stub = websocket_pb2_grpc.WebSocketBridgeStub(channel)

        send_queue: asyncio.Queue = asyncio.Queue()
        for msg in messages:
            await send_queue.put(msg)
        await send_queue.put(None)  # sentinel → close

        async def frame_stream():
            while True:
                item = await send_queue.get()
                if item is None:
                    return
                binary = isinstance(item, bytes)
                payload = item if binary else item.encode()
                print(f"→ {'binary' if binary else 'text'}: {item!r}")
                yield websocket_pb2.Frame(payload=payload, binary=binary)

        async for frame in stub.Connect(frame_stream(), metadata=metadata):
            text = frame.payload.decode() if not frame.binary else frame.payload
            print(f"← {'binary' if frame.binary else 'text'}: {text!r}")


async def main() -> None:
    path     = sys.argv[1]        if len(sys.argv) > 1 else "/ws/"
    messages = sys.argv[2:]       if len(sys.argv) > 2 else None

    print(f"WebSocket test: connecting to {path}")
    print()
    await ws_session(path, list(messages) if messages else None)
    print()
    print("done.")


asyncio.run(main())
