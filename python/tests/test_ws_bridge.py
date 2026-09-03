"""Integration tests for WebSocketBridgeServicer using grpc.aio in-process."""

from __future__ import annotations

import pytest
import grpc
import grpc.aio

from grpcgi._proto.grpcgi.v1 import websocket_pb2, websocket_pb2_grpc
from grpcgi.ws import WebSocketBridgeServicer


# ---------------------------------------------------------------------------
# Minimal ASGI WebSocket echo app
# ---------------------------------------------------------------------------

async def ws_echo_app(scope, receive, send):
    assert scope["type"] == "websocket"
    event = await receive()
    assert event["type"] == "websocket.connect"
    await send({"type": "websocket.accept"})

    while True:
        event = await receive()
        if event["type"] == "websocket.receive":
            if event.get("bytes") is not None:
                await send({"type": "websocket.send", "bytes": event["bytes"]})
            else:
                await send({"type": "websocket.send", "text": event["text"]})
        elif event["type"] == "websocket.disconnect":
            break


# ---------------------------------------------------------------------------
# Helpers
#
# gRPC metadata keys must not start with ":". Envoy maps HTTP/2 pseudo-headers
# to "grpcgi-*" keys which ws._build_scope() understands.
# ---------------------------------------------------------------------------

def _ws_metadata(path="/ws", scheme="ws", authority="localhost:8080", protocol=None):
    md = [
        ("grpcgi-path", path),
        ("grpcgi-scheme", scheme),
        ("grpcgi-authority", authority),
    ]
    if protocol:
        md.append(("grpcgi-protocol", protocol))
    return tuple(md)


# ---------------------------------------------------------------------------
# In-process gRPC server fixture
# ---------------------------------------------------------------------------

@pytest.fixture
async def ws_channel():
    server = grpc.aio.server()
    websocket_pb2_grpc.add_WebSocketBridgeServicer_to_server(
        WebSocketBridgeServicer(ws_echo_app), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    yield channel
    await channel.close()
    await server.stop(grace=0)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ws_text_echo(ws_channel):
    stub = websocket_pb2_grpc.WebSocketBridgeStub(ws_channel)
    messages = ["hello", "world"]

    async def frame_gen():
        for msg in messages:
            yield websocket_pb2.Frame(payload=msg.encode(), binary=False)

    received = []
    async for frame in stub.Connect(frame_gen(), metadata=_ws_metadata("/ws/chat")):
        received.append(frame)

    assert len(received) == len(messages)
    for i, msg in enumerate(messages):
        assert received[i].binary is False
        assert received[i].payload.decode() == msg


@pytest.mark.asyncio
async def test_ws_binary_echo(ws_channel):
    stub = websocket_pb2_grpc.WebSocketBridgeStub(ws_channel)
    payloads = [b"\x00\x01\x02", b"\xff\xfe\xfd"]

    async def frame_gen():
        for p in payloads:
            yield websocket_pb2.Frame(payload=p, binary=True)

    received = []
    async for frame in stub.Connect(frame_gen(), metadata=_ws_metadata("/ws/data")):
        received.append(frame)

    assert len(received) == len(payloads)
    for i, p in enumerate(payloads):
        assert received[i].binary is True
        assert received[i].payload == p


@pytest.mark.asyncio
async def test_ws_scope_fields():
    """Verify websocket scope is correctly built from gRPC initial metadata."""
    captured: list[dict] = []

    async def capture_app(scope, receive, send):
        captured.append(scope)
        event = await receive()
        assert event["type"] == "websocket.connect"
        await send({"type": "websocket.accept"})
        await send({"type": "websocket.close"})

    server = grpc.aio.server()
    websocket_pb2_grpc.add_WebSocketBridgeServicer_to_server(
        WebSocketBridgeServicer(capture_app), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")

    try:
        stub = websocket_pb2_grpc.WebSocketBridgeStub(channel)
        metadata = _ws_metadata(
            path="/chat?room=42",
            scheme="wss",
            authority="example.com:443",
            protocol="chat, superchat",
        )

        async def no_frames():
            return
            yield  # make it a generator

        async for _ in stub.Connect(no_frames(), metadata=metadata):
            pass
    finally:
        await channel.close()
        await server.stop(grace=0)

    assert len(captured) == 1
    s = captured[0]
    assert s["type"] == "websocket"
    assert s["path"] == "/chat"
    assert s["query_string"] == b"room=42"
    assert s["scheme"] == "wss"
    assert s["server"] == ("example.com", 443)
    assert "chat" in s["subprotocols"]
    assert "superchat" in s["subprotocols"]
