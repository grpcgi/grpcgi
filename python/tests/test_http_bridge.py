"""Integration tests for HttpBridgeServicer using grpc.aio in-process."""

from __future__ import annotations

import asyncio
import pytest
import grpc
import grpc.aio

from grpcgi._proto.grpcgi.v1 import http_pb2, http_pb2_grpc
from grpcgi.http import HttpBridgeServicer
from grpcgi.server import serve as _serve_fn  # import to verify it's callable


# ---------------------------------------------------------------------------
# Minimal ASGI echo app under test
# ---------------------------------------------------------------------------

async def echo_app(scope, receive, send):
    """Echo HTTP body back as response."""
    assert scope["type"] == "http"
    body_parts = []
    while True:
        event = await receive()
        body_parts.append(event.get("body", b""))
        if not event.get("more_body", False):
            break
    body = b"".join(body_parts)
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [(b"content-type", b"text/plain")],
    })
    await send({
        "type": "http.response.body",
        "body": body or b"hello",
        "more_body": False,
    })


# ---------------------------------------------------------------------------
# Helpers to build proto messages
# ---------------------------------------------------------------------------

def make_headers(*pairs: tuple[str, str]) -> http_pb2.ProcessingRequest:
    headers = [http_pb2.Header(key=k, value=v) for k, v in pairs]
    return http_pb2.ProcessingRequest(
        request_headers=http_pb2.RequestHeaders(headers=headers)
    )


def make_body(data: bytes, end: bool = True) -> http_pb2.ProcessingRequest:
    return http_pb2.ProcessingRequest(
        request_body=http_pb2.BodyChunk(data=data, end_of_stream=end)
    )


# ---------------------------------------------------------------------------
# In-process gRPC server fixture
# ---------------------------------------------------------------------------

@pytest.fixture
async def http_channel():
    server = grpc.aio.server()
    http_pb2_grpc.add_HttpBridgeServicer_to_server(HttpBridgeServicer(echo_app), server)
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
async def test_http_no_body(http_channel):
    stub = http_pb2_grpc.HttpBridgeStub(http_channel)

    async def req_gen():
        yield make_headers(
            (":method", "GET"),
            (":path", "/hello"),
            (":scheme", "http"),
            (":authority", "example.com:80"),
        )
        # No body messages; stream ends here.

    responses = []
    async for resp in stub.Process(req_gen()):
        responses.append(resp)

    # Should get response_headers then response_body
    assert len(responses) == 2
    assert responses[0].HasField("response_headers")
    assert responses[0].response_headers.status == 200
    assert responses[1].HasField("response_body")
    assert responses[1].response_body.data == b"hello"
    assert responses[1].response_body.end_of_stream is True


@pytest.mark.asyncio
async def test_http_with_body(http_channel):
    stub = http_pb2_grpc.HttpBridgeStub(http_channel)

    async def req_gen():
        yield make_headers(
            (":method", "POST"),
            (":path", "/echo?foo=bar"),
            (":scheme", "http"),
            (":authority", "example.com"),
            ("content-type", "text/plain"),
        )
        yield make_body(b"world", end=True)

    responses = []
    async for resp in stub.Process(req_gen()):
        responses.append(resp)

    assert len(responses) == 2
    hdr = responses[0].response_headers
    assert hdr.status == 200
    body = responses[1].response_body
    assert body.data == b"world"
    assert body.end_of_stream is True


@pytest.mark.asyncio
async def test_http_scope_fields(http_channel):
    """Verify that scope fields are correctly parsed."""
    received_scopes: list[dict] = []

    async def scope_capture_app(scope, receive, send):
        received_scopes.append(scope)
        # Drain receive
        while True:
            ev = await receive()
            if not ev.get("more_body", False):
                break
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    server = grpc.aio.server()
    http_pb2_grpc.add_HttpBridgeServicer_to_server(
        HttpBridgeServicer(scope_capture_app), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")

    try:
        stub = http_pb2_grpc.HttpBridgeStub(channel)

        async def req_gen():
            yield make_headers(
                (":method", "DELETE"),
                (":path", "/items/42?page=1&sort=asc"),
                (":scheme", "https"),
                (":authority", "api.example.com:8443"),
                ("x-request-id", "abc123"),
            )

        async for _ in stub.Process(req_gen()):
            pass
    finally:
        await channel.close()
        await server.stop(grace=0)

    assert len(received_scopes) == 1
    s = received_scopes[0]
    assert s["type"] == "http"
    assert s["method"] == "DELETE"
    assert s["path"] == "/items/42"
    assert s["query_string"] == b"page=1&sort=asc"
    assert s["scheme"] == "https"
    assert s["server"] == ("api.example.com", 8443)
    assert s["http_version"] == "2"
    # host header should be present
    assert (b"host", b"api.example.com:8443") in s["headers"]
    # custom header should be present
    assert (b"x-request-id", b"abc123") in s["headers"]
