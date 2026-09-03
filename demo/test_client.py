#!/usr/bin/env python3
"""
HTTP test client for grpcgi — simulates what the Envoy WASM filter does.

Wraps a plain HTTP request in the grpcgi.v1.HttpBridge.Process gRPC protocol
and prints the response.

Usage:
    python test_client.py [METHOD] [PATH] [BODY]
    python test_client.py GET /
    python test_client.py GET /hello/
    python test_client.py POST /echo/ '{"hello": "world"}'
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

import grpc.aio
from grpcgi._proto.grpcgi.v1 import http_pb2, http_pb2_grpc


async def http_request(
    method: str = "GET",
    path: str = "/",
    body: bytes = b"",
    host: str = "localhost",
    port: int = 50051,
) -> tuple[int, list, bytes]:
    async with grpc.aio.insecure_channel(f"{host}:{port}") as channel:
        stub = http_pb2_grpc.HttpBridgeStub(channel)

        async def request_stream():
            # First message: request headers (pseudo-headers + real headers)
            yield http_pb2.ProcessingRequest(
                request_headers=http_pb2.RequestHeaders(
                    headers=[
                        http_pb2.Header(key=":method",    value=method),
                        http_pb2.Header(key=":path",      value=path),
                        http_pb2.Header(key=":authority", value=host),
                        http_pb2.Header(key=":scheme",    value="http"),
                        http_pb2.Header(key="user-agent", value="grpcgi-test-client/1.0"),
                    ]
                )
            )
            # Optional body chunk
            if body:
                yield http_pb2.ProcessingRequest(
                    request_body=http_pb2.BodyChunk(
                        data=body,
                        end_of_stream=True,
                    )
                )

        status = 0
        resp_headers: list[tuple[str, str]] = []
        resp_body = b""

        async for msg in stub.Process(request_stream()):
            if msg.HasField("response_headers"):
                status = msg.response_headers.status
                resp_headers = [
                    (h.key, h.value)
                    for h in msg.response_headers.headers
                ]
            elif msg.HasField("response_body"):
                resp_body += msg.response_body.data

    return status, resp_headers, resp_body


async def main() -> None:
    method = sys.argv[1].upper() if len(sys.argv) > 1 else "GET"
    path   = sys.argv[2]         if len(sys.argv) > 2 else "/"
    body   = sys.argv[3].encode() if len(sys.argv) > 3 else b""

    print(f"→ {method} {path}")
    print()

    status, headers, body_bytes = await http_request(method, path, body)

    print(f"← HTTP {status}")
    for k, v in headers:
        print(f"   {k}: {v}")
    print()
    print(body_bytes.decode(errors="replace"))


asyncio.run(main())
