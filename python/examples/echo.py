"""Minimal ASGI echo app for grpcgi.

HTTP:      echos the request body back with status 200.
WebSocket: echos every received frame back to the sender.

Run:
    cd /home/code/proj/grpcgi/python
    python -m examples.echo
or:
    python examples/echo.py
"""

from __future__ import annotations

import asyncio
import json
import sys
import os

# Allow running as a script from anywhere without an editable install.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import grpcgi


async def app(scope, receive, send):
    """ASGI application handling http and websocket scopes."""
    if scope["type"] == "http":
        await _handle_http(scope, receive, send)
    elif scope["type"] == "websocket":
        await _handle_websocket(scope, receive, send)
    else:
        raise RuntimeError(f"Unknown scope type: {scope['type']!r}")


async def _handle_http(scope, receive, send):
    """Echo the request body back as the response body."""
    # Accumulate the full request body.
    body_parts: list[bytes] = []
    while True:
        event = await receive()
        body_parts.append(event.get("body", b""))
        if not event.get("more_body", False):
            break

    body = b"".join(body_parts)

    # Build a simple JSON response that echoes back path and body.
    response_data = json.dumps(
        {
            "echo": "http",
            "method": scope["method"],
            "path": scope["path"],
            "query_string": scope["query_string"].decode(),
            "body": body.decode(errors="replace"),
        }
    ).encode()

    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(response_data)).encode()),
            ],
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": response_data,
            "more_body": False,
        }
    )


async def _handle_websocket(scope, receive, send):
    """Accept the connection and echo every frame back."""
    # Wait for the connect event.
    event = await receive()
    assert event["type"] == "websocket.connect"

    await send({"type": "websocket.accept"})

    while True:
        event = await receive()

        if event["type"] == "websocket.receive":
            if event.get("bytes") is not None:
                # Binary frame — echo as binary.
                await send({"type": "websocket.send", "bytes": event["bytes"]})
            else:
                # Text frame — echo as text.
                await send({"type": "websocket.send", "text": event["text"]})

        elif event["type"] == "websocket.disconnect":
            # Client disconnected; nothing more to do.
            break


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    host = os.environ.get("GRPCGI_HOST", "0.0.0.0")
    port = int(os.environ.get("GRPCGI_PORT", "50051"))

    print(f"Starting grpcgi echo server on {host}:{port}")
    asyncio.run(grpcgi.serve(app, host=host, port=port))
