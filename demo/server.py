#!/usr/bin/env python3
"""
grpcgi demo server.

Serves the Django hello-world app and a WebSocket echo endpoint over the
grpcgi.v1.HttpBridge and grpcgi.v1.WebSocketBridge gRPC protocols.

Run: ./run_server.sh
"""
import asyncio
import os
import sys

# Make the Python prototype importable from ../python/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

# Django setup
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "django_app"))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")

import django
django.setup()
from django.core.asgi import get_asgi_application

import grpcgi

_django = get_asgi_application()


async def application(scope, receive, send):
    """ASGI app: Django for HTTP, inline echo for WebSocket."""
    if scope["type"] == "http":
        await _django(scope, receive, send)

    elif scope["type"] == "websocket":
        await receive()  # websocket.connect
        await send({"type": "websocket.accept"})
        while True:
            event = await receive()
            if event["type"] == "websocket.disconnect":
                break
            if event.get("text") is not None:
                await send({"type": "websocket.send",
                            "text": f"echo: {event['text']}"})
            elif event.get("bytes") is not None:
                await send({"type": "websocket.send",
                            "bytes": event["bytes"]})


if __name__ == "__main__":
    print("grpcgi demo server — grpcgi.v1.HttpBridge + WebSocketBridge on :50051")
    asyncio.run(grpcgi.serve(application))
