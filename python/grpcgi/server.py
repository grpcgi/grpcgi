"""gRPC server that registers both HttpBridge and WebSocketBridge servicers."""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

import grpc.aio

from grpcgi._proto.grpcgi.v1 import http_pb2_grpc, websocket_pb2_grpc
from grpcgi.http import HttpBridgeServicer
from grpcgi.ws import WebSocketBridgeServicer

logger = logging.getLogger(__name__)


async def serve(
    app: Any,
    host: str = "0.0.0.0",
    port: int = 50051,
) -> None:
    """Start the grpcgi gRPC server and block until shutdown.

    Parameters
    ----------
    app:
        An ASGI callable, ``async def app(scope, receive, send)``.
    host:
        Interface to bind (default ``0.0.0.0``).
    port:
        TCP port to listen on (default ``50051``).
    """
    server = grpc.aio.server()

    http_pb2_grpc.add_HttpBridgeServicer_to_server(
        HttpBridgeServicer(app), server
    )
    websocket_pb2_grpc.add_WebSocketBridgeServicer_to_server(
        WebSocketBridgeServicer(app), server
    )

    listen_addr = f"{host}:{port}"
    server.add_insecure_port(listen_addr)

    await server.start()
    logger.info("grpcgi listening on %s", listen_addr)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _on_signal() -> None:
        logger.info("grpcgi received shutdown signal")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except (NotImplementedError, RuntimeError):
            # Signal handlers are not supported in some environments (e.g. Windows
            # threads). Fall through and rely on KeyboardInterrupt instead.
            pass

    try:
        await stop_event.wait()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        logger.info("grpcgi shutting down")
        await server.stop(grace=5)
        logger.info("grpcgi stopped")
