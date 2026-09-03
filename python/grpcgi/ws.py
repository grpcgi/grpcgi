"""WebSocket bridge servicer: translates gRPC bidi-streaming to ASGI websocket."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator

import grpc
import grpc.aio

from grpcgi._proto.grpcgi.v1 import websocket_pb2
from grpcgi._proto.grpcgi.v1 import websocket_pb2_grpc

logger = logging.getLogger(__name__)

# Sentinel placed on send_queue when the ASGI app finishes.
_APP_DONE = object()

# gRPC metadata keys that carry HTTP/2 pseudo-header values for WebSocket
# upgrades forwarded by Envoy. Envoy cannot pass literal `:path` etc. as gRPC
# metadata because gRPC rejects keys that start with `:`.  We use a
# `grpcgi-` prefix by convention; the Envoy filter is configured to match.
_META_PATH = "grpcgi-path"
_META_SCHEME = "grpcgi-scheme"
_META_AUTHORITY = "grpcgi-authority"
_META_PROTOCOL = "grpcgi-protocol"  # sec-websocket-protocol value


def _parse_authority(authority: str) -> tuple[str, int]:
    """Return (host, port) from an authority/host string."""
    if not authority:
        return ("localhost", 80)
    if ":" in authority:
        host, _, port_str = authority.rpartition(":")
        try:
            return (host, int(port_str))
        except ValueError:
            return (authority, 80)
    return (authority, 80)


def _build_scope(metadata: list[tuple[str, str]]) -> dict[str, Any]:
    """Build an ASGI websocket scope from gRPC call metadata.

    Envoy passes the WebSocket upgrade headers as gRPC initial metadata.
    HTTP/2 pseudo-headers are mapped to ``grpcgi-*`` keys because gRPC
    forbids metadata keys starting with ``:``.

    Regular HTTP headers that aren't pseudo-headers are forwarded verbatim
    (lower-cased, as required by the HTTP/2 spec).
    """
    path = "/"
    query_string = b""
    scheme = "ws"
    authority = ""
    raw_headers: list[tuple[bytes, bytes]] = []
    subprotocols: list[str] = []

    for key, value in metadata:
        key_lower = key.lower()

        if key_lower == _META_PATH:
            if "?" in value:
                path, _, qs = value.partition("?")
                query_string = qs.encode()
            else:
                path = value

        elif key_lower == _META_SCHEME:
            scheme = "wss" if value in ("https", "wss") else "ws"

        elif key_lower == _META_AUTHORITY:
            authority = value
            raw_headers.append((b"host", value.encode()))

        elif key_lower == _META_PROTOCOL:
            subprotocols = [p.strip() for p in value.split(",")]
            raw_headers.append((b"sec-websocket-protocol", value.encode()))

        elif key_lower.startswith("grpcgi-"):
            # Other grpcgi-* control keys — skip; not forwarded to the app.
            pass

        elif key_lower in ("user-agent", ":authority"):
            # Skip gRPC internals.
            pass

        else:
            raw_headers.append((key_lower.encode(), value.encode()))

    host, port = _parse_authority(authority)

    return {
        "type": "websocket",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "scheme": scheme,
        "path": path,
        "query_string": query_string,
        "root_path": "",
        "server": (host, port),
        "headers": raw_headers,
        "subprotocols": subprotocols,
        "extensions": {},
    }


class WebSocketBridgeServicer(websocket_pb2_grpc.WebSocketBridgeServicer):
    """ASGI-bridging implementation of the WebSocketBridge gRPC service."""

    def __init__(self, app: Any) -> None:
        self._app = app

    async def Connect(
        self,
        request_iterator: Any,  # grpc.aio _MessageReceiver (async iterable)
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[websocket_pb2.Frame]:
        """Handle one WebSocket connection: bidi gRPC stream ↔ ASGI websocket."""

        # receive_queue: grpcgi → ASGI app
        #   websocket.connect / websocket.receive / websocket.disconnect
        # send_queue: ASGI app → grpcgi
        #   websocket.accept / websocket.send / websocket.close
        receive_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        send_queue: asyncio.Queue[Any] = asyncio.Queue()

        # Build scope from gRPC initial metadata (contains HTTP upgrade headers).
        metadata = list(context.invocation_metadata())
        scope = _build_scope(metadata)

        # Seed the receive queue with the mandatory websocket.connect event.
        await receive_queue.put({"type": "websocket.connect"})

        # ---- Background task: pump incoming gRPC frames → receive_queue ------
        async def _pump_frames() -> None:
            try:
                async for frame in request_iterator:
                    if frame.binary:
                        event: dict[str, Any] = {
                            "type": "websocket.receive",
                            "bytes": frame.payload,
                            "text": None,
                        }
                    else:
                        event = {
                            "type": "websocket.receive",
                            "bytes": None,
                            "text": frame.payload.decode(),
                        }
                    await receive_queue.put(event)
                # Client closed the stream → send disconnect to the app.
                await receive_queue.put({"type": "websocket.disconnect", "code": 1000})
            except asyncio.CancelledError:
                pass

        frame_pump_task = asyncio.create_task(_pump_frames())

        # ---- Run ASGI app concurrently ----------------------------------------
        async def _run_app() -> None:
            try:
                await self._app(scope, receive_queue.get, send_queue.put)
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("WebSocketBridge: ASGI app raised an exception")
            finally:
                await send_queue.put(_APP_DONE)

        app_task = asyncio.create_task(_run_app())

        # ---- Consume send_queue and yield gRPC Frames ------------------------
        try:
            while True:
                event = await send_queue.get()

                if event is _APP_DONE:
                    break

                etype = event.get("type")

                if etype == "websocket.accept":
                    # The gRPC bidi stream is already open; accepting is implicit.
                    # Propagate the negotiated subprotocol as response metadata.
                    subprotocol = event.get("subprotocol")
                    if subprotocol:
                        await context.send_initial_metadata(
                            [("sec-websocket-protocol", subprotocol)]
                        )
                    # No Frame to yield — just continue.

                elif etype == "websocket.send":
                    text: str | None = event.get("text")
                    data: bytes | None = event.get("bytes")
                    if text is not None:
                        yield websocket_pb2.Frame(
                            payload=text.encode(), binary=False
                        )
                    elif data is not None:
                        yield websocket_pb2.Frame(payload=data, binary=True)

                elif etype == "websocket.close":
                    break

                else:
                    logger.debug(
                        "WebSocketBridge: ignoring unknown event type %r", etype
                    )

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("WebSocketBridge.Connect: error consuming send_queue")
            raise
        finally:
            frame_pump_task.cancel()
            app_task.cancel()
            await asyncio.gather(frame_pump_task, app_task, return_exceptions=True)
