"""HTTP bridge servicer: translates gRPC streaming requests to ASGI http calls."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator

import grpc
import grpc.aio

from grpcgi._proto.grpcgi.v1 import http_pb2
from grpcgi._proto.grpcgi.v1 import http_pb2_grpc

logger = logging.getLogger(__name__)

# Sentinel placed on send_queue by the app_task wrapper when the ASGI app exits.
_APP_DONE = object()


def _parse_authority(authority: str) -> tuple[str, int]:
    """Return (host, port) from an :authority value like 'example.com:8080'."""
    if not authority:
        return ("localhost", 80)
    if ":" in authority:
        host, _, port_str = authority.rpartition(":")
        try:
            return (host, int(port_str))
        except ValueError:
            return (authority, 80)
    return (authority, 80)


def _build_scope(headers_msg: http_pb2.RequestHeaders) -> dict[str, Any]:
    """Build an ASGI http scope dict from the RequestHeaders proto message."""
    method = "GET"
    raw_path = "/"
    query_string = b""
    scheme = "http"
    authority = ""

    raw_headers: list[tuple[bytes, bytes]] = []

    for h in headers_msg.headers:
        key = h.key
        value = h.value
        # HTTP/2 pseudo-headers drive scope fields but are NOT included in raw_headers
        # (with the exception of :authority which becomes the host header).
        if key == ":method":
            method = value
        elif key == ":path":
            if "?" in value:
                raw_path, _, qs = value.partition("?")
                query_string = qs.encode()
            else:
                raw_path = value
        elif key == ":scheme":
            scheme = value
        elif key == ":authority":
            authority = value
            # :authority becomes the host header visible to the app.
            raw_headers.append((b"host", value.encode()))
        else:
            raw_headers.append((key.encode(), value.encode()))

    host, port = _parse_authority(authority)

    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "2",
        "method": method.upper(),
        "path": raw_path,
        "query_string": query_string,
        "root_path": "",
        "scheme": scheme,
        "server": (host, port),
        "headers": raw_headers,
        "extensions": {},
    }


def _build_scope_from_request(request: http_pb2.HttpRequest) -> dict[str, Any]:
    """Build an ASGI http scope from a unary HttpRequest proto message."""
    return _build_scope(
        http_pb2.RequestHeaders(headers=list(request.headers))
    )


class HttpBridgeServicer(http_pb2_grpc.HttpBridgeServicer):
    """ASGI-bridging implementation of the HttpBridge gRPC service."""

    def __init__(self, app: Any) -> None:
        self._app = app

    async def Handle(
        self,
        request: http_pb2.HttpRequest,
        context: grpc.aio.ServicerContext,
    ) -> http_pb2.HttpResponse:
        """Unary handler: used by Envoy WASM filter via dispatch_grpc_call."""
        scope = _build_scope_from_request(request)

        _body_sent = asyncio.Event()
        _received = False

        async def receive() -> dict[str, Any]:
            nonlocal _received
            if not _received:
                _received = True
                return {"type": "http.request", "body": request.body, "more_body": False}
            await _body_sent.wait()
            return {"type": "http.disconnect"}

        resp_status = 200
        resp_headers: list[tuple[bytes | str, bytes | str]] = []
        resp_body = bytearray()

        async def send(event: dict[str, Any]) -> None:
            nonlocal resp_status
            if event["type"] == "http.response.start":
                resp_status = event.get("status", 200)
                resp_headers.extend(event.get("headers", []))
            elif event["type"] == "http.response.body":
                resp_body.extend(event.get("body", b""))
                if not event.get("more_body", False):
                    _body_sent.set()

        try:
            await self._app(scope, receive, send)
        except Exception:
            logger.exception("HttpBridge.Handle: ASGI app raised")
            await context.abort(grpc.StatusCode.INTERNAL, "ASGI app error")
            return http_pb2.HttpResponse()

        return http_pb2.HttpResponse(
            status=resp_status,
            headers=[
                http_pb2.Header(
                    key=k.decode() if isinstance(k, bytes) else k,
                    value=v.decode() if isinstance(v, bytes) else v,
                )
                for k, v in resp_headers
            ],
            body=bytes(resp_body),
        )

    async def Process(
        self,
        request_iterator: Any,  # grpc.aio _MessageReceiver (async iterable)
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[http_pb2.ProcessingResponse]:
        """Handle one HTTP request: gRPC stream → ASGI → gRPC stream."""

        # Two queues decouple the gRPC read loop from the ASGI coroutine.
        #   receive_queue: grpcgi → ASGI app   (http.request events)
        #   send_queue:    ASGI app → grpcgi   (http.response.* events)
        receive_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        send_queue: asyncio.Queue[Any] = asyncio.Queue()

        # ---- Step 1: read the first message (must be request_headers) --------
        # request_iterator is an async iterable (_MessageReceiver); use anext().
        try:
            first_msg: http_pb2.ProcessingRequest = await request_iterator.__anext__()
        except StopAsyncIteration:
            logger.warning("HttpBridge.Process: stream closed before headers")
            return

        if not first_msg.HasField("request_headers"):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "first message must be request_headers",
            )
            return

        scope = _build_scope(first_msg.request_headers)

        # ---- Step 2: pump remaining gRPC messages → receive_queue in bg task -
        async def _pump_body() -> None:
            """Drain BodyChunk messages from the gRPC request stream."""
            try:
                async for msg in request_iterator:
                    if msg.HasField("request_body"):
                        chunk = msg.request_body
                        await receive_queue.put(
                            {
                                "type": "http.request",
                                "body": chunk.data,
                                "more_body": not chunk.end_of_stream,
                            }
                        )
                        if chunk.end_of_stream:
                            return
                # Stream ended without an explicit end_of_stream flag — signal EOF.
                await receive_queue.put(
                    {"type": "http.request", "body": b"", "more_body": False}
                )
            except asyncio.CancelledError:
                pass

        body_pump_task = asyncio.create_task(_pump_body())

        # ---- Step 3: run the ASGI app concurrently ---------------------------
        # Wrap the app so we can detect when it exits (even without sending body).
        async def _run_app() -> None:
            try:
                await self._app(scope, receive_queue.get, send_queue.put)
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("HttpBridge: ASGI app raised an exception")
            finally:
                # Unblock the send-queue consumer regardless of how the app exits.
                await send_queue.put(_APP_DONE)

        app_task = asyncio.create_task(_run_app())

        # ---- Step 4: consume send_queue and yield gRPC responses -------------
        try:
            while True:
                event = await send_queue.get()

                # App finished without sending more events.
                if event is _APP_DONE:
                    break

                etype = event.get("type")

                if etype == "http.response.start":
                    status: int = event.get("status", 200)
                    resp_headers = [
                        http_pb2.Header(
                            key=k.decode() if isinstance(k, bytes) else k,
                            value=v.decode() if isinstance(v, bytes) else v,
                        )
                        for k, v in event.get("headers", [])
                    ]
                    yield http_pb2.ProcessingResponse(
                        response_headers=http_pb2.ResponseHeaders(
                            status=status,
                            headers=resp_headers,
                        )
                    )

                elif etype == "http.response.body":
                    body_data: bytes = event.get("body", b"")
                    more_body: bool = event.get("more_body", False)
                    if body_data or not more_body:
                        yield http_pb2.ProcessingResponse(
                            response_body=http_pb2.BodyChunk(
                                data=body_data,
                                end_of_stream=not more_body,
                            )
                        )
                    if not more_body:
                        break

                elif etype == "http.disconnect":
                    break

                else:
                    logger.debug("HttpBridge: ignoring unknown event type %r", etype)

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("HttpBridge.Process: error consuming send_queue")
            raise
        finally:
            body_pump_task.cancel()
            app_task.cancel()
            await asyncio.gather(body_pump_task, app_task, return_exceptions=True)
