"""Wire-schema compatibility tests for the candidate grpcgi v1 contract."""

from grpcgi._proto.grpcgi.v1 import http_pb2, websocket_pb2


def test_header_value_oneof_preserves_empty_values_and_raw_bytes():
    text = http_pb2.Header(key="x-empty", value="")
    raw = http_pb2.Header(key="x-raw", raw_value=b"\xff\x00")

    assert text.WhichOneof("value_kind") == "value"
    assert raw.WhichOneof("value_kind") == "raw_value"
    assert raw.raw_value == b"\xff\x00"


def test_http_stream_messages_expose_eos_context_and_trailers():
    request = http_pb2.ProcessingRequest(
        request_headers=http_pb2.RequestHeaders(
            end_of_stream=True,
            http_version="3",
            remote_address="192.0.2.10",
            remote_port=443,
            root_path="/app",
        )
    )
    trailers = http_pb2.ProcessingResponse(
        response_trailers=http_pb2.Trailers(
            headers=[http_pb2.Header(key="grpc-status", raw_value=b"0")]
        )
    )

    assert request.request_headers.end_of_stream is True
    assert request.request_headers.http_version == "3"
    assert trailers.WhichOneof("response") == "response_trailers"


def test_websocket_session_has_explicit_handshake_and_close_events():
    methods = websocket_pb2.DESCRIPTOR.services_by_name["WebSocketBridge"].methods_by_name
    assert "Connect" in methods  # legacy compatibility RPC
    assert "Session" in methods

    opened = websocket_pb2.WebSocketEvent(
        open=websocket_pb2.WebSocketOpen(
            path="/chat?room=42",
            scheme="wss",
            authority="example.test",
            subprotocols=["chat"],
        )
    )
    closed = websocket_pb2.WebSocketEvent(
        close=websocket_pb2.WebSocketClose(code=1001, reason="draining")
    )

    assert opened.WhichOneof("event") == "open"
    assert closed.close.code == 1001
