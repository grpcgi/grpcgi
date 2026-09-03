"""grpcgi — gRPC-to-ASGI bridge.

Public API
----------
serve(app, host="0.0.0.0", port=50051)
    Start the bridge server for the given ASGI *app*.
"""

from grpcgi.server import serve

__all__ = ["serve"]
