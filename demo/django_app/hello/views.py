from django.http import HttpResponse

def index(request, **kwargs):
    return HttpResponse(
        "<h1>Hello from Django via grpcgi!</h1>"
        f"<p>Method: {request.method} &nbsp; Path: {request.path}</p>"
        "<p>Transport: Envoy ext_proc → gRPC → ASGI → Django</p>",
        content_type="text/html; charset=utf-8",
    )

def hello(request):
    return HttpResponse(
        "Hello, World!\n",
        content_type="text/plain",
    )
