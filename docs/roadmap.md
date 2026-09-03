# grpcgi Roadmap — from prototype to "the way web apps join the mesh"

Strategy: win on **credibility** (spec + conformance + benchmarks), then
**reach** (languages + platforms), then **gravity** (upstreaming + flagship
users). Each phase produces something announceable.

## Phase 1 — Credibility (next 3–6 months)

1. **Freeze and document `grpcgi/v1` as a real spec.** Prose protocol spec
   (message ordering, error mapping, metadata keys, close semantics, flow
   control expectations) separate from the .protos. A protocol without a spec
   is an implementation detail; with one, it's a standard other people can
   implement.
2. **Conformance test suite.** Language-agnostic test client that drives any
   bridge implementation over the wire (think gRPC interop tests). This is the
   single highest-leverage artifact for a multi-language future.
3. **Benchmarks vs. the proxy sandwich.** p50/p99 latency, throughput, and
   memory for: Envoy→HTTP→uvicorn vs. Envoy→grpcgi, plain and streaming, plus
   WebSocket frame throughput. Publish honestly, including where it loses.
4. **Production hardening of the Python bridge:** backpressure/flow-control
   mapping, HTTP trailers, informational responses, graceful drain
   (`SIGTERM` → GOAWAY → ASGI lifespan shutdown), structured errors, and
   observability (metrics + tracing propagation from Envoy metadata).
5. **Packaging:** publish to PyPI, versioned wasm artifact releases, a
   `grpcgi serve app:app` CLI mirroring uvicorn's UX.

## Phase 2 — Reach (6–12 months)

6. **Second-language bridge (Rack for Ruby, or Node http handler).** Proves
   language-neutrality; conformance suite from Phase 1 keeps it honest. Pick
   whichever community shows pull after the talk.
7. **AI inference flagship demo/integration.** vLLM (OpenAI-compatible server
   is FastAPI/ASGI) behind grpcgi: weighted model canaries via xDS, mTLS to
   GPU pods, least-request LB on streaming completions. This is the
   growth-market wedge — write it up as a blog post + KubeCon/AI-infra talk.
8. **SPIFFE/SPIRE integration guide** and an Istio/plain-xDS deployment guide
   with Helm examples. Meet platform teams where they deploy.
9. **gRPC-native health/reflection**: expose `grpc.health.v1` driven by ASGI
   lifespan state so mesh ejection is app-aware.

## Phase 3 — Gravity (12+ months)

10. **Upstream the native Envoy filter** (`envoy.filters.http.grpc_websocket_bridge`
    + HTTP bridge filter). Path: envoyproxy contrib/ first, RFC issue, find an
    Envoy maintainer sponsor. Upstreaming is the moat — once stock Envoy ships
    the filter, grpcgi is infrastructure, not an add-on.
11. **Consider donating the protocol** to a neutral home (CNCF sandbox, or the
    gRPC ecosystem org) once ≥2 language bridges + conformance suite exist.
12. **Flagship production users.** Target teams already on Envoy/xDS with big
    Python fleets (fintech, ML platforms). One public case study beats ten
    features.

## Talk-adjacent amplification

- Blog post version of the talk the same week it's delivered; post to
  r/grpc-adjacent communities, Hacker News, Envoy + gRPC Slack.
- Short demo video (the backup recording) pinned in the README.
- Follow-on talk submissions: EnvoyCon ("upstreaming a serving filter"),
  PyCon ("your Django app is now a gRPC service"), AI-infra events (the vLLM
  story).

## Honest risks to manage

- **ext_proc convergence**: if Envoy's ext_proc grows response-generation
  semantics, differentiate on WebSocket + serving-lifetime semantics, or align
  with it deliberately.
- **Single-maintainer risk**: conformance suite + spec are the mitigation —
  they let contributions scale without you reviewing every line.
- **Perf skepticism**: don't let benchmarks lag the claims; publish early.
