# Changelog

## 0.2.0 - 2026-11-22

- HTTP and gRPC client nodes with mandatory timeouts, TLS by default,
  and retries only for idempotent HTTP methods.
- Logging observer that records the node and the outcome, not the payload.
- Gate, mux, demux, and named subgraphs.
- Graphviz and Mermaid exports.

## 0.1.0 - 2026-11-22

- Immutable graph definition with cycle detection, typed ports, and
  per-run values.
- Ready-queue scheduler, async, thread, and process executors, and a
  shared-memory byte handle.
- Side packets and a concurrency limit.
