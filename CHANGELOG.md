# Changelog

## 0.3.0 - 2026-11-22

- A run closes an executor it created. A caller-supplied executor stays
  open. Fail-fast cancels the other tasks in the wave. Thread and
  process workers run until they return. Observer failures are logged
  and do not replace the node result. `policy` is `fail` or `collect`.
  Collect skips dependents of a failed node and returns the outputs
  that succeeded. Process workers pass bytes through shared memory and
  the parent unlinks that memory if the call fails.
- Gate `open` must be a bool. Mux `select` must be `left` or `right`.
  Graphviz and Mermaid exports quote node ids. A subgraph forwards the
  parent limit, timeout, policy, observer, and executor.
- HTTP returns the body with the status, and stops reading once the
  body passes `max_bytes`. Clients this library builds do not follow
  redirects. `verify=False` requires `allow_insecure=True`. Private,
  loopback, and link-local addresses are rejected unless
  `allow_private=True`. That check runs when the request starts.
  A final HTTP 5xx on an idempotent method raises. The log line records
  `duration_s`, the method, the host, the status, and the attempt.
- A node may implement `open`, `process`, and `close`. `open` runs once
  before the first `process`. `close` runs afterward, including when
  `process` fails or the run is cancelled. `run(..., cancel=)` accepts
  an `asyncio.Event`. An optional `on_output` callback runs when a node
  publishes a value. An edge or the graph may set `queue_size`. The
  default stays unbounded. A full edge waits instead of dropping values.
- `nodeio[grpc]` opens a `grpc.aio` channel from `target`, with a
  deadline and optional mutual TLS. An insecure channel requires
  `allow_insecure=True`. Without the extra, the error says it is not
  installed. With the extra and no target, the error says target is
  required. `nodeio[otel]` starts a span per node when OpenTelemetry is
  installed, and does nothing when it is not.

## 0.2.0 - 2026-11-22

- HTTP and gRPC client nodes with mandatory timeouts, TLS by default,
  and retries only for idempotent HTTP methods. Retries can wait with
  jitter. The owned client accepts a CA bundle and a client certificate.
- Logging observer that records the run id, the node, the duration, and
  the outcome. It does not record the payload. HTTP attempts record the
  method, host, status, and attempt.
- Gate, mux, demux, and named subgraphs.
- Graphviz and Mermaid exports.

## 0.1.0 - 2026-11-22

- Immutable graph definition with cycle detection, typed ports, and
  per-run values.
- Ready-queue scheduler, async, thread, and process executors, and a
  shared-memory byte handle.
- Side packets and a concurrency limit.
