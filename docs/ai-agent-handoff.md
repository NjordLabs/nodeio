# AI agent handoff

## Identity

`nodeio` is an async graph engine. Python 3.13 and 3.14. Public
repository `NjordLabs/nodeio`. [`v0.4.0`](https://github.com/NjordLabs/nodeio/releases/tag/v0.4.0)
is the GitHub Release. The wheel sha256 is written when the release
workflow uploads it. That workflow pins pydevops `3.11.3`
(`8b0d7d08`), which checks the repository out before `gh` runs.
Quality jobs stay on pydevops `3.11.1` (`efef9824`).
Pins: guardrails `1.17.1` (`8508e936`), scaffold `1.14.3` (`1fca8a3b`), commondevops `v6.12.0` (`ff65e7ef`).

## Scope

Immutable definitions, a ready-queue run, client pipes, and built-in
routing nodes. Bindings stay rejected (`BIND-001`). Cross-host workers
and timestamped streaming stay open (`WORK-001`, `STREAM-001`). PyPI
publish stays open (`ENG-001-T2`). Lifecycle, edge bounds, cancellation,
output callbacks, the gRPC channel, and the OpenTelemetry observer are
done (`LIFE-001`, `FLOW-001`, `CANCEL-001`, `CB-001`, `GRPC-001`,
`OTEL-001`).

## Commands

```bash
sh scripts/check-ci-local.sh
sh scripts/check-ci-docker.sh
```

The host script is the gate. The container script uses the private
`ghcr.io/pirlruc/ci-python` image and is the stand-in for tools that
are not installed on the host. `uv sync --frozen --extra dev` is the
first step of the host script.

## Known pitfalls

- Private, loopback, and link-local HTTP hosts are rejected at request
  time. A later DNS answer can still change. `allow_private=True`
  disables the check.
- Cancelling a run cancels asyncio node tasks. Thread and process
  workers keep running until they return. `close` still runs.
- `nodeio[grpc]` and `nodeio[otel]` are optional. Importing `nodeio`
  does not import `grpcio` or OpenTelemetry. Quality CI installs the
  extras, so a test must pass both when those packages are present and
  when they are absent. That job also covers the whole tree
  (`--cov=.`), including lines inside tests.
- Observer logging lives in `nodeio.notify`. Tests that replace the
  logger patch `nodeio.notify._logger`.
- `boundary.pyi` is the type stub. Pylint ignores `*.pyi` so the stub
  is not compared with `boundary.py` as a second copy of the same file.
- Coverage data goes stale when sources change. Remove `.coverage`
  before `coverage run` if a report mixes an old file.

## Suggested next work

Publish `0.3.0` to PyPI (`ENG-001-T2`). `WORK-001` and `STREAM-001`
stay open.

## Recent history

- chore(ci): drop stale config and pin pydevops 3.9.0
- fix(executors): share process bytes and close lifecycles
- fix(graph): quote node ids in exports
- feat(run): cancel waves and bound edge buffers
- fix(builtin): reject invalid routes and forward subgraph options
- fix(pipes): stream http bodies and open grpc channels
- feat(otel): span each node from an optional extra
- docs(release): record 0.3.0 without publishing
- fix(quality): bring complexity and docstring gates back under the limits
- build(deps): pin mimirlog 0.2.0 and pymjolnir 0.1.1 release wheels
- release: tag v0.3.0

*Last updated: 2026-11-26.*
