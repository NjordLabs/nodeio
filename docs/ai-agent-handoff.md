# AI agent handoff

## Identity

`nodeio` is an async graph engine. Python 3.13. Public repository
`NjordLabs/nodeio`. `v0.2.0` is the GitHub Release.

## Scope

Immutable definitions, a ready-queue run, client pipes, and built-in
routing nodes. Bindings are rejected in `docs/issues.yml` (`BIND-001`).
Cross-host workers and timestamped streaming stay open (`WORK-001`,
`STREAM-001`).

## Commands

```bash
sh scripts/check-ci-local.sh
```

## Known pitfalls

`v0.2.0` is released. Quality on that pull request passed.

## Suggested next work

PyPI publish stays open. nodeio-serve `v0.1.0` is the ingress adapter.

## Recent history

- feat(engine): replace the loader with a ready-queue run
- fix(quality): split tests, drop Any expressions, and replace random jitter
- docs(readme): install the v0.2.0 release wheel

*Last updated: 2026-11-22.*
