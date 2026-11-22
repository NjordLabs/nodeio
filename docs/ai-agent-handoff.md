# AI agent handoff

## Identity

`nodeio` is an async graph engine. Python 3.13. Public repository
`NjordLabs/nodeio`. The branch `feature-nodeio-engine` starts from
`origin/feature-ci` and replaces that tree.

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

Quality jobs pull a private `ci-lint` image. They stay red until
`GHCR_READ_TOKEN` on this repository can read that image.

## Suggested next work

Publish 0.2.0 after the quality job is green. A serving adapter belongs
in a separate repository.

## Recent history

- feat(engine): replace the loader with a ready-queue run
- fix(quality): split tests, drop Any expressions, and replace random jitter

*Last updated: 2026-11-22.*
