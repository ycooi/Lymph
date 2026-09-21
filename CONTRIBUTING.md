# Contributing

## Development setup

Use the Go version declared in `go.mod`, then run:

```bash
make test
make vet
make smoke
make py-smoke
```

Before proposing a release-sensitive change, run `make verify-release`.

## Change requirements

- Preserve the ledger and object store as canonical truth; SQLite must remain
  rebuildable.
- Add tests for state transitions, crash boundaries, authorization decisions,
  and backward compatibility affected by the change.
- Never make push deployment, production file writes, or execution authority an
  implicit side effect of an existing API.
- Keep protocol, daemon, projection, and canonical-format versions separate.
- Update operator documentation and `CHANGELOG.md` for user-visible changes.
- Do not commit credentials, production data, private corpora, generated
  binaries, local spools, or SQLite files.

## Pull requests

Describe the problem, safety invariants, failure modes, tests run, and any
migration or rollback requirements. CI must pass on Linux and macOS. Generated
lab reports should be updated only when the graded evidence intentionally
changes.
