# Changelog

All notable changes to this project will be documented in this file. The format
is based on Keep a Changelog, and releases use semantic version tags.

## Unreleased

### Added

- Kernel-backed peer credential authorization on macOS.
- Production `--require-acl` startup gate.
- Strict ACL parsing and policy-file safety checks.
- Complete route-level role enforcement for application, worker, and operator
  control-plane operations.
- Bounded HTTP request bodies and hardened server timeouts.
- Build provenance metadata and release verification automation.
- Hardened systemd service and production operations runbook.
- GitHub CI, CodeQL, dependency updates, issue templates, and release workflow.
- GitHub-facing use-case guide covering junction selection, configuration
  evolution, and the boundary between adaptation evidence and operations.
- MIT licensing and project attribution for OOI YC and KELE Research.

### Changed

- Build configuration now obtains `GOROOT` and `GOPATH` from the Go tool,
  fixing Homebrew and other symlinked installations.
- Module downloads default to Go's verified public proxy instead of treating a
  partially populated module cache as a complete offline proxy.
- UID and GID value `0` now identify root exactly; omitted selectors, rather
  than zero, express single-dimension matching.

### Security

- Production service startup fails closed when its ACL is absent.
- Group- or world-writable ACL files, symlink policies, negative identities,
  unknown JSON fields, and oversized policies are rejected.
