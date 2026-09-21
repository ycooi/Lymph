# Security policy

## Supported versions

Security fixes are applied to the latest tagged release and the `main` branch.
Generated operational evidence and private deployment data are not accepted in
the public repository.

## Reporting a vulnerability

When the repository is public, use GitHub's private vulnerability reporting
feature. Do not open a public issue containing an exploit, credentials, private
configuration, or production data. Include the affected version, operating
system, deployment mode, reproduction steps, impact, and any proposed
mitigation.

If private vulnerability reporting is not enabled yet, the repository owner
must configure a private security contact before publishing the repository.

## Security boundary

Lymph exposes HTTP only over a local Unix socket. Production deployments must:

- run as a dedicated unprivileged service account;
- use `--require-acl` with a root-controlled policy file;
- keep the socket non-world-accessible;
- keep secrets out of managed configuration revisions;
- keep push deployment disabled; and
- protect and verify the canonical ledger and object store.

The ledger provides tamper evidence through a hash chain; it is not a digital
signature and does not protect a host whose privileged accounts are compromised.
See `docs/PRODUCTION.md` and `docs/IMPLEMENTATION-STATUS.md` for the complete
supported boundary and known limitations.
