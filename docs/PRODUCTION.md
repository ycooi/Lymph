# Production operations

This document defines the production boundary for Lymph and the evidence needed
before a release is promoted.

## Supported production scope

Lymph is production-supported as a local configuration-governance control
plane in two modes:

1. **feedback shadowing** — applications emit non-blocking feedback while their
   normal operation remains independent of Lymph;
2. **human-gated pull delivery** — an application fetches an approved immutable
   revision, verifies it, applies it with its own application-specific safety
   logic, and reports the outcome.

Lymph does not perform push deployment. It does not overwrite application
files, execute reload commands, restart services, or claim rollback happened.
Those are intentionally outside the supported boundary. A deployment system
may be built later, but it must have a separately reviewed threat model and
failure-atomic protocol; it must not be inferred from pull delivery.

## Release gate

Run from a clean checkout using the Go version declared by `go.mod`:

```bash
make verify-release
```

The gate checks formatting, module checksums, unit and integration tests,
static analysis, both end-to-end smoke paths, the graded lab, the race detector,
and every fuzz target. CI repeats the portable parts on Linux and macOS. A
release is not promotable while any required step fails.

The public release gate uses synthetic fixtures only. Validate application
adapters and deployment behavior separately in the target environment without
committing private paths, logs, measurements, or corpus details.

## Hardened Linux installation

Create a dedicated service identity and directories using the account-management
tools for the distribution. Install the binaries and documentation under
`/opt/lymph`, then install `deploy/lymph.service` under systemd.

Create `/etc/lymph/acl.json` before starting the service. Replace the numeric
IDs with the real service/application, worker, and operator identities:

```json
{
  "principals": [
    { "uid": 991, "roles": ["APPLICATION"] },
    { "uid": 992, "roles": ["WORKER"] },
    { "gid": 993, "roles": ["OPERATOR"] }
  ],
  "default_roles": []
}
```

```bash
sudo install -d -o root -g root -m 0755 /etc/lymph
sudo install -o root -g root -m 0644 acl.json /etc/lymph/acl.json
sudo install -o root -g root -m 0644 deploy/lymph.service /etc/systemd/system/lymph.service
sudo systemctl daemon-reload
sudo systemctl enable --now lymph
sudo systemctl status lymph
```

The service unit requires an ACL, drops capabilities, permits only Unix sockets,
uses a private network namespace, and makes the host filesystem read-only except
for the Lymph state directory. Applications needing the socket must be able to
traverse `/run/lymph` and open `/run/lymph/lymph.sock`; use a deliberately
managed group rather than widening the socket to world access.

Confirm the effective gate from each service identity:

```bash
lymphctl --socket /run/lymph/lymph.sock whoami
lymphctl --socket /run/lymph/lymph.sock health
```

## Backup

The minimum restorable canonical set is:

```text
identity.json
ledger/
objects/
```

Back up the ACL separately. The SQLite database is a disposable projection and
must not be treated as canonical. For a portable, consistent backup, stop the
daemon or use a filesystem snapshot that captures the three canonical items at
one point in time. Do not copy a live ledger and object store independently.

Example with a short maintenance stop:

```bash
sudo systemctl stop lymph
sudo tar --numeric-owner -C /var/lib/lymph -czf lymph-canonical.tgz identity.json ledger objects
sudo sha256sum lymph-canonical.tgz > lymph-canonical.tgz.sha256
sudo systemctl start lymph
```

Store the archive and checksum outside the host. Test restoration periodically;
an untested backup is not a recovery plan.

## Restore and disaster recovery

Restore into an empty state directory, preserve ownership and permissions, and
let Lymph rebuild the projection from canonical truth:

```bash
sudo systemctl stop lymph
sudo install -d -o lymph -g lymph -m 0750 /var/lib/lymph
sudo tar --numeric-owner -C /var/lib/lymph -xzf lymph-canonical.tgz
sudo chown -R lymph:lymph /var/lib/lymph
sudo -u lymph /opt/lymph/bin/lymphd --root /var/lib/lymph --check
sudo systemctl start lymph
```

Do not synthesize a missing `identity.json`; identity loss is a recovery event,
not permission to silently create a different daemon identity.

## Monitoring and incident response

- Monitor service restarts and `GET /v1/health` over the Unix socket.
- Alert on ledger-integrity startup failures, projection rebuild failures,
  rejected handshakes, and sustained spool growth in applications.
- Preserve `identity.json`, `ledger/`, and `objects/` before attempting repair.
- Never edit a ledger segment. Restore verified canonical files and rebuild the
  projection.
- If an approved revision is unsafe, withdraw it and use the application's own
  rollback procedure. Lymph's pull mode records the outcome but does not claim
  authority over application deployment.

## Upgrade and rollback

1. Verify a fresh backup and checksum.
2. Run the new binary with `--check` against a restored copy of the store.
3. Stop the service, replace both binaries atomically, and restart.
4. Confirm `lymphctl version`, `health`, `status`, `whoami`, and `ledger verify`.
5. Retain the previous binaries until the observation window closes.

Projection schema changes are rebuilt from the ledger. Canonical-format changes
require an explicit migration and are not permitted as an implicit startup side
effect.
