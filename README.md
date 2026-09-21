# Lymph

**Author:** OOI YC · **Organization:** KELE Research · **License:** [MIT](LICENSE)

An independent local daemon for adaptive configuration governance: it
remembers where an application's configuration failed, versions the
configuration that fixes it, and proves the fix before it is promoted.

It is not an MCP server, not an agent, not an LLM, and not a runtime dependency
of the applications it watches. If Lymph dies, they keep running.

> **Lymph is infrastructure for self-improving software.
> Lymph is not itself an autonomous self-healing or self-modifying system.**

> **Automated workers may propose and validate improvements.
> Human approval is required before a new improvement becomes consumable by a
> node.**

This implements *Lymph Architecture — Detailed Design 001*, stages L0 through
L2.7. Push deployment (L3) is deliberately absent; see
[docs/IMPLEMENTATION-STATUS.md](docs/IMPLEMENTATION-STATUS.md).

For a hardened service installation, backup and restore procedures, release
gates, and the exact supported production boundary, see
[docs/PRODUCTION.md](docs/PRODUCTION.md).

**New here?** [docs/USER-GUIDE.md](docs/USER-GUIDE.md) is the whole story:
getting it running, connecting an application, operating it, and getting the
accumulated feedback back out later for development.

## Why AI software needs Lymph

AI applications are often useful enough to deploy before every real-world case
is known. Their behavior may still depend on prompts, routing rules, tool
policies, entity mappings, retrieval profiles, evidence requirements,
thresholds, normalizers, and domain terminology that continue to evolve in
production.

Once many of these surfaces evolve at the same time, teams start losing the
evidence behind their changes. Git shows what changed, logs show what happened,
monitoring shows whether the system is healthy, and tracing shows how a request
moved through the system. None of them naturally answers:

> **What is production teaching us about how this application's evolving
> configuration needs to improve?**

Lymph gives those lessons a durable path: an application reports meaningful
feedback at a **junction**, Lymph records the reported configuration revision,
groups repeated observations, and preserves the history through proposed and
approved improvements.

## Use cases

| Evolving surface | Example junction | Example application-defined signal |
| --- | --- | --- |
| Classification rules | `classification.outcome` | `UNKNOWN_PHRASE` |
| Entity and alias resolution | `entity.resolve` | `UNKNOWN_ENTITY_MENTION` |
| Agent routing and tool selection | `routing.selection` | `AMBIGUOUS_ROUTE` |
| Prompts | `prompt.response` | `MISSING_EVIDENCE` |
| Retrieval and evidence policies | `retrieval.coverage` | `EVIDENCE_COVERAGE_LOW` |
| Human corrections | any relevant junction | `HUMAN_CORRECTION` |
| Schema and document interpretation | `parser.document` | `UNKNOWN_LAYOUT` |
| Domain terminology and normalization | `normalization.product` | `UNKNOWN_TERM` |

These signals are evidence, not instructions to modify production. Lymph does
not decide the correct repair and does not require a particular improvement
workflow. See [Use Cases](docs/USE-CASES.md) for complete examples, the junction
model, and guidance on what belongs in Lymph rather than operational monitoring.

```text
   application                    Lymph                        external
   -----------                    -----                        --------
   something unexpected  -->  LymphEvent (CloudEvents)
   in production              |
                              +--> append-only hash-chained ledger   (canonical)
                              +--> content-addressed object store    (canonical)
                              +--> SQLite projection                 (rebuildable)
                              |
                              +--> fingerprint --> LymphIssue  (grouped, not 2,813 tickets)
                              +--> WorkItem --lease--> worker / Codex / human
                              |                            |
                              |                      ConfigCandidate
                              |                            |
                              +--> validation records <----+
                              +--> approval (manual)
                              +--> promotion with compare-and-swap
                                       |
                                  immutable revision, refs moved,
                                  reflog explains why, permanently
```

## Quickstart

```bash
make build        # builds bin/lymphd and bin/lymphctl, offline if needed
make smoke        # runs the whole loop in a temporary directory
```

By hand:

```bash
lymphctl init --root /tmp/lymph-demo
lymphd --root /tmp/lymph-demo &
lymphctl --root /tmp/lymph-demo register-app --file examples/semantic-service.manifest.json
lymphctl --root /tmp/lymph-demo status
```

Emit feedback and see it grouped:

```bash
lymphctl emit --app semantic-service --junction semantic.event_state \
  --type UNKNOWN --reason UNKNOWN_EVENT_PHRASE \
  --payload '{"text":"Acme business was booked at 512.00"}'

lymphctl issues
```

From Python (`examples/python/lymph_client.py`, standard library only):

```python
from lymph_client import LymphClient, feedback

lymph = LymphClient(socket_path="/run/lymph/lymph.sock", spool_dir=".lymph-spool")
lymph.emit(
    application="019a8a51-...",
    junction="semantic.event_state",
    feedback_type=feedback.UNKNOWN,
    reason_code="UNKNOWN_EVENT_PHRASE",
    payload={"text": "Acme business was booked at 512.00"},
)
```

If the daemon is down, `emit` spools the event locally and returns. The event
already carries its identity, so the resend is deduplicated by the daemon.

## Layout

```text
cmd/lymphd          the daemon: the only writer of the store
cmd/lymphctl        the operator interface
internal/identity   instance, application, junction and family identities (UUIDv7)
internal/ledger     append-only, hash-chained canonical log
internal/objectstore content-addressed blobs, trees and revisions (SHA-256)
internal/projection SQLite projection, rebuildable from the ledger
internal/engine     registration, ingress, issues, work, config, promotion, replay
internal/httpapi    HTTP+JSON over a Unix domain socket
internal/fingerprint generic normaliser plus junction-supplied fingerprints
pkg/client          Go SDK with a bounded, non-blocking spool
examples/           a manifest, fixture configs and the Python client
docs/               use cases, operations, integration and implementation status
```

Store layout, `/var/lib/lymph` by default:

```text
identity.json          instance identity, never silently regenerated
ledger/2026/09/...     canonical, hash-chained records
objects/sha256/ab/...  canonical, content-addressed bytes
db/lymph.sqlite        projection: derived, disposable, rebuildable
spool/ staging/ snapshots/ locks/
```

## Commands that matter

```bash
lymphctl status                      # what the daemon sees
lymphctl issues --limit 20           # the highest-occurrence problems
lymphctl events --limit 20           # raw feedback, newest first
lymphctl baseline --app A --family F --file /etc/app/config.yaml
lymphctl candidate --app A --family F --dir ./proposed --explanation "..."
lymphctl validate --candidate ID --kind replay --result PASS
lymphctl review  ID                  # what a human reads before deciding
lymphctl approve ID --installation INSTALLATION
lymphctl reject  ID --reason "..."
lymphctl defer   ID --reason "..."
lymphctl promote --candidate ID --actor NAME
lymphctl refs                        # what each pointer currently means
lymphctl reflog                      # why each pointer moved
lymphctl ledger verify               # recompute the hash chain
lymphctl rebuild --yes               # drop SQLite, replay the ledger
```

`lymphctl promote` moves `active`, `approved` and `desired`. It does not touch
`deployed` or `last_good`, because nothing has deployed anything.

## Approved update delivery (L2.7)

Lymph can hand an approved revision back to the node that runs it — as a *pull*.
The node asks, fetches the exact bytes, verifies them against the hash the human
approved, applies them with its own logic at its own safe moment, and reports the
outcome. Lymph never writes an application's files, never signals a reload, and
never restarts anything.

```bash
lymphctl updates --app ts-dataingest-shadow        # what this node may take
lymphctl review  <candidate-id>                    # the human review package
lymphctl approve <candidate-id> --installation <id>
lymphctl application-results --app A               # what nodes reported doing
lymphctl update-dispositions --app A               # per-installation delivery state
lymphctl withdraw <revision-id> --app A --family F --installation I
```

```go
update, err := node.CheckUpdate(ctx, "coverage_policy", currentRevision)
content, err := node.FetchUpdate(ctx, update.ConfigFamilyID, update.RevisionID)
// the application validates, stages, applies and checks — itself
node.ReportApplied(ctx, update.RevisionID, client.AppliedReport{...})
```

The refs stay honest about who did what: `observed` and `last_good` move only
when a node says it applied something, `deployed` stays reserved for a push
adapter that does not exist, and `active`/`approved`/`desired` remain governance.

The public test suite covers this pull contract with synthetic fixtures. It does
not contain adapters, paths, measurements, or data derived from a real
deployment.

## Principles the code enforces

- Lymph controls a configuration's lifecycle; it never invents its meaning. It
  stores `semantic_event_rules` and knows nothing about what AWARDED means.
- Events are immutable. A correction is another event, never an edit.
- Canonical truth is a ledger plus an object store. SQLite can be deleted and
  rebuilt; that path is tested, not assumed.
- Compare-and-swap before promotion. A candidate built on r42 can never
  overwrite r43; it is marked `STALE` and kept.
- Leases, not fire-and-forget. Workers crash, so work returns to the queue and
  every attempt is preserved.
- Deployment targets are registered in advance. An event cannot name a path and
  cannot ask Lymph to execute anything.
- The application never waits. Non-blocking integration is a requirement, not
  an optimisation.

## Development

```bash
make test        # unit and integration tests
make smoke       # the full loop over a real Unix socket
make py-smoke    # the Python client, online and offline
make vet
make lab         # the Lymph Lab: five synthetic applications and graded safety checks
make lab-race    # the concurrency and chaos tests under -race
make lab-fuzz    # time-boxed fuzzing of the parsers and state machines
make lab-full    # all of the above
```

The Makefile resolves `GOROOT` and `GOPATH` through the active Go toolchain and
uses Go's local module cache before its configured proxy. After dependencies
have been cached, set `GOPROXY=off` to enforce a deliberately offline build.

## The Lymph Lab

`lymphd` serving five deliberately unrelated fake applications — an agent, an
MCP-shaped text classifier, an ML service with an external model artifact, an
ingestion parser and an ordinary retry/timeout daemon — all sharing file names
and config-family names on purpose.

The lab answers one question: can one unchanged daemon serve completely
different kinds of software, keep their identities and histories apart, route
each problem to the right repair process, and safely version the result without
understanding anyone's business logic?

```bash
make lab          # ~30 s at small scale; grades 53 items and writes lab/REPORT.md
LYMPH_LAB_SCALE=full make lab    # larger workloads
```

It grades the four verdicts from the acceptance gate:

| Verdict | Current result |
| --- | --- |
| `LYMPH_EVENT_CORE_VALIDATED` | **VALIDATED** |
| `LYMPH_ADAPTATION_ROUTING_VALIDATED` | **VALIDATED** |
| `LYMPH_CONFIG_GOVERNANCE_VALIDATED` | blocked on the deployment transaction |
| `LYMPH_RESILIENCE_VALIDATED` | blocked on the deployment transaction and drift detection |
| `LYMPH_001_READY_FOR_SHADOW_INTEGRATION` | **NOT READY** |

Those three blocked items (`S11_deploy_kill`, `S21_manual_edit`, `S26_rollback`)
describe push-deployment behaviour that Lymph does not have. They were left
blocked rather than satisfied with near-misses, which is why the final gate stays
`NOT READY` while the pull-delivery verdicts below are green.

The first lab run found and fixed seven defects — a ledger that could tear on a
failed write, validation and approval evidence that only existed in SQLite, a
promotion that checked the wrong refs, a full-ledger replay on every start, a
startup/shutdown race in the daemon's own lifecycle, state machines that
accepted unknown states, and a Unicode edge in the normalizer. The details, and
the numbers behind each PASS, are in
[docs/IMPLEMENTATION-STATUS.md](docs/IMPLEMENTATION-STATUS.md).

### Typed artifacts and installations (L2.5)

A worker no longer has to pretend that every answer is a configuration. It
returns an `ImprovementResult` carrying any of five typed artifacts:

```text
CONFIG_BUNDLE   the only kind that creates a configuration candidate
CODE_REF        a repository and commit; Lymph never clones it
MODEL_REF       a URI and digest; Lymph never copies the model
DATA_FIX_REF    a data correction to route, never to apply
NO_CHANGE       the conclusion that the current config is correct
```

One return is one canonical ledger record: the result, its artifacts, any
candidate they created, and the work item moving to `RETURNED`. A crash leaves
either all of it or none of it.

Configuration state is also per installation now. Two deployments of one
application keep separate `active`, `approved` and `desired` refs, separate
candidates, separate reflog entries and separate managed targets — while the
feedback they emit still aggregates into one application's issues, because a
problem is worth understanding once even when it needs fixing twice.

```bash
lymphctl return-result WORK_ID --worker NAME --attempt N --file result.json
lymphctl artifacts --kind MODEL_REF
lymphctl installations --application semantic-service
lymphctl refs --app semantic-service --family semantic_event_rules --installation region-a
```

See [docs/IMPLEMENTATION-STATUS.md](docs/IMPLEMENTATION-STATUS.md) for the
implemented scope and known limitations.

### The daemon handshake and client protocol (L2.6)

Applications now identify themselves once, at start-up or lazily, and stop
thinking about Lymph:

```text
POST /v1/hello          application + installation + process → session
X-Lymph-Session         carried on every event afterwards
X-Lymph-Replay: 1       set when replaying spooled events
```

```python
lymph = LymphClient.from_identity_file("/etc/myapp/lymph.json", spool_dir="/var/lib/myapp/lymph-spool")

if result.unknown:
    lymph.emit_feedback(junction="semantic.event_state",
                        feedback_type="UNKNOWN", reason_code="NO_RULE_MATCH",
                        payload={"text": text})
```

The SDK handles the session, the event UUID, reconnects and spool replay. If the
daemon is down the event is spooled and the application continues; if the daemon
is alive but the identity is wrong, the SDK says so instead of queuing forever.

```bash
lymphctl hello --application semantic-service        # perform the handshake by hand
lymphctl sessions --state ACTIVE            # who is connected
lymphctl identity export --application semantic-service --out /etc/semantic-service/lymph.json
```

Four further verdicts cover it: `LYMPH_CLIENT_PROTOCOL_VALIDATED`,
`LYMPH_SESSION_IDENTITY_VALIDATED`, `LYMPH_RECONNECT_SPOOL_VALIDATED` and
`LYMPH_SOCKET_ISOLATION_VALIDATED`, all VALIDATED — reported separately from the
deployment gate, which is still NOT READY. See
[docs/INTEGRATING_APPLICATIONS.md](docs/INTEGRATING_APPLICATIONS.md).

## Why the name "Lymph"?

The name comes from biology and the original author's biomedical engineering
background. The analogy is deliberately limited: Lymph sits outside the
application's critical request path, collecting unusual observations so they
can be preserved, grouped, examined, and—only when appropriate—used to inform
later adaptation.

The application does not have to stop while this happens. If Lymph is
unavailable, the application continues operating and its client can spool
feedback for later delivery.

> **The application handles what is happening now. Lymph keeps track of what
> it may need to learn from later.**

## License

Copyright © 2026 OOI YC, KELE Research. Distributed under the
[MIT License](LICENSE).
