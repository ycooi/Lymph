# Lymph — implementation status

This file says, plainly, what exists in this repository, what does not, and
where the code agrees with or departs from *Lymph Architecture — Detailed
Design 001*. Read it before trusting the code.

Stages are the ones from design section 102.

---

## Stage L0 — Foundation: implemented

| Design item | Where | Notes |
| --- | --- | --- |
| Instance identity, never silently regenerated (§5) | `internal/identity` | `identity.json`; store present but identity missing is a startup failure |
| UUIDv7 for every persistent object (§4) | `internal/identity`, callers | `google/uuid` `NewV7` |
| Append-only hash-chained ledger (§12, §13, §14) | `internal/ledger` | `ledger/YYYY/MM/segment-NNNNNN.log`, versioned hash framing |
| Durability modes (§15) | `internal/ledger`, callers | `ASYNC` writes without fsync; `DURABLE` writes and fsyncs; config mutations are always `DURABLE` |
| Content-addressed object store (§17, §18, §73) | `internal/objectstore` | blobs, trees and revisions under `objects/sha256/ab/...` |
| SQLite projection (§16) | `internal/projection` | WAL, pure-Go driver, no CGO |
| Application and junction registry (§7, §8) | `internal/engine` | immutable registration revisions; UUID identity, names as metadata |
| Managed targets and path ownership (§25, §26) | `internal/engine`, `projection.path_ownership` | a second application registering the same path is refused |
| CloudEvents 1.0 envelope (§10) | `internal/protocol` | hand-implemented and wire-compatible; see deviations |
| HTTP+JSON over a Unix socket (§60, §63) | `internal/httpapi` | socket mode `0660`; no TCP listener exists at all |
| `lymphctl` (§96) | `cmd/lymphctl` | status, issues, events, refs, reflog, audit, rebuild, ledger verify |
| Rebuild from canonical truth (§58, §88) | `internal/engine` | `lymphctl rebuild --yes`, plus automatic catch-up at startup |

## Stage L1 — Learning inbox: implemented

| Design item | Where | Notes |
| --- | --- | --- |
| Fingerprinting and issue grouping (§34, §35, §36) | `internal/fingerprint`, `internal/engine` | generic fallback plus a junction-supplied `fingerprint` override on emit |
| Issue lifecycle (§37) | `internal/engine/state.go` | explicit state machine; illegal transitions are errors |
| Work items and routing (§38, §39) | `internal/engine/work.go` | workflow type inherited from the junction; Lymph routes, it does not fix |
| Leases, at-least-once, expiry sweeps (§41, §42, §43, §87) | `internal/engine/work.go` | lease, renew, complete, sweep at startup |
| Non-blocking client with bounded spool (§66, §67, §68) | `pkg/client`, `examples/python` | Go and Python clients; events carry their identity before first send |

## Stage L2 — Config genome: implemented

| Design item | Where | Notes |
| --- | --- | --- |
| Config families (§24) | `internal/engine`, `projection.config_families` | one family per independently versioned domain |
| Raw-byte canonical revisions (§23, §33) | `internal/engine/config.go` | exact bytes hashed; no YAML canonicalisation |
| Refs, reflog, compare-and-swap (§20, §21, §22) | `internal/engine/config.go` | every ref move is journaled with old, new, reason, issue and candidate |
| Candidates and validation results (§44 to §48) | `internal/engine/config.go` | DRAFT, STRUCTURALLY_VALID, TESTING, PASSED, APPROVED, DEPLOYABLE |
| Fresh-holdout gate (§49) | `internal/engine/config.go` | a family with `requires_holdout` cannot pass without a holdout result |
| Manual approval (§50) | `internal/engine/config.go` | approval required for every promotion in this build |
| Safe promotion with stale-base refusal (§22, §51) | `internal/engine/config.go` | `STALE_CANDIDATE`; the stale candidate is preserved as `STALE` |
| Deployment refs (§52) | `internal/engine/state.go` | `active`, `approved`, `desired`, `deployed`, `last_good`, `candidate/*`, `rollback/*` |
| Audit events (§95) | `internal/engine`, `projection.audit_events` | registration, baseline import, issue transitions |

## Stage L2.5 — typed artifacts and installation identity: implemented

Added after the first audit, because the audit said exactly what was missing: a
worker could only return a configuration, and configuration state could not
differ between two deployments of one application.

| Design item | Where | Notes |
| --- | --- | --- |
| Typed improvement artifacts (§8 of the L2.5 mission) | `internal/engine/improvement.go` | `CONFIG_BUNDLE`, `CODE_REF`, `MODEL_REF`, `DATA_FIX_REF`, `NO_CHANGE`; unknown kinds refused |
| One immutable worker return | `internal/engine/improvement.go`, `projection.improvement_results` | one result per work item, idempotent on repeat |
| Artifact records | `projection.improvement_artifacts` | small immutable metadata; a model is referenced by URI and digest, never copied |
| Atomic canonical return | `ledger.KindImprovementResult` | one record carries result, artifacts, any candidate, and the `RETURNED` work item |
| Lease token | `ReturnImprovementRequest.Attempt` | a stale worker attempt is refused with `ErrStaleWorkerAttempt` |
| Installation registry | `projection.installations`, manifest `installations` | deterministic default; several installations must declare theirs |
| Installation-scoped refs | `config_refs` primary key, reflog, candidates, targets | `active[application, family, installation]` is the stale-base anchor |
| Projection upgrade | `projection.SchemaVersion = 2` | version mismatch drops and replays the projection; canon untouched |

The public lab and package tests cover these contracts with synthetic fixtures.

## Stage L2.6 — daemon handshake and client protocol: implemented

| Design item | Where | Notes |
| --- | --- | --- |
| Handshake | `POST /v1/hello`, `internal/engine/session.go` | validates protocol, identities, installation ownership; returns a session plus capabilities |
| Protocol versioning | `internal/protocol/version.go` | protocol `1`, independent of the daemon version |
| Process identity | `pkg/client/session.go` | UUIDv7 per process, generated once at construction |
| Sessions | `projection.sessions`, `internal/projection/queries_session.go` | ACTIVE, CLOSED, STALE, REJECTED; operational state, never canonical |
| Session header validation | `internal/engine/ingress.go` | application + installation + process must agree; `SESSION_UNKNOWN` on a forgotten session |
| Replay exception | same | a replayed event keeps its original producer and matches on application + installation only |
| Duplicate-process policy | `internal/engine/session.go` | multi-process by default; single-process warns; strict refusal only if configured |
| Typed client errors | `pkg/client/session.go` | `UnavailableError`, `HandshakeRejectedError`, `ProtocolMismatchError`, `IdentityMismatchError`, `SpoolFullError` |
| Reconnect and replay | `pkg/client/client.go` | one handshake + retry on `SESSION_UNKNOWN`; spool flush in order with prefix checkpointing |
| Peer credentials | `internal/httpapi/peercred_*.go` | SO_PEERCRED on Linux, LOCAL_PEERCRED on macOS, "unsupported" elsewhere |
| Socket mode | `lymphd --socket-mode`, `LYMPH_SOCKET_MODE` | default 0660; world-accessible modes are refused |
| CLI diagnostics | `cmd/lymphctl/session.go` | `hello`, `sessions`, `session`, `identity export` |

Four verdicts cover this stage and are reported separately from the deployment
gate. See [INTEGRATING_APPLICATIONS.md](INTEGRATING_APPLICATIONS.md).

## Stage L3 — Circulation closed: not implemented

Deliberately. Design section 102 is explicit: do not jump directly to L3. Prove
first that Lymph can remember and organise system experience correctly, then
that it can version configuration, and only then give it permission to replace
production files.

Not built, and not hidden behind a flag either:

- deployment adapters, atomic single-file install (§29), staged bundle install (§30), legacy exact paths (§31)
- reload strategies (§54), health checks (§55), failed-deployment rollback (§56), crash-during-deployment recovery (§57)
- drift detection for externally edited files (§28)
- CUE schema validation (§32): families register a `schema_revision`, nothing enforces it yet
- secret references and redaction (§59)

`lymphctl deploy` exists only to say so.

## Gaps and honest caveats

1. `ASYNC` durability is a real trade. Async appends reach the file but not the
   platter, so a machine crash can lose the last few feedback events. Every
   configuration mutation is `DURABLE`.
2. The generic fingerprinter is deliberately narrow. It masks digit runs and
   Titlecase words and keeps everything else. It will split a problem that
   varies by a lowercase or all-caps token, and it will not invent a domain
   pattern it cannot see. Junctions that know their own vocabulary should send
   their own `fingerprint`, which is what design section 35 asks for.
   Over-masking was rejected on purpose: merging two different failures into
   one issue with a confident count is worse than listing two issues.
3. Single node. No NATS, no federation, no multi-machine convergence (§77, §80,
   L4). Refs are installation-aware in the schema, but one daemon serves one
   store.
4. No observability export yet (§76). `lymphctl status` and the audit trail
   exist; OpenTelemetry does not.
5. No object garbage collection (§74), which matches the design: nothing
   canonical is deleted.
6. Secrets must not be placed in managed configs yet. Because §59 is
   unimplemented, every historical revision would retain the secret forever.
7. Workflow execution is external by construction (§40, §93): no plugins, no
   embedded Python, no embedded model. Workers are separate processes using the
   same socket API. This build ships the queue and the contracts, not a worker
   runtime.
8. Startup walks the entire ledger to verify the hash chain (§58). That is the
   right behaviour for a store this size and the wrong behaviour for a
   multi-gigabyte one; the fix is a durable checkpoint plus a tail check on
   open, with `lymphctl ledger verify` for the full pass on demand.

---

# What the Lymph Lab found

The lab (`lab/`, `make lab`) drives five deliberately unrelated fake
applications through one daemon and grades 41 items from the test plan. Its
report is `lab/REPORT.md`.

Current grades: **37 PASS, 0 FAIL, 1 PARTIAL, 3 BLOCKED** (after L2.5).

| Verdict | Result |
| --- | --- |
| `LYMPH_EVENT_CORE_VALIDATED` | **VALIDATED** |
| `LYMPH_ADAPTATION_ROUTING_VALIDATED` | **VALIDATED** |
| `LYMPH_CONFIG_GOVERNANCE_VALIDATED` | not validated — no deployment transaction |
| `LYMPH_RESILIENCE_VALIDATED` | not validated — no deployment transaction, no drift detection |
| `LYMPH_001_READY_FOR_SHADOW_INTEGRATION` | **NOT READY** |

## Defects the lab found and this build fixes

These were not hypothetical. Each one was found by a specific test, and each
one is now fixed and covered.

1. **A failed append could tear the ledger** (found by §23 disk-full and §10
   chaos). A write that failed halfway left part of a record on disk, and the
   next append then built on a chain that had moved. `Append` now rewinds the
   segment to the last accepted record, and `Open` truncates a torn tail it
   finds, reporting the byte count instead of pretending nothing happened.
2. **Validation and approval evidence lived only in SQLite** (found by §24
   rebuild). Deleting the projection and replaying the ledger silently erased
   every test result and every approval — the exact evidence the design calls
   the point of the system. Both are now canonical ledger records
   (`VALIDATION`, `APPROVAL`).
3. **Promotion applied the stale-base check to the wrong refs** (found by §41
   organism). `approved` and `desired` legitimately lag `active` in a store
   whose history was imported, and comparing them against the candidate's base
   made a correct promotion fail. Only `active` is the safety anchor now.
4. **Startup replayed the whole ledger every time** (found by §41 restarts,
   as a digest mismatch hiding a missing index row). `Open` replayed from
   sequence zero, which was slow and masked a record that `SweepLeases` never
   indexed. It now resumes from the projection checkpoint, and the missing
   index entry is written.
5. **The daemon's own lifecycle raced** (found by §13 under `-race`).
   `Server.Listen` assigned the `http.Server` in one goroutine while a signal
   handler could call `Shutdown` in another; a SIGTERM arriving early would
   have been ignored. Listening is now split into `Start` (synchronous, socket
   ready) and `Serve`.
6. **The state machines accepted unknown states** (found by §32 fuzzing).
   `CheckCandidateTransition("", "")` returned success because self-transitions
   short-circuited before validation. Every transition function now validates
   both ends first.
7. **A Unicode edge in the fingerprint normalizer** (found by §31 fuzzing).
   Letters with no lowercase mapping (U+03D2) survive normalisation as
   uppercase. That is deterministic and therefore harmless for grouping; the
   property the code claims was narrowed to what is actually true.

## What the lab proves, with numbers

- **Universality**: 5 applications sharing file names (`rules.yaml`,
  `config.yaml`) and family names, driven by one daemon, with zero
  application-specific code in `internal/`, `cmd/` or `pkg/` (scanned).
- **Isolation**: 3 applications with an identical family name, junction name and
  file name kept 1,000 events each in their own history; promoting one left the
  others at r1.
- **Grouping**: 375 events in 4 known shapes produced exactly 4 issues with the
  expected counts; 100 duplicate sends of one event id produced 1 canonical
  event.
- **Safety gate**: an over-general rule was rejected because it misclassified
  freight as an award; production stayed put.
- **Compare-and-swap**: 20 concurrent promotions produced exactly 1 winner, 19
  `STALE_CANDIDATE` refusals, 1 ref move in the reflog and no duplicate revision
  sequence.
- **Crash tolerance**: 6 × `kill -9` during in-flight appends; every
  acknowledged event survived exactly once, the chain verified, and the rebuild
  digest was unchanged.
- **Write failure**: 1,322 events accepted before the filesystem refused
  further writes; nothing half-written became canonical, and the store reopened
  without repair.
- **Rebuild**: deleting the projection and corrupting it both end at the same
  state digest as the original.
- **Disaster recovery**: restoring only `identity.json`, `ledger/` and
  `objects/` reproduces the store exactly.
- **Performance**: benchmark results are environment-dependent and are not
  published as deployment promises. Run the lab in the target environment.

## What is blocked, precisely

Five items cannot run because the capability does not exist yet. They are not
failures; they are the definition of the remaining work.

After L2.5 three items remain blocked, all of them waiting on the same
capability:

| Item | Needs |
| --- | --- |
| §11 kill during deployment | a deployment transaction: stage, validate, atomic install, journal |
| §21 manual edit | target hashing, drift detection, import/restore choice |
| §26 rollback | health checks, `DEPLOYMENT_FAILED`, `last_good` restore |

One item remains PARTIAL: the organism scenario, whose remaining steps are the
manual edit, the failed health check and the rollback. The lineage chain is no
longer partial — it now runs from event through work, improvement result,
artifact, candidate, validations, approval, revision and ref, with only the
deployment link absent because no deployer exists.

## Deviations from the design document

| Design | Here | Why |
| --- | --- | --- |
| CloudEvents Go SDK | hand-written envelope in `internal/protocol` | wire-compatible CloudEvents 1.0 while avoiding a large dependency tree (§92) |
| Watermill for routing (§77) | plain Go, with the ledger as the router | the ledger already is the ordered log; a router abstraction would have added a dependency without adding durability |
| CUE validation (§32) | not implemented | the dependency is unavailable offline; the seam is `schema_revision` plus `RecordValidation` |
| `deployed` ref moves at promotion | moves only at baseline | there is no deployer to report success, and recording a deployment that never happened would poison the audit trail |
