# Lymph user guide

How to put Lymph around an application, and how to get the accumulated feedback
back out later when you sit down to improve the software.

```text
audience   whoever integrates an application, and whoever operates Lymph
scope      getting it running, connecting software, reading the history
not this   a deployment system, a logging system, or a monitoring system
```

Commands use generic names and temporary paths. This public guide contains no
deployment-specific paths, measurements, host details, or application data;
wherever you see `myservice`, read your own service.

---

## 1. What Lymph is, in one page

Lymph remembers where an application's own configuration was not good enough,
versions the change that fixes it, and proves the fix before anyone consumes it.
It is not part of your request path, and it is not a log aggregator.

```text
   your application                 Lymph                       worker / human
   ----------------                 -----                       --------------
   something unexpected  -->  event (CloudEvents)
   in production                 |
                                 +--> append-only hash-chained ledger  (canonical)
                                 +--> content-addressed store          (canonical)
                                 +--> SQLite projection   (derived, disposable)
                                 |
                                 +--> issue (grouped, counted)
                                 |        |
                                 |        +--> work item --> worker / LLM
                                 |                 |
                                 |           candidate config
                                 |                 |
                                 |        validations <--+
                                 |        human approval
                                 |        promotion (compare-and-swap)
                                 |                 |
                                 |     immutable revision, refs moved, reflog
                                 |                 |
                                 +<-- node asks, fetches, verifies, applies,
                                      reports APPLIED or REJECTED
```

Two sentences to keep:

> The application never waits for Lymph. Lymph unavailable is not the
> application unavailable.

> Workers may propose and validate. A human approves before anything becomes
> consumable by a running system.

### 1.1 Three levels of integration

You may stop at any of them.

```text
LEVEL 1  feedback-only      the application reports where its own configuration
                            was inadequate. Lymph groups, counts, remembers.
                            No writes, no risk. Most applications stop here.

LEVEL 2  + governance       Lymph versions the config that fixes it: candidates,
                            tests, validations, human approval, immutable
                            revisions, refs, reflog. Lymph still writes nothing
                            to your machine.

LEVEL 3  + pull delivery    the node asks what is approved, fetches the exact
                            bytes, verifies them, applies them with its own
                            logic, and reports the outcome.
```

Level 1 is worth doing on its own: it answers "which parts of my system keep
being wrong about the world?" with numbers instead of anecdotes.

### 1.2 Vocabulary

| word | meaning |
| --- | --- |
| **application** | a piece of software, identified by a UUID. Not a hostname, not a process |
| **installation** | one deployment of it (Region A, Region B). Approvals and delivery are per installation |
| **junction** | a place in the application where its own rules can turn out to be inadequate |
| **config family** | an independently versioned piece of configuration |
| **feedback event** | one signal at one junction, with a reason code and a small payload |
| **issue** | many events grouped by fingerprint — one problem, counted |
| **occurrence** | one event's membership in an issue |
| **candidate** | a proposed change to a family, before approval |
| **revision** | an immutable, content-addressed version of a family |
| **ref** | a named pointer: `active`, `approved`, `desired`, `last_good`, `observed`, `deployed` |
| **management mode** | `LYMPH_MANAGED` or `EXTERNAL`. Only managed families are ever delivered |

---

## 2. Getting it running

```bash
make build                     # builds bin/lymphd and bin/lymphctl, offline if needed

./bin/lymphctl init --root /var/lib/lymph
./bin/lymphd --root /var/lib/lymph --socket /run/lymph/lymph.sock &

./bin/lymphctl --socket /run/lymph/lymph.sock status
./bin/lymphctl --socket /run/lymph/lymph.sock health
```

The daemon listens on a **Unix socket**, not a TCP port. That is deliberate: it
is local only, it inherits filesystem permissions, it needs no TLS, and it
cannot be reached from the network by accident.

What lands on disk:

```text
/var/lib/lymph/identity.json      instance identity, never silently regenerated
/var/lib/lymph/ledger/2026/09/    canonical, hash-chained, append-only
/var/lib/lymph/objects/sha256/    canonical, content-addressed bytes
/var/lib/lymph/db/lymph.sqlite    derived index; delete it and rebuild
/var/lib/lymph/locks, snapshots, staging/, spool/   reserved working directories
```

Know these three facts before you rely on any of it:

* the **ledger and the object store are the truth**;
* SQLite is a convenience index and can be rebuilt at any time;
* sessions and the client spool are operational state and are *not* history.

```bash
./bin/lymphctl ledger verify      # recompute the hash chain
./bin/lymphctl rebuild            # drop the index, replay the ledger
```

---

## 3. Register your application (once)

Everything Lymph knows about your software comes from one manifest. It never
reads your code, your config files, or your database.

```json
{
  "name": "myservice",
  "type": "service",
  "owner": "ops",
  "repository": "team/myservice",
  "junctions": [
    { "name": "classification.outcome", "improvement_workflow": "rule-repair",
      "config_family": "classification_rules",
      "feedback_types": ["UNKNOWN", "AMBIGUOUS", "CONFLICT", "HUMAN_CORRECTION"] },
    { "name": "entity.resolve", "improvement_workflow": "alias-review",
      "config_family": "entity_aliases",
      "feedback_types": ["UNKNOWN", "AMBIGUOUS", "HUMAN_CORRECTION"] }
  ],
  "config_families": [
    { "name": "classification_rules", "schema_revision": "rules-v3",
      "workflow_type": "rule-repair", "management_mode": "LYMPH_MANAGED",
      "targets": [{ "target_type": "SINGLE_FILE", "path": "/etc/myservice/rules.yaml",
                    "reload_policy": "SIGHUP", "atomicity": "FULL" }] },
    { "name": "entity_aliases", "schema_revision": "aliases-v1",
      "workflow_type": "alias-review", "management_mode": "LYMPH_MANAGED",
      "targets": [{ "target_type": "SINGLE_FILE", "path": "/etc/myservice/aliases.yaml",
                    "reload_policy": "WATCH_FILE", "atomicity": "FULL" }] },
    { "name": "api_tokens", "schema_revision": "tokens-v1",
      "management_mode": "EXTERNAL" }
  ]
}
```

```bash
./bin/lymphctl register-app --file myservice.manifest.json
./bin/lymphctl apps          # registered applications
./bin/lymphctl junctions     # what Lymph knows how to route
./bin/lymphctl families      # families and their management mode
```

Re-registering with the same manifest is idempotent: it produces no new
revision. Changing it creates a new registration revision; history is not
edited.

### 3.1 Choosing junctions

Three good ones beat thirty. The test for each: *could a configuration or rule
change plausibly improve this behaviour?*

| good junction | why | bad junction | why not |
| --- | --- | --- | --- |
| "the classifier matched no rule" | a rule could fix it | every function call | noise |
| "the mention resolved to two entities" | an alias map could fix it | request latency | not configuration |
| "the claim had too little evidence" | a threshold could fix it | handled exceptions you log | already handled |
| "a human overruled the answer" | the best signal there is | successful operations | nothing to fix |

Likely junctions by shape: `classification.*`, `entity.*`, `qualifier.*`,
`retrieval.*`, `parser.*`, `schema.*`, `release.*`, `router.*`, `policy.*`.

### 3.2 The management boundary

`LYMPH_MANAGED` families can be delivered to a node. `EXTERNAL` families are
**never** deliverable — not after approval, not after promotion. The default is
`EXTERNAL`, so a family has to opt in.

Lymph has no secret detection and no secret storage. The boundary is declared,
not inferred, and that is the whole mechanism: credentials, tokens and anything
financial go in families you leave `EXTERNAL`.

### 3.3 Give the application its identity

```bash
./bin/lymphctl identity export --application myservice --installation region-a \
  --out /etc/myservice/lymph.json
chown myservice:myservice /etc/myservice/lymph.json && chmod 0640 /etc/myservice/lymph.json
```

```json
{ "application_id": "019a8a51-...", "installation_id": "019a8a52-..." }
```

It is not a secret — it is the pair of UUIDs the daemon already knows — but it is
identity, so it should not be world-readable. Export one file per installation:
approvals and delivery bind to an installation, not to "the application".

---

## 4. Level 1 — reporting feedback

### 4.1 Build one client, once

```python
# myservice/lymph.py
from lymph_client import LymphClient, feedback

client = LymphClient.from_identity_file(
    "/etc/myservice/lymph.json",
    spool_dir="/var/lib/myservice/lymph-spool",
    client_name="myservice",
    client_version="1.4.0",
)

# Opportunistic, never blocking: hand over whatever was measured while Lymph
# was unreachable.
try:
    client.flush_spool()
except Exception:
    pass
```

```go
// Go SDK, same contract
lymph, err := client.ClientFromIdentityFile("/etc/myservice/lymph.json",
    "/var/lib/myservice/lymph-spool",
    func(o *client.ApplicationOptions) { o.ClientName = "myservice" })
```

Construct it once at start-up, never per request, and never block on it.

### 4.2 Emit where your own rules fell short

```python
def classify(document, evidence_id, config_revision):
    result = rules.match(document)

    if result.is_unknown:
        client.emit_feedback(
            junction="classification.outcome",
            feedback_type=feedback.UNKNOWN,
            reason_code="UNKNOWN_PHRASE",
            payload={
                "evidence_id": evidence_id,       # a stable pointer
                "span": result.closest_span,      # the smallest useful text
                "normalized": result.normalized,  # what the matcher actually saw
            },
            input_ref=evidence_id,
            config_revision=config_revision,      # what was in force
        )
    return result          # the caller's answer is unchanged either way
```

Four more, one per common shape:

```python
# matched, but the decision is not determinable
client.emit_feedback(junction="classification.outcome", feedback_type=feedback.AMBIGUOUS,
                     reason_code="ACTOR_ROLE_UNCLEAR",
                     payload={"evidence_id": ev, "candidates": ["buyer", "broker"]},
                     config_revision=rev)

# two mutually exclusive outcomes both matched
client.emit_feedback(junction="classification.outcome", feedback_type=feedback.CONFLICT,
                     reason_code="STATE_CONFLICT",
                     payload={"evidence_id": ev, "states": ["AWARDED", "TENDER_OPEN"]},
                     config_revision=rev)

# a person overruled it — the most valuable signal there is
client.emit_feedback(junction="classification.outcome", feedback_type=feedback.HUMAN_CORRECTION,
                     reason_code="HUMAN_CORRECTION_RECORDED",
                     payload={"evidence_id": ev, "from": "UNKNOWN", "to": "AWARDED"},
                     config_revision=rev)

# a mention that resolves to nothing
client.emit_feedback(junction="entity.resolve", feedback_type=feedback.UNKNOWN,
                     reason_code="UNKNOWN_ENTITY_MENTION",
                     payload={"evidence_id": ev, "mention": mention},
                     config_revision=rev)
```

Do **not** emit: every function call, ordinary handled exceptions, request
timings, debug messages, or successful operations. Lymph is not the log file.

### 4.3 Payload discipline

| put in | keep out |
| --- | --- |
| a stable evidence id (`doc://2026-09-14/4100`) | the whole document |
| the smallest span that failed, normalised | the whole attachment |
| the candidate set the matcher produced | the whole database row |
| counters, thresholds, revision ids | anything you would not email |

The rule: a payload should let somebody *reproduce the judgement*, not read your
business. Every byte you send lives in a ledger and in every export you make from
it afterwards. This is much easier to hold to at the start than to retrofit after
six months.

### 4.4 Two guarantees that make this safe

```text
1  Lymph down is not the application down
   emit() tries the socket. If it cannot reach it, the event goes to
   <spool_dir>/incoming.jsonl (bounded at 10 000 events) and the call returns.
   Your caller's answer is unchanged. Always.

2  Sending the same event twice is free
   the event id is generated before the send, so a resend after a crash is
   deduplicated by the daemon ("duplicate": true) and changes nothing.
```

The synthetic client tests cover this behavior by stopping the daemon, spooling
events, restarting it, and verifying an ordered drain.

---

## 5. Level 2 — governing a change

This is the operator's half. Nothing here touches your machine; it is how a fix
becomes an approved revision.

```bash
# 1. import what production is running today, as the starting revision.
#    This is a prerequisite: a candidate cannot exist for a family that has no
#    baseline, and the error you get if you skip it says exactly that.
lymphctl baseline --app myservice --family classification_rules \
  --file /etc/myservice/rules.yaml --author ops

# 2. a candidate proposes a change (a worker or a human produces this).
#    --base defaults to the current active revision.
lymphctl candidate --app myservice --family classification_rules \
  --file ./proposed/rules.yaml --base r1 --issues ISSUE_ID \
  --explanation "add the phrase the desk kept hitting"
lymphctl candidates --app myservice --family classification_rules --state DRAFT

# 3. validations are evidence; they are not approval
lymphctl validate --candidate ID --kind structural --result PASS --suite myservice.rules.structural.v1
lymphctl validate --candidate ID --kind replay     --result PASS --suite myservice.rules.replay.v1
lymphctl validate --candidate ID --kind regression --result PASS --suite myservice.rules.regression.v1

# 4. the review package a human reads before deciding
lymphctl review ID

# 5. the decision, and then the immutable revision
lymphctl approve ID --installation region-a --comment "reviewed the replay output"
lymphctl promote ID --actor operator

lymphctl refs --app myservice --family classification_rules --installation region-a
lymphctl reflog
lymphctl audit
```

Three things about that sequence that are easy to get wrong, and that running it
will tell you:

* **A baseline comes first.** `semantic rules have no active revision` means step
  1 has not happened for that family yet.
* **A family that declares `requires_holdout` needs a fourth validation.**
  Structural, replay and regression take a candidate to `TESTING`; without a
  `--kind holdout` result it stays there, and `promote` answers
  `only APPROVED candidates can be promoted`. Ask for the holdout before you
  plan the change. The delivery gate itself requires structural, replay and
  regression.
* **A candidate is an anchor too.** `refs` shows `candidate/<id>` entries: the
  exact content a decision was made about, kept next to the governance refs.

A work item is queued from issues, not from a junction:

```bash
lymphctl queue --issues ISSUE_ID[,ISSUE_ID] [--workflow W] [--installation I]
```

A complete synthetic test covers baseline import, work queueing, a returned
`CONFIG_BUNDLE`, validation, review, approval, and promotion. The expected end
state is:

```text
refs (per family, per installation)
  active      <revision>        approved    <revision>
  desired     <revision>        deployed    <baseline>   <- never moves in pull mode
  last_good   <baseline>        observed    (unset)
  candidate/<candidate-id>      sha256:<hash of the content that was decided on>

lymphctl updates --app myservice --family retrieval_profile
  <revision> base_compatible True sha256:ea84cf37533a...
```

`last_good` and `observed` are still on the baseline because no node has reported
anything yet. That is the pull model working: governance moved, and the running
system has not.

What the refs mean, and what moves them:

| ref | means | moves when |
| --- | --- | --- |
| `active` | the governance head, the anchor for compare-and-swap | promotion |
| `approved` | the revision a human approved | promotion |
| `desired` | what this installation should eventually run | promotion |
| `last_good` | the last revision a node confirmed working | a node reports applied |
| `observed` | the revision the node says it is running | a node reports applied |
| `deployed` | reserved for a future push adapter | nothing, in this build |

Two rules worth internalising: promotion is compare-and-swap, so a candidate built
on an old revision is marked `STALE` rather than overwriting anything; and
credentials or money-touching configuration should never be in a family marked
`LYMPH_MANAGED`.

### 5.1 Human approval is not optional

```text
no approval        nothing is offered; a fetch is refused
machine approval   recorded as SYSTEM; still not deliverable
human approval     deliverable, for one installation and one exact content hash
```

Only an operator principal can approve, and who counts as an operator is decided
by the daemon from local peer credentials plus a policy file — never from the
request body. `lymphctl whoami` tells you what this connection may do.

---

## 6. Level 3 — taking an approved update

Only once Level 1 has shown you something worth changing. The node does the work;
Lymph only says what is approved.

```python
# at a safe point: start-up, an idle transition, or an operator signal
update = client.check_update("classification_rules", current_revision)

if update is None:
    pass                                   # the normal answer; stay quiet
elif update.get("base_compatible") is False:
    log.warning("approved %s was built on %s; we run %s",
                update["revision_id"], update["base_revision_id"], current_revision)
else:
    content = client.fetch_update(update["config_family_id"], update["revision_id"])
    # Every file and the tree have already been re-hashed. A mismatch raises
    # IntegrityError and you must not apply it.

    problem = validate_rules(content["bundle"]["rules.yaml"])   # YOUR validator
    if problem:
        client.report_rejected(update["revision_id"], update["config_family_id"],
                               "LOCAL_VALIDATION_FAILED", message=problem)
    else:
        stage_and_swap(content["bundle"]["rules.yaml"])   # your atomicity
        reload_rules()                                    # SIGHUP, HTTP, in-process
        if healthy():
            client.report_applied(update["revision_id"], update["config_family_id"],
                                  observed_hash=content["root_tree_hash"],
                                  previous_revision_id=current_revision,
                                  details={"reload": "SIGHUP", "took_ms": 40})
        else:
            client.report_rejected(update["revision_id"], update["config_family_id"],
                                   "HEALTH_CHECK_FAILED", message="post-reload probe failed")
```

There is no `apply_update()` in the SDK, in any language, on purpose. Only your
application knows its format, its safe reload point, and whether a restart is
needed.

The four calls, one line each:

```text
CheckUpdate    is there something approved for me that I am not running?
FetchUpdate    give me the exact bytes — I verify them myself
ReportApplied  I am running it  (the only thing that moves observed/last_good)
ReportRejected I will not run it, and here is the reason code
```

Worth knowing:

* **Refusing is a first-class outcome.** A rejection becomes feedback, linked to
  the problem the change claimed to fix, so the next worker sees it.
* **A refused revision does not block the future.** A later revision built on top
  of one you refused is still offered; the response names what it steps over.
* **Reporting twice is safe** (idempotent). Reporting a hash that is not the
  revision's is refused with `INTEGRITY_ERROR`.
* **Nothing is applied that you have not verified.**

---

## 7. Operating it

### 7.1 The commands you will actually type

```bash
lymphctl status                        # overview: applications, issues, sessions
lymphctl whoami                        # what this connection is allowed to do
lymphctl health                        # liveness only
lymphctl apps / junctions / families   # what is registered
lymphctl issues --limit 20 --order count
lymphctl issue ISSUE_ID                # one problem in full
lymphctl events --limit 50             # raw events, newest first
lymphctl sessions --state ACTIVE       # which processes are connected
lymphctl hello --application myservice # the handshake by hand, as a diagnostic
lymphctl refs / reflog / audit         # configuration history, who did what
lymphctl ledger verify                 # integrity of the canonical log
lymphctl rebuild                       # drop the index, replay the ledger
lymphctl updates --app myservice       # what this node may take
lymphctl application-results --app myservice
lymphctl update-dispositions --app myservice
```

### 7.2 Daily shape

```text
once, at install        init, register, export identity
start-up of the app     construct the client, try to flush the spool, carry on
at failure boundaries   emit; nothing else, ever
weekly (five minutes)   issues --order count; read what is growing
when something is fixed baseline, candidate, validate, review, approve, promote
monthly                 export the feedback, read the histograms, archive
```

### 7.3 Backup, retention, what is safe to delete

```text
canonical, keep         <root>/ledger/   +  <root>/objects/
derived, disposable     <root>/db/       (rebuild regenerates it)
operational, short      sessions and client spools
your own exports        wherever you put them, timestamped
```

A copy of the ledger plus the object store is a complete backup. The synthetic
rebuild tests restore events, issues, approvals, revisions, application results,
and delivery refs after a full apply.

### 7.4 Authorization

```json
{
  "principals": [
    { "uid": 1001, "roles": ["APPLICATION"], "applications": ["<app uuid>"] },
    { "uid": 1002, "roles": ["WORKER"] },
    { "gid": 1003, "roles": ["OPERATOR"] }
  ],
  "default_roles": []
}
```

```bash
lymphd --acl /etc/lymph/acl.json --root /var/lib/lymph &
lymphctl whoami          # authorization_mode: ACL, and the roles you hold
```

Three roles: `APPLICATION` (emit, check, fetch, report), `WORKER` (claim work,
return results, record validations), `OPERATOR` (approve, reject, defer,
withdraw). Roles come from the kernel's view of the local user, never from the
request. Without a policy file the daemon says so plainly: single-principal local
trust, where the socket permissions are the boundary.

In production, add `--require-acl` (or `LYMPH_REQUIRE_ACL=true`). Policy files
must be regular, non-symlink files that are not writable by group or others.
Omitting `uid` means match by `gid`; omitting `gid` means match by `uid`; numeric
zero identifies root and is not a wildcard.

---

## 8. Getting the feedback back out, later

This is the part that pays for the integration. The feedback accumulates; the
question is how to retrieve it when you sit down to improve the software.

### 8.1 Four ways to read, and when to use each

| surface | answers | use it for |
| --- | --- | --- |
| `lymphctl events` / `issues` / `audit` | "what just happened?" | day-to-day operator work |
| `GET /v1/events`, `/v1/issues`, `/v1/events/{id}` | the same, from a script | small reads, dashboards, alerts |
| `scripts/export-feedback.py` | "what has accumulated, as a dataset?" | development, mining, feeding a worker |
| the ledger files | "what is canonically true?" | audit, migration, proving a rebuilt index |

Read the projection for questions — it is the shape that answers them — and treat
the ledger as the truth. If the index is lost, `lymphctl rebuild` recreates it.

### 8.2 The export tool

```bash
python3 scripts/export-feedback.py \
  --root /var/lib/lymph \
  --application myservice \
  --since 2026-09-01 \
  --out /tmp/myservice-feedback
```

It opens the store in SQLite `query_only` mode, so it cannot write to it, and it
works while the daemon is running. It takes an application name or UUID, and
`--limit` caps the events (issues are always complete, because the summary is
what you read first).

Four files, deterministic and diffable:

```text
summary.json        counts, window, and the histograms below
issues.ndjson       one issue per line, most occurrences first
events.ndjson       one event per line, each carrying its issue_id
occurrences.ndjson  the event-to-issue link, with timing
```

The summary contains counts, time windows, per-junction totals, per-reason totals,
spool replay counts, and events without an issue. Two figures answer different
questions:

```text
occurrences   how much pain this cause is responsible for   triage with it
issues        how many distinct things have to be fixed      work with it
```

Many occurrences resolving into a few issues call for different work than the
same number of occurrences spread across many issues.

### 8.3 Read the files, not the console

In `events.ndjson`, each line is the whole story for one event: the junction, the
reason code, the decoded payload, which revision was in force, when the
application saw it and when Lymph received it, which session and process sent it,
and the issue it was grouped into. Trimmed:

```json
{
  "event_id": "01a0be42-c434-7000-8d87-cee7daeb971c",
  "junction": "retrieval.coverage",
  "feedback_type": "OUT_OF_CONTRACT",
  "reason_code": "NO_EVIDENCE_FOR_CLAIM",
  "fingerprint": "sha256:bd3b210f1b58...",
  "issue_id": "01a0be42-c3bb-70dc-b9cc-479fc8588a65",
  "config_revision": "8f2c1a94",
  "input_ref": "doc://2026-09-14/4100",
  "data": { "feedback_type": "OUT_OF_CONTRACT", "reason_code": "NO_EVIDENCE_FOR_CLAIM",
            "payload": { "claim_type": "shipment_closed", "spans_found": 0 } },
  "occurred_at": "2026-09-20T10:00:41.78Z",
  "received_at": "2026-09-20T10:00:41.780087499Z",
  "durability": "ASYNC",
  "ledger_sequence": 165
}
```

`occurred_at` and `received_at` differ whenever an event waited in a client spool.
That gap is how much of your history arrived late, and a large gap is worth
knowing about.

### 8.4 The one judgement call: what counts as one thing

Groups are decided by the **fingerprint**, and the default fingerprint includes
the payload. By default, then, one unknown phrase is one issue and one unresolved
mention is one issue — usually right, because each needs its own rule.

It is wrong when one root cause produces hundreds of instances. Choose the
grouping level that matches the intended fix:

| one config change fixes… | payload / key | resulting grouping |
| --- | --- | --- |
| one instance at a time (a phrase, an alias) | payload varies per instance | one issue per unique instance |
| one class of instance at a time | payload stable per reason code | one issue per stable class |
| every instance of one root cause | junction-supplied key | one issue per root cause |

All three are the same data grouped three ways; none of them is a bug. Pick the
one that matches how you would actually fix the problem, and decide it at the
junction rather than by retuning Lymph.

If you are drowning in issues from one cause, supply your own key at the junction
rather than retuning Lymph:

```python
client.emit_feedback(junction="retrieval.coverage",
                     feedback_type=feedback.LOW_CONFIDENCE,
                     reason_code="EVIDENCE_COVERAGE_LOW",
                     payload={"claim_type": "price_fixed", "spans_found": 1, "required": 3},
                     fingerprint="myservice:retrieval:coverage:price_fixed")
```

### 8.5 From an export to an improvement

The export is the input to the loop, not a report to file away.

```bash
# 1. what is worth fixing first?
python3 - <<'PY'
import json
s = json.load(open("/tmp/myservice-feedback/summary.json"))
for reason, n in s["occurrences_by_reason_code"].items():
    print(f"{n:5d}  {reason}")
PY

# 2. the examples behind the top reason
grep '"reason_code": "UNKNOWN_PHRASE"' /tmp/myservice-feedback/events.ndjson |
  python3 -c "import sys,json
for line in sys.stdin: print(json.loads(line)['data']['payload']['span'])" |
  sort | uniq -c | sort -rn | head

# 3. queue it; a worker proposes; tests run; a human approves
lymphctl issues --limit 20 --order count
lymphctl queue --issues ISSUE_ID
CLAIM=$(lymphctl --json claim --worker worker-1)      # take work_id and attempts from this
lymphctl return-result "$(jq -r .work_id <<<"$CLAIM")" \
  --worker worker-1 --attempt "$(jq -r .attempts <<<"$CLAIM")" --file result.json
lymphctl validate --candidate ID --kind structural --result PASS --suite myservice.rules.structural.v1
lymphctl validate --candidate ID --kind replay     --result PASS --suite myservice.rules.replay.v1
lymphctl validate --candidate ID --kind regression --result PASS --suite myservice.rules.regression.v1
# ... plus --kind holdout if the family declares requires_holdout
lymphctl review ID
lymphctl approve ID --installation region-a
lymphctl promote ID --actor ops

# 4. export again later and compare: the targeted reason code should carry fewer
#    occurrences, and the probes that should still fire must still fire
python3 scripts/export-feedback.py --application myservice \
  --since 2026-10-01 --out /tmp/myservice-feedback-oct
```

### 8.6 Reading the store by hand

Two joins cover most manual reading. The projection is derived, so open it
read-only and be ready for `rebuild` to rewrite it:

```sql
-- every event with the issue it was grouped into
SELECT e.received_at, e.reason_code, o.issue_id, e.input_ref
FROM events e
LEFT JOIN issue_occurrences o ON o.event_id = e.event_id
WHERE e.application_id = ?
ORDER BY e.received_at DESC LIMIT 100;

-- the issues behind one junction, worst first
SELECT reason_code, occurrence_count, status, last_seen
FROM issues
WHERE application_id = ? AND junction_id = ?
ORDER BY occurrence_count DESC;
```

`git blame` for configuration means `lymphctl reflog`: every ref move, why it
moved, and what it pointed at before.

---

## 9. Troubleshooting

| symptom | likely cause | what to do |
| --- | --- | --- |
| nothing shows in `lymphctl events` | the events are still in the client spool | `lymphctl status` shows spool replays; look at `<spool_dir>/incoming.jsonl` |
| `INVALID_IDENTITY` on handshake | `process_id` was set to something that is not a UUID | let the SDK generate it; do not pass a name |
| `UNKNOWN_APPLICATION` | the manifest was never registered, or the client is pointed at another store | `lymphctl apps`, then register |
| `UNKNOWN_JUNCTION` | a typo in the junction name | `lymphctl junctions` |
| `401 SESSION_REQUIRED` / `SESSION_UNKNOWN` | no session yet, or the daemon restarted | the SDK re-handshakes; only a repeated fault matters |
| `403 FORBIDDEN` on approve | this local user holds no `OPERATOR` role | `lymphctl whoami`, then fix the policy |
| one issue per event | payload varies and the default fingerprint is in use | see §8.4 before changing anything |
| an approved update is never offered | the family is `EXTERNAL`, the approval is for another installation, or the node already rejected it | `lymphctl updates --app ..., update-dispositions` |
| `INTEGRITY_ERROR` | the bytes do not hash to what was approved | do not apply them; this is the check working |
| `BASE_REVISION_MISMATCH` | the update was built on a revision this node is not on | expected after skipping revisions; see §6 |
| the daemon refuses to start | store permissions, or an unparsable `--acl` file | it fails loudly rather than silently; read the log line |
| you deleted the SQLite file | not a disaster | `lymphctl rebuild` |

---

## 10. Reference

### 10.1 Feedback types — the closed vocabulary (13)

```text
UNKNOWN              no rule matched at all
AMBIGUOUS            several rules matched, or a role is unclear
CONFLICT             two mutually exclusive outcomes both matched
LOW_CONFIDENCE       matched, but below the bar the application requires
OUT_OF_CONTRACT      the input is outside what the rules cover
QUALITY_FAILURE      the output was wrong in a way a rule could fix
REGRESSION           it used to work and stopped
SCHEMA_DRIFT         a field or format moved underneath the application
PERFORMANCE_DRIFT    it got slower
HUMAN_CORRECTION     a person overruled it — the most valuable signal
RUNTIME_FAILURE      it crashed
CONFIG_DRIFT         the live config is not the config Lymph believes it is
DEPLOYMENT_FAILURE   an applied change did not take
```

The list also comes from the daemon, which is the authority on what it accepts:

```bash
curl -s --unix-socket /run/lymph/lymph.sock http://lymph/v1/feedback-types
```

### 10.2 Event fields

| field | required | meaning |
| --- | --- | --- |
| `specversion` | yes | `1.0` |
| `id` | yes | UUID; the dedup key, generated before the send |
| `source` | yes | `lymph://<application-id>/<junction>` |
| `type` | yes | `lymph.feedback.<lowercase-type>.v1` |
| `time` | yes | when the application saw it, not when Lymph received it |
| `data.feedback_type` | yes | from the vocabulary above |
| `data.reason_code` | in practice | free text, and it is what groups |
| `data.payload` | optional | opaque to Lymph; keep it small |
| `data.input_ref` | optional | stable pointer to the evidence |
| `data.config_revision` | strongly recommended | what was in force at the time |
| header `X-Lymph-Session` | optional | transport fact, never a body field |
| header `X-Lymph-Replay` | optional | marks a spool replay |

### 10.3 Naming reason codes

Use `<SUBJECT>_<CONDITION>`, in one consistent style from day one. Two names for
the same condition are two issues forever, and merging them later means asking a
human to reconcile history. Patterns that work well:

```text
classification.*    UNKNOWN_PHRASE, ACTOR_ROLE_UNCLEAR, STATE_CONFLICT
entity.*            UNKNOWN_ENTITY_MENTION, AMBIGUOUS_ENTITY_MENTION
retrieval.*         EVIDENCE_COVERAGE_LOW, NO_EVIDENCE_FOR_CLAIM
parser.*            DECLARED_FIELD_NEVER_POPULATED, SUFFIX_MAPS_TO_DIFFERENT_EXCHANGE
schema.*            COLUMN_MISSING, TYPE_CHANGED
release.*           GATE_FAILED, COVERAGE_GAP
human.*             HUMAN_CORRECTION_RECORDED, DESK_OVERRIDE
```

### 10.4 Protocol, in one table

| method | path | who |
| --- | --- | --- |
| `POST` | `/v1/hello` | any client, to obtain a session |
| `POST` | `/v1/events` | applications (emit feedback) |
| `GET` | `/v1/events`, `/v1/events/{id}` | anyone local |
| `GET` | `/v1/issues`, `/v1/issues/{id}` | anyone local |
| `GET` | `/v1/status`, `/v1/health`, `/v1/version`, `/v1/whoami` | anyone local |
| `POST` | `/v1/applications` | registration |
| `POST` | `/v1/baselines`, `/v1/revisions`, `/v1/candidates` | operators and workers |
| `POST` | `/v1/candidates/{id}/validations` | workers |
| `POST` | `/v1/candidates/{id}/approve` | **operators only** |
| `POST` | `/v1/candidates/{id}/promote` | operators |
| `GET` | `/v1/updates`, `/v1/updates/{id}/content` | applications, with a session |
| `POST` | `/v1/updates/{id}/applied`, `/rejected` | applications, with a session |
| `POST` | `/v1/updates/{id}/withdraw` | **operators only** |
| `GET` | `/v1/application-results`, `/v1/update-dispositions` | operators |

Transport: HTTP+JSON over a Unix domain socket. `curl --unix-socket
/run/lymph/lymph.sock http://lymph/v1/status` is a complete client for reads.

### 10.5 Where to read more

| you want | read |
| --- | --- |
| to integrate quickly | [`INTEGRATING_APPLICATIONS.md`](INTEGRATING_APPLICATIONS.md) |
| event and update behavior | [`INTEGRATING_APPLICATIONS.md`](INTEGRATING_APPLICATIONS.md) |
| client APIs | [`../pkg/client`](../pkg/client) and [`../examples/python`](../examples/python) |
| roles and the ACL boundary | this guide, §7 |
| what is implemented and what is not | [`IMPLEMENTATION-STATUS.md`](IMPLEMENTATION-STATUS.md) |
| the current implementation status | [`IMPLEMENTATION-STATUS.md`](IMPLEMENTATION-STATUS.md) |

### 10.6 Verified, and not

```text
VERIFIED BY THE PUBLIC SYNTHETIC TEST SUITE
  the event contract, dedup, sessions, the spool and its drain
  the human gate and the authorization model
  the rebuild guarantee

NOT VERIFIED
  integration with any private or production corpus
  any code you write for your own application — the snippets in §4 to §6 are
  written against the public client API but have not been run against your system
  any production path of yours: nothing here has touched a live system
```

That second box is why §1.1 says to start at Level 1 on something non-critical,
watch it for a week, and only then let anything write.
