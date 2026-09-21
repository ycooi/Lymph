# Integrating an application with Lymph

Three steps. Nothing in a normal request path.

> For the full version — concepts, the three levels of integration, operating
> Lymph, and how to retrieve the accumulated feedback later for development — see
> [USER-GUIDE.md](USER-GUIDE.md).

There are two levels of integration, and they are additive:

```text
feedback-only      the application tells Lymph where its configuration failed
update-capable     the application also takes approved revisions back, on its terms
```

Every application should do the first. Only an application whose configuration
is registered as `LYMPH_MANAGED` can do the second, and only after a human has
approved the exact content.

## 1. Register once

Registration is an explicit control-plane operation. Lymph never auto-registers,
and nothing about a running process can create identity.

```bash
lymphctl register-app --file myapp.manifest.json
```

```json
{
  "name": "myapp",
  "type": "service",
  "owner": "ops",
  "installations": [
    { "name": "region-a", "environment": "production" },
    { "name": "region-b",  "environment": "production" }
  ],
  "installation_id": "<the region-a uuid>",
  "junctions": [
    {
      "name": "semantic.event_state",
      "improvement_workflow": "semantic-rule-repair",
      "config_family": "semantic_event_rules",
      "feedback_types": ["UNKNOWN", "AMBIGUOUS", "CONFLICT"]
    }
  ],
  "config_families": [
    {
      "name": "semantic_event_rules",
      "schema_revision": "semantic-rules-v3",
      "targets": [
        { "installation_id": "<the region-a uuid>", "target_type": "SINGLE_FILE",
          "path": "/etc/myapp/semantic/events.yaml", "reload_policy": "SIGHUP" }
      ]
    }
  ]
}
```

The **junctions** are the interesting part. Each one is a place where the
application may discover that its own configuration is inadequate, and each one
declares which workflow repairs it. Lymph never learns what UNKNOWN means; it
learns where to send it.

## 2. Give the application its identity

```bash
lymphctl identity export --application myapp --installation region-a --out /etc/myapp/lymph.json
```

```json
{ "application_id": "019a8a51-...", "installation_id": "019a8a52-..." }
```

Readable by the application's user only. It is not a secret — it is the pair of
UUIDs the daemon already knows — but it should not be world-readable either.

## 3. Emit feedback at failure boundaries

Not for every request. Only where the application discovers that its *own
configuration* was inadequate for a real input.

### MCP-style text classifier

```python
lymph = LymphClient.from_identity_file("/etc/myapp/lymph.json", spool_dir="/var/lib/myapp/lymph-spool")

result = classify(text)
if result.unknown:
    lymph.emit_feedback(
        junction="semantic.event_state",
        feedback_type="UNKNOWN",
        reason_code="NO_RULE_MATCH",
        payload={"text": text, "evidence_id": evidence_id},
        config_revision=current_config_revision(),
    )
```

Normal successful classification sends nothing.

### Agent

```python
intent = route(request)
if intent is None:
    lymph.emit_feedback(
        junction="agent.tool_policy",
        feedback_type="OUT_OF_CONTRACT",
        reason_code="UNKNOWN_INTENT",
        payload={"request": request},
    )
    return fallback_response()   # the agent still answers
```

### ML service

```python
if batch.confidence_mean < threshold or audit.accuracy < expected:
    lymph.emit_feedback(
        junction="model.serving",
        feedback_type="PERFORMANCE_DRIFT",
        reason_code="DISTRIBUTION_DRIFT",
        payload={"confidence_mean": batch.confidence_mean, "window": window},
        config_revision=current_config_revision(),
    )
```

No inference passes through Lymph.

### Ingestion / ETL

```python
try:
    parse(row)
except SchemaDrift as drift:
    lymph.emit_feedback(
        junction="ingest.schema",
        feedback_type="SCHEMA_DRIFT",
        reason_code="NEW_SOURCE_TABLE_LAYOUT",
        payload={"columns": drift.columns},
        input_ref=drift.input_ref,
    )
    quarantine(row)   # keep the pipeline moving
```

### Ordinary daemon

```python
if upstream.p95_ms > settings.timeout_ms * 0.8:
    lymph.emit_feedback(
        junction="service.runtime",
        feedback_type="PERFORMANCE_DRIFT",
        reason_code="UPSTREAM_LATENCY",
        payload={"p95_ms": upstream.p95_ms},
    )
```

## Startup patterns

Either is fine; both are supported and neither blocks for seconds.

```python
# eager: find out early, log once, carry on regardless
try:
    lymph.hello()
    lymph.flush_spool()      # deliver anything left from the last outage
except Exception as exc:
    log.warning("lymph unavailable at startup: %s", exc)

# lazy: do nothing until the first failure
lymph = LymphClient.from_identity_file("/etc/myapp/lymph.json", spool_dir="...")
```

## Anti-patterns

Do **not**:

```text
call Lymph on every normal request
make a runtime answer depend on Lymph being up
query Lymph for semantic rules on every request
store business data in Lymph
send ordinary debug logs to Lymph
block application startup waiting indefinitely for Lymph
```

Lymph is adaptation infrastructure — the place a system notices that its
configuration no longer fits reality. It is not runtime middleware, and nothing
about serving a request should pass through it.

## Operational checks

```bash
lymphctl status                      # applications, issues, sessions, sessionsless events
lymphctl sessions --state ACTIVE     # who is connected right now
lymphctl hello --application myapp   # the handshake, performed by hand
lymphctl issues --limit 20           # what production keeps hitting
lymphctl artifacts --kind MODEL_REF  # what workers have produced
lymphctl whoami                      # which local roles this connection holds
```

If an application cannot connect, `lymphctl hello --application myapp
--installation region-a` produces exactly the reason code the SDK would surface:
`UNKNOWN_APPLICATION`, `UNKNOWN_INSTALLATION`,
`INSTALLATION_APPLICATION_MISMATCH`, `PROTOCOL_INCOMPATIBLE`.

## Retrieving what accumulated

Feedback is for reading later, not just for counting now:

```bash
python3 scripts/export-feedback.py --application myservice \
  --out ./myservice-feedback --since 2026-09-01
```

It writes `summary.json` (counts and histograms), `issues.ndjson`, `events.ndjson`
and `occurrences.ndjson`, read-only, with the daemon running or not. The full
chapter — including how grouping decisions change what you get — is in
[USER-GUIDE.md §8](USER-GUIDE.md).

## Update-capable integration

This section is the difference between "Lymph remembers" and "Lymph improves".
An update-capable application does five things, and the fourth one is the whole
point.

### 1. Mark exactly one config family as managed

```json
{
  "name": "coverage_policy",
  "schema_revision": "coverage-v1",
  "management_mode": "LYMPH_MANAGED",
  "targets": [
    { "target_type": "SINGLE_FILE", "path": "config/coverage_policy.yaml",
      "reload_policy": "RESTART", "atomicity": "FULL" }
  ]
}
```

`management_mode` defaults to `EXTERNAL`, and an `EXTERNAL` family is never
delivered — not after approval, not after promotion, not ever. Secrets and
credentials belong in families you do not mark managed. Lymph has no secret
detection; the boundary is the declaration, and that is deliberate.

### 2. Ask, on your own schedule

```go
node, err := client.NewApplicationClient(client.ApplicationOptions{
    Socket: "/run/lymph/lymph.sock",
    ApplicationID: appID, InstallationID: installationID,
})

update, err := node.CheckUpdate(ctx, "coverage_policy", currentRevision)
switch {
case err == nil && update == nil:
    // Nothing to do. This is the normal answer; stay quiet.
case errors.Is(err, client.ErrBaseRevisionMismatch):
    // Something is approved, but it was built on a revision you are not on.
    // `update` still describes what was offered, so the operator can see why.
case err != nil:
    // Lymph is unreachable or refused. Keep running your current revision.
}
```

### 3. Fetch and verify

```go
content, err := node.FetchUpdate(ctx, update.ConfigFamilyID, update.RevisionID)
if errors.Is(err, client.ErrIntegrity) {
    // The bytes do not hash to what was approved. Do not apply them.
}
```

`FetchUpdate` re-hashes every file and the tree itself. It never asks the daemon
whether the content is correct.

### 4. Validate, stage, apply, check — yourself

```go
if problem := validateLocally(content.Bundle); problem != "" {
    node.ReportRejected(ctx, update.RevisionID, client.RejectedReport{
        ConfigFamilyID: update.ConfigFamilyID,
        ReasonCode:     "LOCAL_VALIDATION_FAILED",
        Message:        problem,
    })
    return   // your current revision is untouched
}
stageAndRename(content.Bundle)   // then reload at your safe point
```

Refusing is a first-class outcome, not a failure. A refusal becomes an issue in
Lymph, linked to the problem the revision claimed to fix, so the next worker
sees it.

There is no `ApplyUpdate` in the SDK, in any language. Only your application
knows its own format, its safe reload point and whether a restart is required.

### 5. Report the outcome

```go
node.ReportApplied(ctx, update.RevisionID, client.AppliedReport{
    ConfigFamilyID:     update.ConfigFamilyID,
    ObservedHash:       content.RootTreeHash,
    PreviousRevisionID: currentRevision,
    Details:            map[string]any{"reload": "SIGHUP", "took_ms": 12},
})
```

This is the only thing that moves `observed` and `last_good`. It is idempotent:
reporting the same outcome twice produces one canonical result. A reported hash
that does not match the revision is refused with `INTEGRITY_ERROR` — the daemon
will not record a success that is not true.

### Human approval is not optional

```text
no approval        → nothing is offered, and a fetch is refused
machine approval   → recorded as SYSTEM, and still not deliverable
human approval     → deliverable, for one installation and one exact hash
```

Approval happens through `lymphctl approve`, by an operator principal, and it
binds to the content hash. A test that passes is evidence for a human, never a
substitute for one.

### Full reference

* [`USER-GUIDE.md`](USER-GUIDE.md) — operator workflow, ACLs, exports, and
  troubleshooting.
* [`IMPLEMENTATION-STATUS.md`](IMPLEMENTATION-STATUS.md) — implemented scope and
  explicit limitations.
* [`pkg/client`](../pkg/client) and [`examples/python`](../examples/python) — the
  public Go and Python client implementations.
