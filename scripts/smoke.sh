#!/usr/bin/env bash
# End-to-end smoke test: a real lymphd, a real Unix socket, and lymphctl
# driving the whole loop from production feedback to a promoted revision.
#
#   ./scripts/smoke.sh
#
# Everything happens inside a temporary directory. No managed deployment target
# is ever touched: deployment is L3 and is deliberately not implemented here.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -z "${GOROOT:-}" && -d "$HOME/go/bin" ]]; then
  export GOROOT="$HOME/go"
  export PATH="$HOME/go/bin:$PATH"
fi
command -v go >/dev/null 2>&1 || { echo "missing dependency: go" >&2; exit 1; }
if [[ -z "${GOPATH:-}" ]]; then
  if [[ -n "${GOROOT:-}" && -d "$GOROOT/pkg/mod" ]]; then
    export GOPATH="$GOROOT"
  else
    export GOPATH="$HOME/go"
  fi
fi
export GOFLAGS="${GOFLAGS:--mod=mod}"
export GOSUMDB="${GOSUMDB:-sum.golang.org}"
export GOPROXY="${GOPROXY:-https://proxy.golang.org,direct}"

step() { printf '\n== %s ==\n' "$*"; }

step "building"
mkdir -p bin
go build -o bin/lymphd ./cmd/lymphd
go build -o bin/lymphctl ./cmd/lymphctl

work="$(mktemp -d)"
root="$work/lymph"
socket="$root/lymph.sock"
log="$work/lymphd.log"

cleanup() {
  if [[ -n "${daemon_pid:-}" ]] && kill -0 "$daemon_pid" 2>/dev/null; then
    kill "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

export LYMPH_ROOT="$root"
export LYMPH_SOCKET="$socket"

step "1. init: instance identity is created once and never regenerated"
mkdir -p "$root"
./bin/lymphctl init

step "2. start lymphd"
./bin/lymphd --root "$root" --socket "$socket" --log-level warn >"$log" 2>&1 &
daemon_pid=$!
for _ in $(seq 1 100); do
  if ./bin/lymphctl health >/dev/null 2>&1; then break; fi
  sleep 0.05
done
./bin/lymphctl health >/dev/null || { echo "daemon never came up; log follows"; cat "$log"; exit 1; }

step "3. register the application"
./bin/lymphctl --json register-app --file examples/semantic-service.manifest.json >"$work/reg.json"
app_id="$(sed -n 's/.*"application_id": "\([^"]*\)".*/\1/p' "$work/reg.json" | head -1)"
echo "application_id: $app_id"

step "4. production hits something unexpected, three times"
./bin/lymphctl emit --app "$app_id" --junction semantic.event_state \
  --type UNKNOWN --reason UNKNOWN_EVENT_PHRASE \
  --payload '{"text":"Acme business was booked at 512.00"}' --producer producer-1 >/dev/null
./bin/lymphctl emit --app "$app_id" --junction semantic.event_state \
  --type UNKNOWN --reason UNKNOWN_EVENT_PHRASE \
  --payload '{"text":"Beta business was booked at 918.45"}' --producer producer-2 >/dev/null
./bin/lymphctl emit --app "$app_id" --junction semantic.event_state \
  --type UNKNOWN --reason UNKNOWN_EVENT_PHRASE \
  --payload '{"text":"Gamma business was booked at 44.10"}' --producer producer-1 >/dev/null

step "5. three events, one recurring issue"
./bin/lymphctl issues
issue_count="$(./bin/lymphctl --json issues | grep -c '"issue_id"' || true)"
if [[ "$issue_count" != "1" ]]; then
  echo "expected exactly 1 issue, got $issue_count" >&2
  exit 1
fi
issue_id="$(./bin/lymphctl --json issues | sed -n 's/.*"issue_id": "\([^"]*\)".*/\1/p' | head -1)"

step "6. bootstrap the configuration that is already on disk (r1)"
./bin/lymphctl baseline --app "$app_id" --family semantic_event_rules \
  --file examples/semantic/events.yaml --author operator --label 2.3.1 >/dev/null
r1="$(./bin/lymphctl --json refs --app "$app_id" --family semantic_event_rules \
  | sed -n 's/.*"revision_id": "\([^"]*\)".*/\1/p' | head -1)"
echo "active revision: $r1"

step "7. queue the issue for an external workflow and lease it"
work_id="$(./bin/lymphctl --json queue --issues "$issue_id" \
  | sed -n 's/.*"work_id": "\([^"]*\)".*/\1/p' | head -1)"
echo "work item: $work_id"
attempt="$(./bin/lymphctl --json claim --worker codex-maintenance --workflow semantic-rule-repair --ttl 60 \
  | sed -n 's/.*"attempts": \([0-9]*\).*/\1/p' | head -1)"

step "8. the workflow returns a candidate, never a production write"
candidate_id="$(./bin/lymphctl --json candidate --app "$app_id" --family semantic_event_rules \
  --dir examples/semantic-candidate --base "$r1" --work-item "$work_id" \
  --explanation "add the booked-phrase rule observed three times in production" \
  | sed -n 's/.*"candidate_id": "\([^"]*\)".*/\1/p' | head -1)"
echo "candidate: $candidate_id"

step "9. validate, approve, promote"
./bin/lymphctl validate --candidate "$candidate_id" --kind structural --result PASS \
  --suite semantic-service.semantic-rules.validate.v2 >/dev/null
./bin/lymphctl validate --candidate "$candidate_id" --kind replay --result PASS \
  --suite semantic-service.semantic-rules.replay --worker operator >/dev/null
./bin/lymphctl validate --candidate "$candidate_id" --kind regression --result PASS \
  --suite semantic-service.semantic-rules.regression --worker operator >/dev/null
./bin/lymphctl approve --candidate "$candidate_id" --approver operator \
  --comment "no precision loss on the historical replay" >/dev/null
./bin/lymphctl complete "$work_id" --worker codex-maintenance --state RETURNED \
  --result "{\"candidate_id\":\"$candidate_id\"}" >/dev/null
./bin/lymphctl promote --candidate "$candidate_id" --actor operator

step "10. refs and reflog: what changed, and why"
./bin/lymphctl refs --app "$app_id" --family semantic_event_rules
echo
./bin/lymphctl reflog --app "$app_id" --family semantic_event_rules --limit 5

step "10b. a worker returns a typed result (L2.5)"
# One more issue, routed and leased, then returned through the typed API.
./bin/lymphctl emit --app "$app_id" --junction semantic.event_state \
  --type OUT_OF_CONTRACT --reason BAD_RETRY_POLICY \
  --payload '{"note":"the desk wants a different retry policy"}' --producer producer-3 >/dev/null
typed_issue="$(./bin/lymphctl --json issues --order recent | sed -n 's/.*"issue_id": "\([^"]*\)".*/\1/p' | head -1)"
typed_work="$(./bin/lymphctl --json queue --issues "$typed_issue" \
  | sed -n 's/.*"work_id": "\([^"]*\)".*/\1/p' | head -1)"
./bin/lymphctl claim --worker codex-maintenance --workflow semantic-rule-repair --ttl 60 >/dev/null

# A worker says what it produced: here a code reference and a NO_CHANGE note,
# neither of which is a configuration.
cat > "$work/result.json" <<'JSON'
{
  "artifacts": [
    {
      "kind": "CODE_REF",
      "code_ref": {
        "repository": "git@example/semantic-service",
        "commit": "9f2c1ab77c0d",
        "branch": "main",
        "description": "fix the retry storm in the fetch loop"
      }
    },
    {
      "kind": "NO_CHANGE",
      "no_change": {
        "reason": "the semantic rules already classify this correctly",
        "disposition": "CURRENT_CONFIG_CORRECT"
      }
    }
  ]
}
JSON
./bin/lymphctl return-result "$typed_work" --worker codex-maintenance --attempt "$attempt" --file "$work/result.json"
echo
echo "-- what workers produced --"
./bin/lymphctl artifacts --limit 5
echo
echo "-- the return itself --"
./bin/lymphctl results --limit 3
echo
echo "-- installations --"
./bin/lymphctl installations --application "$app_id"

step "11. walk the issue to FIXED"
echo "-- status after queueing --"
./bin/lymphctl --json issues | sed -n 's/.*"status": "\([^"]*\)".*/\1/p' | head -1
for status in IN_PROGRESS CANDIDATE_PRODUCED VALIDATING FIXED; do
  ./bin/lymphctl issue-status "$issue_id" --status "$status" --actor operator >/dev/null
done
./bin/lymphctl issues --status FIXED

step "12. ledger integrity, then rebuild the projection from canonical truth"
./bin/lymphctl ledger verify
./bin/lymphctl rebuild --yes
echo
echo "-- after rebuild --"
./bin/lymphctl status

step "13. the store explains itself"
./bin/lymphctl events --limit 5
echo
./bin/lymphctl audit --limit 5

step "14. a duplicate send is a no-op (idempotent at-least-once delivery)"
first_event_id="$(./bin/lymphctl --json events --limit 1 | sed -n 's/.*"event_id": "\([^"]*\)".*/\1/p' | head -1)"
events_before="$(./bin/lymphctl --json status | sed -n 's/.*"events": \([0-9]*\).*/\1/p' | head -1)"
./bin/lymphctl emit --app "$app_id" --junction semantic.event_state \
  --type UNKNOWN --reason UNKNOWN_EVENT_PHRASE --id "$first_event_id" \
  --payload '{"text":"Acme business was booked at 512.00"}' --producer producer-1
events_after="$(./bin/lymphctl --json status | sed -n 's/.*"events": \([0-9]*\).*/\1/p' | head -1)"
echo "events before/after the duplicate send: $events_before / $events_after"
if [[ "$events_after" != "$events_before" ]]; then
  echo "expected the duplicate to be ignored, count moved from $events_before to $events_after" >&2
  exit 1
fi

printf '\nSMOKE OK\n'
