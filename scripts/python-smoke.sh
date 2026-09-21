#!/usr/bin/env bash
# Verifies the Python client path end to end: start lymphd, register an
# application, emit from Python, confirm the event was grouped, then prove the
# offline spool by killing the daemon.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -z "${GOROOT:-}" && -d "$HOME/go/bin" ]]; then
  export GOROOT="$HOME/go"
  export PATH="$HOME/go/bin:$PATH"
fi
if [[ -z "${GOPATH:-}" && -n "${GOROOT:-}" && -d "$GOROOT/pkg/mod" ]]; then
  export GOPATH="$GOROOT"
fi
export GOFLAGS="${GOFLAGS:--mod=mod}"
export GOSUMDB="${GOSUMDB:-sum.golang.org}"
export GOPROXY="${GOPROXY:-https://proxy.golang.org,direct}"

command -v python3 >/dev/null 2>&1 || { echo "missing dependency: python3" >&2; exit 1; }

mkdir -p bin
go build -o bin/lymphd ./cmd/lymphd
go build -o bin/lymphctl ./cmd/lymphctl

work="$(mktemp -d)"
root="$work/lymph"
socket="$root/lymph.sock"
spool="$work/spool"

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

mkdir -p "$root"
./bin/lymphd --root "$root" --socket "$socket" --log-level warn >"$work/lymphd.log" 2>&1 &
daemon_pid=$!
for _ in $(seq 1 100); do
  ./bin/lymphctl health >/dev/null 2>&1 && break
  sleep 0.05
done
./bin/lymphctl health >/dev/null || { cat "$work/lymphd.log"; exit 1; }

./bin/lymphctl --json register-app --file examples/semantic-service.manifest.json >"$work/reg.json"
app_id="$(sed -n 's/.*"application_id": "\([^"]*\)".*/\1/p' "$work/reg.json" | head -1)"
echo "registered application: $app_id"

python3 examples/python/lymph_client.py --socket "$socket" emit \
  --app "$app_id" --junction semantic.event_state \
  --type UNKNOWN --reason UNKNOWN_EVENT_PHRASE \
  --payload '{"text":"Acme business was booked at 512.00"}'

echo "-- python client sees the issue --"
python3 - "$socket" <<'PY'
import sys
sys.path.insert(0, "examples/python")
from lymph_client import LymphClient

client = LymphClient(socket_path=sys.argv[1])
issues = client.issues()
assert len(issues) == 1, issues
assert issues[0]["occurrence_count"] == 1, issues
print(f"{issues[0]['issue_id']}  {issues[0]['status']}  {issues[0]['pattern']}")
PY

echo "-- python worker returns a typed improvement (L2.5) --"
python3 - "$socket" "$app_id" <<'PY'
import sys
sys.path.insert(0, "examples/python")
from lymph_client import LymphClient, config_bundle, model_ref, no_change

client = LymphClient(socket_path=sys.argv[1], producer_instance="python-worker")
application = sys.argv[2]

# Bootstrap the configuration that is already on disk, so a candidate has a base.
# The family comes from the junction, which is how a worker knows where its
# feedback lands.
junctions = client._request("GET", f"/v1/junctions?application={application}")["junctions"]
junction = next(j for j in junctions if j["name"] == "semantic.event_state")
client._request("POST", "/v1/baselines", {
    "application": application,
    "config_family": junction["config_family_id"],
    # config_bundle() base64-encodes the exact bytes, which is what the wire
    # (and the hash) needs.
    "bundle": config_bundle({"rules.yaml": "version: 1\nrules:\n  - id: a\n    phrase: awarded\n    state: AWARDED\n"})["config_bundle"]["bundle"],
    "author": "python-smoke",
})

# Queue the issue and lease it, as an external worker would.
issues = client.issues()
work = client._request("POST", "/v1/work-items", {"issue_ids": [issues[0]["issue_id"]]})
claimed = client._request("POST", "/v1/work-items/claim",
                          {"worker": "python-maintainer", "ttl_seconds": 60})
assert claimed["work_id"] == work["work_id"], (claimed, work)

# One return, two typed artifacts: a model reference and the config that uses it.
returned = client.return_improvement(
    claimed["work_id"],
    worker="python-maintainer",
    attempt=claimed["attempts"],
    summary="point serving at model-v2",
    artifacts=[
        model_ref("s3://models/classifier/model-v2.bin", "sha256:" + "ab" * 32),
        config_bundle({"config.yaml": "active_model: model-v2\nthreshold: 0.35\n"},
                      explanation="serving config for the new model"),
    ],
)
kinds = sorted(a["kind"] for a in returned["artifacts"])
assert kinds == ["CONFIG_BUNDLE", "MODEL_REF"], kinds
assert len(returned["candidates"]) == 1, returned
print(f"result {returned['result']['result_id']}  artifacts {kinds}  candidate {returned['candidates'][0]['candidate_id']}")

# A non-config conclusion is recorded just as first-class.
issues2 = client.issues(status="QUEUED_FOR_IMPROVEMENT")
if issues2:
    work2 = client._request("POST", "/v1/work-items", {"issue_ids": [issues2[0]["issue_id"]]})
    claimed2 = client._request("POST", "/v1/work-items/claim",
                               {"worker": "python-maintainer", "ttl_seconds": 60})
    if claimed2:
        second = client.return_improvement(
            claimed2["work_id"], worker="python-maintainer", attempt=claimed2["attempts"],
            artifacts=[no_change("upstream malformed record; current parser is correct",
                                 "EXTERNAL_CAUSE")],
        )
        assert second["artifacts"][0]["kind"] == "NO_CHANGE", second
        assert not second["candidates"], second
        print("no-change result recorded with 0 candidates")
PY

echo "-- read APIs --"
python3 - "$socket" "$app_id" <<'PY'
import sys
sys.path.insert(0, "examples/python")
from lymph_client import LymphClient

client = LymphClient(socket_path=sys.argv[1])
results = client.improvement_results(application=sys.argv[2])
artifacts = client.artifacts(application=sys.argv[2])
installations = client.installations(application=sys.argv[2])
assert results, "no improvement results"
assert len(artifacts) >= 2, artifacts
assert installations, "no installations"
print(f"results {len(results)}  artifacts {len(artifacts)}  installations {len(installations)}")
PY

echo "-- stop the daemon, application keeps running --"
kill "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
daemon_pid=""

python3 - "$socket" "$spool" <<'PY'
import sys
sys.path.insert(0, "examples/python")
from lymph_client import LymphClient, feedback

client = LymphClient(socket_path=sys.argv[1], spool_dir=sys.argv[2], producer_instance="p1")
result = client.emit(
    application="019a0000-0000-7000-8000-000000000001",
    junction="semantic.event_state",
    feedback_type=feedback.UNKNOWN,
    reason_code="UNKNOWN_EVENT_PHRASE",
    payload={"text": "Delta business was booked at 11.00"},
)
assert result.spooled, result
assert client.spool.count() == 1, client.spool.count()
print(f"spooled {result.event_id} while lymph was down")
PY

echo "PYTHON SMOKE OK"
