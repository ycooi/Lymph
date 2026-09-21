#!/usr/bin/env bash
# The Lymph Lab: five unrelated synthetic applications, one daemon, plus the chaos,
# fuzz, soak and acceptance gates from the test plan.
#
#   ./scripts/lab.sh            run the graded lab and print the verdicts
#   ./scripts/lab.sh race       the concurrency and chaos tests under -race
#   ./scripts/lab.sh fuzz       time-boxed Go fuzzing of the parsers and state machines
#   ./scripts/lab.sh full       all of the above, in order
#   LYMPH_LAB_SCALE=full ./scripts/lab.sh   run at full scale (slow)
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -z "${GOROOT:-}" && -d "$HOME/go/bin" ]]; then
  export GOROOT="$HOME/go"
  export PATH="$HOME/go/bin:$PATH"
fi
command -v go >/dev/null 2>&1 || { echo "missing dependency: go" >&2; exit 1; }
if [[ -z "${GOPATH:-}" && -n "${GOROOT:-}" && -d "$GOROOT/pkg/mod" ]]; then
  export GOPATH="$GOROOT"
fi
export GOFLAGS="${GOFLAGS:--mod=mod}"
export GOSUMDB="${GOSUMDB:-sum.golang.org}"
export GOPROXY="${GOPROXY:-https://proxy.golang.org,direct}"

FUZZ_TIME="${LYMPH_LAB_FUZZ_TIME:-20s}"
step() { printf '\n== %s ==\n' "$*"; }

run_lab() {
  step "Lymph Lab (scale: ${LYMPH_LAB_SCALE:-small})"
  go test -timeout 30m -v ./lab/... 2>&1 | tee /tmp/lymph-lab.log | grep -E '^(\s+)?(--- |\[|ok|FAIL)' || true
  echo
  echo "report: lab/REPORT.md"
  sed -n '/^## Verdicts/,/^### LYMPH_001/p' lab/REPORT.md
  if grep -qE '^\| S[0-9]+_[a-z_]+ \|.*\| FAIL \|' lab/REPORT.md; then
    echo "lab reported defects" >&2
    exit 1
  fi
}

run_race() {
  step "race detector: concurrent promotion, chaos kill, soak"
  go test -race -timeout 30m -run 'TestS13ConcurrentPromotion|TestS10ChaosKill|TestS33Soak|TestS12StaleCandidate' ./lab/...
  step "race detector: unit suites"
  go test -race -timeout 20m ./internal/... ./pkg/...
}

run_fuzz() {
  step "fuzzing (${FUZZ_TIME} per target)"
  local targets=(
    "./internal/protocol:FuzzEventEnvelope"
    "./internal/ledger:FuzzLedgerRecordLine"
    "./internal/fingerprint:FuzzNormalize"
    "./internal/engine:FuzzStructuralValidation"
    "./internal/engine:FuzzStateTransitions"
    "./internal/engine:FuzzArtifactKindVocabulary"
    "./internal/engine:FuzzArtifactRequestValidation"
    "./internal/engine:FuzzInstallationNormalization"
    "./internal/engine:FuzzHelloRequest"
    "./internal/engine:FuzzSessionHeaders"
  )
  for entry in "${targets[@]}"; do
    local pkg="${entry%%:*}" fn="${entry##*:}"
    printf '\n-- %s %s --\n' "$pkg" "$fn"
    go test -run '^$' -fuzz "^${fn}$" -fuzztime "$FUZZ_TIME" "$pkg" || return 1
  done
}

case "${1:-lab}" in
  lab) run_lab ;;
  race) run_race ;;
  fuzz) run_fuzz ;;
  full)
    step "unit and integration suites"
    go test -timeout 20m ./...
    run_lab
    run_race
    run_fuzz
    ;;
  *)
    echo "usage: $0 [lab|race|fuzz|full]" >&2
    exit 2
    ;;
esac
