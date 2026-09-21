#!/usr/bin/env bash
# Release gate for the supported production boundary.
set -euo pipefail

cd "$(dirname "$0")/.."

export TMPDIR="${TMPDIR:-/tmp}"
export GOSUMDB="${GOSUMDB:-sum.golang.org}"
export GOFLAGS="${GOFLAGS:--mod=readonly}"
export LYMPH_LAB_FUZZ_TIME="${LYMPH_LAB_FUZZ_TIME:-20s}"

step() { printf '\n== %s ==\n' "$*"; }

step "format"
unformatted="$(gofmt -l ./cmd ./internal ./pkg ./lab)"
if [[ -n "$unformatted" ]]; then
  printf 'files need gofmt:\n%s\n' "$unformatted" >&2
  exit 1
fi

step "module integrity"
go mod verify

step "unit and integration tests"
go test -count=1 -timeout 20m ./...

step "static analysis"
go vet ./...

step "known vulnerabilities"
make vuln

step "build"
make build

step "end-to-end smoke"
./scripts/smoke.sh

step "Python client smoke"
./scripts/python-smoke.sh

step "race detector"
./scripts/lab.sh race

step "fuzz targets"
./scripts/lab.sh fuzz

step "release gate complete"
./bin/lymphd --version
./bin/lymphctl version
