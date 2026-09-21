# Lymph — build and verification targets.
#
# Go always consults its local module cache before the configured proxy. Keep
# the default safe for clean CI hosts; use GOPROXY=off for an explicitly
# offline build after dependencies have been cached.

SHELL := /bin/bash

GO      ?= go
GOROOT  ?= $(shell $(GO) env GOROOT 2>/dev/null)
GOPATH  ?= $(shell $(GO) env GOPATH 2>/dev/null)
GOFLAGS ?= -mod=mod
GOSUMDB ?= sum.golang.org
GOPROXY ?= https://proxy.golang.org,direct
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || printf dev)
COMMIT  ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null || printf unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS ?= -X github.com/ycooi/Lymph/internal/buildinfo.Version=$(VERSION) -X github.com/ycooi/Lymph/internal/buildinfo.Commit=$(COMMIT) -X github.com/ycooi/Lymph/internal/buildinfo.Date=$(BUILD_DATE)
TEST_TMPDIR ?= /tmp
GOVULNCHECK_VERSION ?= v1.8.0

export GOROOT GOPATH GOFLAGS GOSUMDB GOPROXY

.PHONY: all build test vet vuln fmt check-format smoke py-smoke lab lab-race lab-fuzz lab-full verify-release clean

all: build

build:
	@mkdir -p bin
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/lymphd ./cmd/lymphd
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/lymphctl ./cmd/lymphctl
	@echo "built bin/lymphd and bin/lymphctl"

test:
	TMPDIR="$(TEST_TMPDIR)" $(GO) test -timeout 180s ./...

vet:
	$(GO) vet ./...

vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

fmt:
	gofmt -l -w ./cmd ./internal ./pkg ./lab

check-format:
	@files="$$(gofmt -l ./cmd ./internal ./pkg ./lab)"; \
	if [[ -n "$$files" ]]; then printf 'files need gofmt:\n%s\n' "$$files" >&2; exit 1; fi

smoke: build
	TMPDIR="$(TEST_TMPDIR)" ./scripts/smoke.sh

py-smoke: build
	TMPDIR="$(TEST_TMPDIR)" ./scripts/python-smoke.sh

lab:
	TMPDIR="$(TEST_TMPDIR)" ./scripts/lab.sh

lab-race:
	TMPDIR="$(TEST_TMPDIR)" ./scripts/lab.sh race

lab-fuzz:
	TMPDIR="$(TEST_TMPDIR)" ./scripts/lab.sh fuzz

lab-full:
	TMPDIR="$(TEST_TMPDIR)" ./scripts/lab.sh full

verify-release:
	TMPDIR="$(TEST_TMPDIR)" ./scripts/verify-release.sh

clean:
	rm -rf bin
