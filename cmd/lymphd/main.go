// Command lymphd is the Lymph daemon: the only writer of the lymph store
// (section 13) and the only process that speaks to the canonical ledger.
//
// It is small on purpose. It does not reason about configuration, does not
// embed a workflow engine, and does not run plugins (section 40, section 93).
// It remembers, versions, routes and audits.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ycooi/Lymph/internal/buildinfo"
	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/httpapi"
)

func main() {
	requireACLDefault, err := envBool("LYMPH_REQUIRE_ACL", false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var (
		root       = flag.String("root", envOr("LYMPH_ROOT", engine.DefaultRoot), "lymph store directory")
		socket     = flag.String("socket", envOr("LYMPH_SOCKET", ""), "unix socket path (default <root>/lymph.sock)")
		socketMode = flag.String("socket-mode", envOr("LYMPH_SOCKET_MODE", "0660"),
			"socket permission bits: 0660 for a service group, 0600 for a user-local install")
		logLevel = flag.String("log-level", envOr("LYMPH_LOG_LEVEL", "info"), "debug, info, warn or error")
		aclPath  = flag.String("acl", envOr("LYMPH_ACL", ""),
			"authorization policy file (default <root>/acl.json when it exists); without one the daemon trusts the socket permissions alone")
		requireACL = flag.Bool("require-acl", requireACLDefault,
			"refuse startup unless an authorization policy file is configured")
		showVer   = flag.Bool("version", false, "print version and exit")
		checkOnly = flag.Bool("check", false, "verify the store and exit without serving")
	)
	flag.Parse()

	if *showVer {
		fmt.Printf("lymphd %s (api v1, format lymph-1, built %s)\n", buildinfo.String(), buildinfo.Date)
		return
	}

	log := newLogger(*logLevel)

	mode, err := parseSocketMode(*socketMode)
	if err != nil {
		log.Error("invalid --socket-mode", "value", *socketMode, "error", err)
		os.Exit(2)
	}

	opts := engine.Options{Root: *root, Logger: log, ServerVersion: buildinfo.Version}
	e, err := engine.Open(opts)
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			log.Error("cannot open lymph store: permission denied", "root", *root)
			fmt.Fprintln(os.Stderr, "hint: the daemon needs write access to the store; run it as the service user or pass --root")
		} else {
			log.Error("startup failed", "error", err)
		}
		os.Exit(1)
	}
	defer e.Close()

	report, err := e.VerifyLedger()
	if err != nil {
		log.Error("ledger integrity failure", "error", err)
		os.Exit(2)
	}
	log.Info("lymph ready",
		"instance", e.Identity().InstanceUUID,
		"root", e.Root(),
		"records", report.Records,
		"head", report.HeadHash)

	if *checkOnly {
		status, err := e.Status(context.Background())
		if err != nil {
			log.Error("status failed", "error", err)
			os.Exit(1)
		}
		fmt.Printf("applications=%d events=%d ledger_records=%d\n",
			status.Applications, status.Events, status.LedgerRecords)
		return
	}

	sockPath := *socket
	if sockPath == "" {
		sockPath = filepath.Join(e.Root(), "lymph.sock")
	}

	// Local authorization (mission sections 22, 23). With a policy file the
	// daemon knows which local uid may approve; without one it can only rely on
	// the socket permissions, and it says so out loud rather than implying a
	// gate it does not have.
	resolvedACL := *aclPath
	if resolvedACL == "" {
		candidate := filepath.Join(e.Root(), "acl.json")
		if _, err := os.Stat(candidate); err == nil {
			resolvedACL = candidate
		}
	}
	acl, err := httpapi.LoadACL(resolvedACL)
	if err != nil {
		log.Error("cannot load authorization policy", "error", err)
		os.Exit(2)
	}
	if acl == nil {
		if *requireACL {
			log.Error("authorization policy is required but none was found", "root", e.Root())
			os.Exit(2)
		}
		log.Warn("no authorization policy; any local user allowed by the socket permissions may approve config changes")
	} else {
		log.Info("authorization policy loaded", "path", acl.Path(), "principals", len(acl.Principals))
	}

	srv := httpapi.New(httpapi.Options{Engine: e, SocketPath: sockPath, Logger: log, SocketMode: mode, ACL: acl})

	// Open the socket before installing signal handling, so a SIGTERM cannot
	// arrive while the listener is still being created.
	if err := srv.Start(); err != nil {
		log.Error("cannot open the control socket", "socket", sockPath, "error", err)
		os.Exit(1)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Info("shutting down", "signal", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Warn("shutdown error", "error", err)
		}
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("server stopped", "error", err)
			os.Exit(1)
		}
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("invalid %s value %q: expected true or false", key, raw)
	}
	return value, nil
}

// parseSocketMode reads permission bits written the way an operator writes
// them: 0660, 600 or 0o600. Lymph never widens the socket to world-accessible,
// and an unparsable value is a startup error rather than a silent default.
func parseSocketMode(raw string) (os.FileMode, error) {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.TrimPrefix(trimmed, "0o")
	trimmed = strings.TrimPrefix(trimmed, "0O")
	if trimmed == "" {
		return 0, fmt.Errorf("empty socket mode")
	}
	value, err := strconv.ParseUint(trimmed, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("expected octal permission bits such as 0660: %w", err)
	}
	mode := os.FileMode(value)
	if mode&0o007 != 0 {
		return 0, fmt.Errorf("refusing world-accessible socket mode %#o", mode)
	}
	if mode&0o600 == 0 {
		return 0, fmt.Errorf("socket mode %#o would not let the owner talk to Lymph", mode)
	}
	return mode, nil
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}
