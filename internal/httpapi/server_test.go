package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/httpapi"
	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
	"github.com/ycooi/Lymph/pkg/client"
)

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestRequestBodyLimit(t *testing.T) {
	e, err := engine.Open(engine.Options{Root: t.TempDir(), Logger: quiet()})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer e.Close()
	srv := httpapi.New(httpapi.Options{Engine: e, Logger: quiet()})
	body := `{"name":"` + strings.Repeat("x", int(httpapi.MaxRequestBodyBytes)+1) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/applications", strings.NewReader(body))
	res := httptest.NewRecorder()
	srv.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest && res.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized request returned %d: %s", res.Code, res.Body.String())
	}
	if !strings.Contains(strings.ToLower(res.Body.String()), "too large") {
		t.Fatalf("oversized request did not explain the limit: %s", res.Body.String())
	}
}

// startDaemon runs a real lymphd HTTP surface on a real Unix socket in a temp
// directory, and returns the socket path plus a stop function.
func startDaemon(t *testing.T) (root, socket string, stop func()) {
	t.Helper()
	root = t.TempDir()
	e, err := engine.Open(engine.Options{Root: root, Logger: quiet()})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	socket = filepath.Join(root, "lymph.sock")
	srv := httpapi.New(httpapi.Options{Engine: e, SocketPath: socket, Logger: quiet()})

	if err := srv.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve()
	}()
	waitForSocket(t, socket)

	return root, socket, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-done
		e.Close()
	}
}

func waitForSocket(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("unix", path, time.Second)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("socket %s never became ready", path)
}

func registerManifest(t *testing.T, cli *client.Client, name string) engine.Registration {
	t.Helper()
	var reg engine.Registration
	err := cli.Post(context.Background(), "/v1/applications", engine.Manifest{
		Name: name,
		Junctions: []engine.JunctionSpec{
			{Name: "semantic.event_state", Workflow: "semantic-rule-repair", ConfigFamily: "semantic_event_rules"},
		},
		ConfigFamilies: []engine.ConfigFamilySpec{
			{Name: "semantic_event_rules", SchemaRevision: "semantic-rules-v3"},
		},
	}, &reg)
	if err != nil {
		t.Fatalf("register over socket: %v", err)
	}
	return reg
}

func TestEmitAndQueryOverUnixSocket(t *testing.T) {
	_, socket, stop := startDaemon(t)
	defer stop()

	cli := client.New(client.Options{Socket: socket, ProducerInstance: "producer-1"})
	reg := registerManifest(t, cli, "semantic-service")

	event, err := engine.NewEvent(
		protocol.EventSource(reg.Application.ApplicationID, "semantic.event_state"),
		protocol.FeedbackEventType(protocol.FeedbackUnknown),
		"semantic.event_state",
		protocol.EventData{
			FeedbackType: protocol.FeedbackUnknown,
			ReasonCode:   "UNKNOWN_EVENT_PHRASE",
			Payload:      json.RawMessage(`{"text":"Acme business was booked at 512.00"}`),
		},
	)
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	resp, err := cli.EmitStrict(context.Background(), client.EmitRequest{Event: event})
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	if !resp.Accepted || resp.IssueID == "" {
		t.Fatalf("unexpected acknowledgement: %+v", resp)
	}

	var issues struct {
		Issues []projection.Issue `json:"issues"`
	}
	if err := cli.Get(context.Background(), "/v1/issues", &issues); err != nil {
		t.Fatalf("list issues: %v", err)
	}
	if len(issues.Issues) != 1 || issues.Issues[0].OccurrenceCount != 1 {
		t.Fatalf("unexpected issues: %+v", issues.Issues)
	}

	var status engine.Status
	if err := cli.Get(context.Background(), "/v1/status", &status); err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Events != 1 || status.LedgerRecords < 2 {
		t.Fatalf("unexpected status: %+v", status)
	}
	if status.DriftRecords != 0 {
		t.Fatalf("projection is behind the ledger by %d records", status.DriftRecords)
	}
}

// TestApplicationKeepsRunningWhenLymphIsDown is the non-blocking requirement
// from section 66: the application must not care that Lymph is unavailable.
func TestApplicationKeepsRunningWhenLymphIsDown(t *testing.T) {
	root := t.TempDir()
	spoolDir := filepath.Join(root, "spool")
	deadSocket := filepath.Join(root, "missing.sock")

	// The application identity is chosen by the application, not by Lymph, so
	// the app can be running (and failing to reach Lymph) before Lymph ever
	// hears about it.
	appID := identity.NewID()

	offline := client.New(client.Options{
		Socket:           deadSocket,
		SpoolDir:         spoolDir,
		ProducerInstance: "producer-1",
		Timeout:          time.Second,
	})
	event, err := engine.NewEvent(
		protocol.EventSource(appID, "semantic.event_state"),
		protocol.FeedbackEventType(protocol.FeedbackUnknown),
		"semantic.event_state",
		protocol.EventData{
			FeedbackType: protocol.FeedbackUnknown,
			ReasonCode:   "UNKNOWN_EVENT_PHRASE",
			Payload:      json.RawMessage(`{"text":"Acme business was booked at 512.00"}`),
		},
	)
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	resp, err := offline.Emit(context.Background(), client.EmitRequest{Event: event})
	if err != nil {
		t.Fatalf("emit with the daemon down must not fail: %v", err)
	}
	if !resp.Spooled {
		t.Fatalf("expected the event to be spooled, got %+v", resp)
	}
	count, err := offline.Spool().Count()
	if err != nil {
		t.Fatalf("spool count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 spooled event, got %d", count)
	}

	// Without a spool the same call must fail loudly instead.
	strict := client.New(client.Options{Socket: deadSocket, Timeout: time.Second})
	if _, err := strict.Emit(context.Background(), client.EmitRequest{Event: event}); err == nil {
		t.Fatal("with spooling disabled, an unreachable daemon must be an error")
	}

	// Lymph comes up later, on the same machine, and the application registers
	// its existing identity (section 79: app UUID stays stable across hosts).
	_, liveSocket, stop := startDaemon(t)
	defer stop()

	live := client.New(client.Options{
		Socket:           liveSocket,
		SpoolDir:         spoolDir,
		ProducerInstance: "producer-1",
	})
	var reg engine.Registration
	if err := live.Post(context.Background(), "/v1/applications", engine.Manifest{
		ApplicationID: appID,
		Name:          "dataingest",
		Junctions: []engine.JunctionSpec{
			{Name: "semantic.event_state", Workflow: "semantic-rule-repair", ConfigFamily: "semantic_event_rules"},
		},
		ConfigFamilies: []engine.ConfigFamilySpec{
			{Name: "semantic_event_rules", SchemaRevision: "semantic-rules-v3"},
		},
	}, &reg); err != nil {
		t.Fatalf("register over socket: %v", err)
	}

	sent, err := live.FlushSpool(context.Background())
	if err != nil {
		t.Fatalf("flush spool: %v", err)
	}
	if sent != 1 {
		t.Fatalf("expected 1 delivered event, got %d", sent)
	}
	pending, err := live.Spool().Count()
	if err != nil {
		t.Fatalf("spool count: %v", err)
	}
	if pending != 0 {
		t.Fatalf("expected an empty spool after flushing, got %d", pending)
	}

	var status engine.Status
	if err := live.Get(context.Background(), "/v1/status", &status); err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Events != 1 {
		t.Fatalf("the spooled event never reached the ledger: %+v", status)
	}
	if status.LedgerRecords < 3 {
		// REGISTRATION + AUDIT + EVENT: the issue bookkeeping rides inside the
		// event record, since grouping is decided once and replayed verbatim.
		t.Fatalf("expected the registration and delivery to be journaled, got %d records", status.LedgerRecords)
	}
}

func TestSocketPermissionsAreNotWorldWritable(t *testing.T) {
	_, socket, stop := startDaemon(t)
	defer stop()

	info, err := os.Stat(socket)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if info.Mode().Perm()&0o007 != 0 {
		t.Fatalf("socket is world-accessible: %v", info.Mode().Perm())
	}
	if info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("expected a socket, got %v", info.Mode())
	}
}
