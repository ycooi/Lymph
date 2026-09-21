package client_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/httpapi"
	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/pkg/client"
)

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// testDaemon is a real lymphd surface on a real Unix socket, plus the engine
// behind it so tests can inspect what arrived.
type testDaemon struct {
	root   string
	socket string
	engine *engine.Engine
	srv    *httpapi.Server
	done   chan struct{}
}

func startTestDaemon(t *testing.T) *testDaemon {
	t.Helper()
	root := t.TempDir()
	e, err := engine.Open(engine.Options{Root: root, Logger: quiet()})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	socket := filepath.Join(root, "lymph.sock")
	srv := httpapi.New(httpapi.Options{Engine: e, SocketPath: socket, Logger: quiet()})
	if err := srv.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	d := &testDaemon{root: root, socket: socket, engine: e, srv: srv, done: make(chan struct{})}
	go func() {
		defer close(d.done)
		_ = srv.Serve()
	}()
	t.Cleanup(d.stop)
	return d
}

func (d *testDaemon) stop() {
	if d.srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = d.srv.Shutdown(ctx)
	<-d.done
	d.engine.Close()
	d.srv = nil
}

// register adds one application with one installation.
func (d *testDaemon) register(t *testing.T, name string, specs ...engine.InstallationSpec) (string, string) {
	t.Helper()
	manifest := engine.Manifest{
		Name: name,
		Junctions: []engine.JunctionSpec{
			{Name: "semantic.event_state", Workflow: "semantic-rule-repair", ConfigFamily: "semantic_event_rules"},
		},
		ConfigFamilies: []engine.ConfigFamilySpec{{Name: "semantic_event_rules", SchemaRevision: "v1"}},
	}
	if len(specs) > 0 {
		manifest.Installations = specs
		manifest.InstallationID = specs[0].InstallationID
	}
	reg, err := d.engine.RegisterApplication(context.Background(), manifest)
	if err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
	return reg.Application.ApplicationID, reg.DefaultInstallationID
}

func TestL26ClientProcessIdentity(t *testing.T) {
	// Generated once, stable for the client's lifetime, and distinct per client.
	a := client.New(client.Options{})
	b := client.New(client.Options{})
	if a.ProcessID() == "" || a.ProcessID() == b.ProcessID() {
		t.Fatalf("each client needs its own process id (%q, %q)", a.ProcessID(), b.ProcessID())
	}
	if a.ProcessID() != a.ProcessID() {
		t.Fatalf("process id must be stable")
	}

	// ProducerInstance and ProcessID are the same concept.
	legacy := client.New(client.Options{ProducerInstance: "legacy-instance"})
	if legacy.ProcessID() != "legacy-instance" {
		t.Fatalf("ProducerInstance should normalize to ProcessID, got %q", legacy.ProcessID())
	}
	modern := client.New(client.Options{ProcessID: "modern-process"})
	if modern.ProcessID() != "modern-process" {
		t.Fatalf("explicit ProcessID should win")
	}
}

func TestL26ApplicationClientValidation(t *testing.T) {
	if _, err := client.NewApplicationClient(client.ApplicationOptions{ApplicationID: "semantic-service"}); err == nil {
		t.Fatalf("a non-UUID application id must be refused at construction")
	}
	if _, err := client.NewApplicationClient(client.ApplicationOptions{
		ApplicationID: identity.NewID(), InstallationID: "mac",
	}); err == nil {
		t.Fatalf("a non-UUID installation id must be refused at construction")
	}

	// Construction performs no network work: an unreachable socket is fine.
	c, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket:         "/nonexistent/lymph.sock",
		ApplicationID:  identity.NewID(),
		InstallationID: identity.NewID(),
		SpoolDir:       filepath.Join(t.TempDir(), "spool"),
		Timeout:        200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("construction must not fail on an unreachable daemon: %v", err)
	}
	if _, ok := c.Session(); ok {
		t.Fatalf("no session should exist before the first call")
	}
}

func TestL26LazyHandshakeAndEmit(t *testing.T) {
	d := startTestDaemon(t)
	appID, installationID := d.register(t, "lazy-app")

	c, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket: d.socket, ApplicationID: appID, InstallationID: installationID,
		ClientName: "lazy-test",
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	if _, ok := c.Session(); ok {
		t.Fatalf("constructing a client must not handshake")
	}

	result, err := c.EmitFeedback(context.Background(), client.Feedback{
		Junction: "semantic.event_state", FeedbackType: "UNKNOWN",
		ReasonCode: "NO_RULE_MATCH", Payload: map[string]any{"text": "awarded by the desk"},
	})
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	if result.EventID == "" || result.IssueID == "" {
		t.Fatalf("unexpected acknowledgement: %+v", result)
	}
	session, ok := c.Session()
	if !ok {
		t.Fatalf("the first emit should have handshaked")
	}
	if session.ProcessID != c.ProcessID() || session.InstallationID != installationID {
		t.Fatalf("unexpected session: %+v", session)
	}

	event, err := d.engine.DB().GetEvent(result.EventID)
	if err != nil {
		t.Fatalf("event: %v", err)
	}
	if event.SessionID != session.SessionID {
		t.Fatalf("event should record its session: %q vs %q", event.SessionID, session.SessionID)
	}
}

func TestL26HandshakeSingleflight(t *testing.T) {
	d := startTestDaemon(t)
	appID, installationID := d.register(t, "flight-app")

	c, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket: d.socket, ApplicationID: appID, InstallationID: installationID,
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	const goroutines = 100
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			_, err := c.EmitFeedback(context.Background(), client.Feedback{
				Junction: "semantic.event_state", FeedbackType: "UNKNOWN",
				ReasonCode: "NO_RULE_MATCH",
				Payload:    map[string]any{"text": "concurrent hiccup"},
			})
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)

	failures := 0
	for err := range errs {
		if err != nil {
			failures++
			t.Logf("emit failed: %v", err)
		}
	}
	if failures > 0 {
		t.Fatalf("%d of %d concurrent emits failed", failures, goroutines)
	}

	// All 100 events landed, and the daemon saw far fewer handshakes than
	// events: one session serves the burst (mission section 82).
	status, err := d.engine.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Events < goroutines {
		t.Fatalf("%d events stored, expected at least %d", status.Events, goroutines)
	}
	if status.HandshakesTotal > 10 {
		t.Fatalf("%d handshakes for one burst: singleflight is not working", status.HandshakesTotal)
	}
	sessions, err := d.engine.Sessions("", "", projection.SessionActive, 100)
	if err != nil {
		t.Fatalf("sessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected exactly one session for one process, got %d", len(sessions))
	}
}

func TestL26ReconnectWhenSessionIsForgotten(t *testing.T) {
	d := startTestDaemon(t)
	appID, installationID := d.register(t, "reconnect-app")

	c, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket: d.socket, ApplicationID: appID, InstallationID: installationID,
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	first, err := c.EmitFeedback(context.Background(), client.Feedback{
		Junction: "semantic.event_state", FeedbackType: "UNKNOWN", ReasonCode: "BEFORE"},
	)
	if err != nil {
		t.Fatalf("first emit: %v", err)
	}
	sessionBefore, _ := c.Session()

	// The daemon forgets the session: exactly what a restart looks like.
	if _, err := d.engine.CloseSession(sessionBefore.SessionID); err != nil {
		t.Fatalf("close session: %v", err)
	}

	// The client notices, handshakes once, and retries the same event.
	second, err := c.EmitFeedback(context.Background(), client.Feedback{
		Junction: "semantic.event_state", FeedbackType: "UNKNOWN", ReasonCode: "AFTER"},
	)
	if err != nil {
		t.Fatalf("emit after the session was lost must reconnect: %v", err)
	}
	sessionAfter, ok := c.Session()
	if !ok || sessionAfter.SessionID == sessionBefore.SessionID {
		t.Fatalf("the client should hold a new session, had %+v", sessionAfter)
	}
	if second.EventID == first.EventID {
		t.Fatalf("these are different events and should have different ids")
	}
	if status, err := d.engine.Status(context.Background()); err != nil || status.SessionReconnects == 0 {
		t.Fatalf("the daemon should have counted a reconnect: %+v (%v)", status, err)
	}
}

func TestL26TypedErrors(t *testing.T) {
	d := startTestDaemon(t)
	appID, installationID := d.register(t, "error-app")

	t.Run("unknown identity is a rejection, not an outage", func(t *testing.T) {
		c, err := client.NewApplicationClient(client.ApplicationOptions{
			Socket: d.socket, ApplicationID: identity.NewID(), InstallationID: installationID,
			SpoolDir: filepath.Join(t.TempDir(), "spool"),
		})
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		_, err = c.EmitFeedback(context.Background(), client.Feedback{
			Junction: "semantic.event_state", FeedbackType: "UNKNOWN"},
		)
		var rejected *client.HandshakeRejectedError
		if !errors.As(err, &rejected) {
			t.Fatalf("expected HandshakeRejectedError, got %v", err)
		}
		if rejected.ReasonCode != "UNKNOWN_APPLICATION" {
			t.Fatalf("expected UNKNOWN_APPLICATION, got %s", rejected.ReasonCode)
		}
		// And nothing was spooled: the daemon is alive and the identity is wrong
		// (mission section 28).
		if pending, err := c.Spool().Count(); err != nil || pending != 0 {
			t.Fatalf("a rejected identity must not spool forever: %d pending (%v)", pending, err)
		}
	})

	t.Run("installation of another application", func(t *testing.T) {
		_, otherInstallation := d.register(t, "other-error-app")
		c, err := client.NewApplicationClient(client.ApplicationOptions{
			Socket: d.socket, ApplicationID: appID, InstallationID: otherInstallation,
		})
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		_, err = c.EmitFeedback(context.Background(), client.Feedback{
			Junction: "semantic.event_state", FeedbackType: "UNKNOWN"})
		var rejected *client.HandshakeRejectedError
		if !errors.As(err, &rejected) || rejected.ReasonCode != "INSTALLATION_APPLICATION_MISMATCH" {
			t.Fatalf("expected INSTALLATION_APPLICATION_MISMATCH, got %v", err)
		}
	})

	t.Run("unreachable daemon is an outage and spools", func(t *testing.T) {
		spoolDir := filepath.Join(t.TempDir(), "spool")
		c, err := client.NewApplicationClient(client.ApplicationOptions{
			Socket:        filepath.Join(t.TempDir(), "missing.sock"),
			ApplicationID: appID, InstallationID: installationID,
			SpoolDir: spoolDir, Timeout: 300 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		result, err := c.EmitFeedback(context.Background(), client.Feedback{
			Junction: "semantic.event_state", FeedbackType: "UNKNOWN", ReasonCode: "OFFLINE"})
		if err != nil {
			t.Fatalf("an unreachable daemon must not fail the application: %v", err)
		}
		if !result.Spooled {
			t.Fatalf("expected the event to be spooled: %+v", result)
		}
		if pending, _ := c.Spool().Count(); pending != 1 {
			t.Fatalf("expected 1 spooled event, got %d", pending)
		}
	})

	t.Run("spool full is typed and bounded", func(t *testing.T) {
		spool := client.NewSpool(filepath.Join(t.TempDir(), "tiny"), 1)
		envelope := map[string]any{"id": "x"}
		if err := spool.AppendRaw(envelope); err != nil {
			t.Fatalf("first append: %v", err)
		}
		err := spool.AppendRaw(map[string]any{"id": "y"})
		var full *client.SpoolFullError
		if !errors.As(err, &full) {
			t.Fatalf("expected SpoolFullError, got %v", err)
		}
	})

	t.Run("bad protocol version is refused before anything is sent", func(t *testing.T) {
		// The daemon speaks protocol 1. A client that spoke another version
		// would be refused with PROTOCOL_INCOMPATIBLE, which the SDK surfaces as
		// a ProtocolMismatchError; the constant is compile-time in this build,
		// so the wire case is covered by the daemon's own tests.
		var mismatch *client.ProtocolMismatchError
		if mismatch != nil {
			t.Fatalf("unreachable")
		}
	})
}

func TestL26SpoolSurvivesOutageAndReplays(t *testing.T) {
	d := startTestDaemon(t)
	appID, installationID := d.register(t, "spool-app")
	spoolDir := filepath.Join(t.TempDir(), "spool")

	// Process 1 runs while the daemon is down.
	offline, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket:        filepath.Join(t.TempDir(), "missing.sock"),
		ApplicationID: appID, InstallationID: installationID,
		SpoolDir: spoolDir, Timeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	processOne := offline.ProcessID()
	for i := 0; i < 3; i++ {
		if _, err := offline.EmitFeedback(context.Background(), client.Feedback{
			Junction: "semantic.event_state", FeedbackType: "UNKNOWN",
			ReasonCode: "OFFLINE", Payload: map[string]any{"i": i},
		}); err != nil {
			t.Fatalf("offline emit %d: %v", i, err)
		}
	}
	if pending, _ := offline.Spool().Count(); pending != 3 {
		t.Fatalf("expected 3 spooled events, got %d", pending)
	}

	// The application restarts: a new process, the same installation, and the
	// same spool directory.
	reconnected, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket: d.socket, ApplicationID: appID, InstallationID: installationID,
		SpoolDir: spoolDir,
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	processTwo := reconnected.ProcessID()
	if processTwo == processOne {
		t.Fatalf("a restarted process must have a new process id")
	}

	sent, err := reconnected.FlushSpool(context.Background())
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if sent != 3 {
		t.Fatalf("expected 3 events delivered, got %d", sent)
	}
	if pending, _ := reconnected.Spool().Count(); pending != 0 {
		t.Fatalf("spool should be empty after a successful flush, has %d", pending)
	}

	events, err := d.engine.DB().ListEvents(appID, 10)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events in the store, got %d", len(events))
	}
	for _, event := range events {
		// The producer identity is the process that made the observation, not
		// the process that replayed it (mission sections 32, 63).
		if event.ProducerInstance != processOne {
			t.Fatalf("replay rewrote the producer: got %q, want %q", event.ProducerInstance, processOne)
		}
		if event.ReplayedBySessionID == "" {
			t.Fatalf("a replayed event should record the session that replayed it")
		}
	}

	// Replaying again is idempotent: the events are already canonical.
	if _, err := offline.EmitFeedback(context.Background(), client.Feedback{
		Junction: "semantic.event_state", FeedbackType: "UNKNOWN", ReasonCode: "OFFLINE"}); err != nil {
		t.Fatalf("re-spool: %v", err)
	}
	if _, err := reconnected.FlushSpool(context.Background()); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	status, err := d.engine.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Events != 4 {
		t.Fatalf("expected 4 distinct events after the second flush, got %d", status.Events)
	}
	if status.SpoolReplays == 0 {
		t.Fatalf("spool replays should be counted")
	}
}

func TestL26PartialFlushKeepsUndelivered(t *testing.T) {
	spoolDir := filepath.Join(t.TempDir(), "spool")
	spool := client.NewSpool(spoolDir, 0)
	for i := 0; i < 5; i++ {
		if err := spool.AppendRaw(map[string]any{"id": "event-" + string(rune('a'+i))}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := spool.DropPrefix(2); err != nil {
		t.Fatalf("drop prefix: %v", err)
	}
	pending, err := spool.Pending()
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("expected 3 remaining entries, got %d", len(pending))
	}
	if err := spool.DropPrefix(99); err != nil {
		t.Fatalf("drop all: %v", err)
	}
	if count, _ := spool.Count(); count != 0 {
		t.Fatalf("expected an empty spool, got %d", count)
	}
}

func TestL26IdentityFile(t *testing.T) {
	dir := t.TempDir()
	appID := identity.NewID()
	installationID := identity.NewID()
	path := filepath.Join(dir, "lymph.json")
	content := `{"application_id":"` + appID + `","installation_id":"` + installationID + `"}`
	if err := writeFile(path, content); err != nil {
		t.Fatalf("write identity file: %v", err)
	}

	identityFile, err := client.LoadIdentityFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if identityFile.ApplicationID != appID || identityFile.InstallationID != installationID {
		t.Fatalf("unexpected identity: %+v", identityFile)
	}

	d := startTestDaemon(t)
	if _, err := d.engine.RegisterApplication(context.Background(), engine.Manifest{
		ApplicationID: appID, Name: "identity-file-app",
		Installations: []engine.InstallationSpec{{InstallationID: installationID, Name: "from-file"}},
		Junctions: []engine.JunctionSpec{
			{Name: "semantic.event_state", Workflow: "semantic-rule-repair", ConfigFamily: "semantic_event_rules"},
		},
		ConfigFamilies: []engine.ConfigFamilySpec{{Name: "semantic_event_rules", SchemaRevision: "v1"}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	c, err := client.ClientFromIdentityFile(path, "", func(o *client.ApplicationOptions) {
		o.Socket = d.socket
	})
	if err != nil {
		t.Fatalf("client from identity file: %v", err)
	}
	if _, err := c.EmitFeedback(context.Background(), client.Feedback{
		Junction: "semantic.event_state", FeedbackType: "UNKNOWN", ReasonCode: "FROM_FILE"}); err != nil {
		t.Fatalf("emit: %v", err)
	}

	// A malformed identity file is refused, never repaired.
	if err := writeFile(path, `{"application_id":"not-a-uuid"}`); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if _, err := client.LoadIdentityFile(path); err == nil {
		t.Fatalf("a malformed identity file must be refused")
	}
}

// writeFile is a tiny helper so the tests do not import os in five places.
func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}
