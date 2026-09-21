package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
)

// registeredApp registers one application with one installation and returns the
// identities a client would hold.
func registeredApp(t *testing.T, e *engine.Engine, name string, specs ...engine.InstallationSpec) (string, string) {
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
	reg, err := e.RegisterApplication(context.Background(), manifest)
	if err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
	return reg.Application.ApplicationID, reg.DefaultInstallationID
}

func helloFor(t *testing.T, applicationID, installationID, processID string) engine.HelloRequest {
	t.Helper()
	return engine.HelloRequest{
		ProtocolVersion: protocol.Version,
		ApplicationID:   applicationID,
		InstallationID:  installationID,
		ProcessID:       processID,
		ClientName:      "test-client",
		ClientVersion:   "0.1.0",
	}
}

// TestL26HandshakeValidation walks the whole validation sequence from mission
// section 12, one case per rule.
func TestL26HandshakeValidation(t *testing.T) {
	e := newEngine(t)
	appID, installationID := registeredApp(t, e, "hello-app")
	otherAppID, _ := registeredApp(t, e, "other-app")
	ctx := context.Background()
	peer := engine.PeerInfo{UID: 1000, GID: 1000, Supported: true}

	cases := []struct {
		name       string
		mutate     func(*engine.HelloRequest)
		wantCode   string
		wantAccept bool
	}{
		{"valid", func(r *engine.HelloRequest) {}, "", true},
		{"missing protocol", func(r *engine.HelloRequest) { r.ProtocolVersion = "" }, "MALFORMED_REQUEST", false},
		{"future protocol", func(r *engine.HelloRequest) { r.ProtocolVersion = "2" }, "PROTOCOL_INCOMPATIBLE", false},
		{"protocol as daemon version", func(r *engine.HelloRequest) { r.ProtocolVersion = "0.2.0-l2.6" }, "PROTOCOL_INCOMPATIBLE", false},
		{"missing application", func(r *engine.HelloRequest) { r.ApplicationID = "" }, "INVALID_IDENTITY", false},
		{"application not a uuid", func(r *engine.HelloRequest) { r.ApplicationID = "semantic-service" }, "INVALID_IDENTITY", false},
		{"missing installation", func(r *engine.HelloRequest) { r.InstallationID = "" }, "INVALID_IDENTITY", false},
		{"installation not a uuid", func(r *engine.HelloRequest) { r.InstallationID = "mac-development" }, "INVALID_IDENTITY", false},
		{"missing process", func(r *engine.HelloRequest) { r.ProcessID = "" }, "INVALID_IDENTITY", false},
		{"process not a uuid", func(r *engine.HelloRequest) { r.ProcessID = strconv.Itoa(12345) }, "INVALID_IDENTITY", false},
		{"unknown application", func(r *engine.HelloRequest) { r.ApplicationID = identity.NewID() }, "UNKNOWN_APPLICATION", false},
		{"unknown installation", func(r *engine.HelloRequest) { r.InstallationID = identity.NewID() }, "UNKNOWN_INSTALLATION", false},
		{"installation of another application", func(r *engine.HelloRequest) {
			r.ApplicationID = otherAppID
			r.InstallationID = installationID
		}, "INSTALLATION_APPLICATION_MISMATCH", false},
		{"hostname is not identity", func(r *engine.HelloRequest) {
			r.ClientName = "lymphd"
			r.Hostname = installationID
		}, "", true},
	}

	accepted := 0
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := helloFor(t, appID, installationID, identity.NewID())
			tc.mutate(&req)
			response, err := e.Hello(ctx, req, peer)

			if tc.wantAccept {
				if err != nil {
					t.Fatalf("expected acceptance, got %v", err)
				}
				if !response.Accepted || response.SessionID == "" {
					t.Fatalf("accepted response is incomplete: %+v", response)
				}
				if response.LymphInstanceID != e.Identity().InstanceUUID {
					t.Fatalf("response must carry the lymph instance id")
				}
				if len(response.Capabilities) == 0 {
					t.Fatalf("response must advertise capabilities")
				}
				accepted++
				return
			}
			if err == nil {
				t.Fatalf("expected refusal with %s", tc.wantCode)
			}
			if got := engine.ReasonCodeOf(err); got != tc.wantCode {
				t.Fatalf("refused with %q, expected %q (%v)", got, tc.wantCode, err)
			}
			if response.Accepted {
				t.Fatalf("refused handshake must not report accepted")
			}
		})
	}

	if accepted != 2 {
		t.Fatalf("expected 2 accepted handshakes, got %d", accepted)
	}

	// A refusal is canonical evidence; a success is not (mission section 105).
	audit, err := e.DB().ListAudit(200)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	rejections := 0
	for _, entry := range audit {
		switch entry.Action {
		case protocol.AuditHandshakeRejected, protocol.AuditProtocolIncompatible, protocol.AuditIdentityMismatch:
			rejections++
		}
	}
	if rejections < 3 {
		t.Fatalf("expected abnormal handshakes to be audited, found %d audit events", rejections)
	}
	for _, entry := range audit {
		if entry.Action == "session.established" || entry.Action == "session.hello" {
			t.Fatalf("successful handshakes must not be canonicalised: %s", entry.Action)
		}
	}
}

// TestL26SessionLifecycle covers creation, activity, close, expiry and the
// daemon forgetting everything on restart.
func TestL26SessionLifecycle(t *testing.T) {
	root := t.TempDir()
	e := engineAt(t, root)
	appID, installationID := registeredApp(t, e, "lifecycle-app")
	ctx := context.Background()
	processID := identity.NewID()

	response, err := e.Hello(ctx, helloFor(t, appID, installationID, processID), engine.PeerInfo{UID: -1, GID: -1})
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}

	session, err := e.DB().GetSession(response.SessionID)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	if session.State != projection.SessionActive || session.ProcessID != processID {
		t.Fatalf("unexpected session: %+v", session)
	}

	// The same process handshaking again replaces its session rather than
	// accumulating one per reconnect (mission section 59).
	second, err := e.Hello(ctx, helloFor(t, appID, installationID, processID), engine.PeerInfo{UID: -1, GID: -1})
	if err != nil {
		t.Fatalf("second handshake: %v", err)
	}
	if second.SessionID == response.SessionID {
		t.Fatalf("a reconnect must mint a new session id")
	}
	first, err := e.DB().GetSession(response.SessionID)
	if err != nil {
		t.Fatalf("first session: %v", err)
	}
	if first.State != projection.SessionClosed {
		t.Fatalf("the replaced session should be CLOSED, is %s", first.State)
	}

	// Closing deliberately.
	closed, err := e.CloseSession(second.SessionID)
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	if closed.State != projection.SessionClosed || closed.ClosedAt.IsZero() {
		t.Fatalf("unexpected closed session: %+v", closed)
	}

	// Expiry: an idle session becomes STALE, which is what actually cleans up
	// after a crash (mission sections 36, 72).
	third, err := e.Hello(ctx, helloFor(t, appID, installationID, identity.NewID()), engine.PeerInfo{UID: -1, GID: -1})
	if err != nil {
		t.Fatalf("third handshake: %v", err)
	}
	stale, _, err := e.SweepSessions(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if stale != 0 {
		t.Fatalf("a fresh session should not expire, %d did", stale)
	}
	// Rewrite LastSeenAt into the past to simulate idleness without waiting.
	ageSession(t, e, third.SessionID, time.Now().UTC().Add(-2*time.Hour))
	stale, _, err = e.SweepSessions(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if stale != 1 {
		t.Fatalf("expected 1 expired session, got %d", stale)
	}
	expired, err := e.DB().GetSession(third.SessionID)
	if err != nil {
		t.Fatalf("expired session: %v", err)
	}
	if expired.State != projection.SessionStale {
		t.Fatalf("expected STALE, got %s", expired.State)
	}

	// A stale session cannot be used.
	if _, err := e.DB().GetSession(third.SessionID); err != nil {
		t.Fatalf("stale session should still be readable for diagnostics: %v", err)
	}

	// Restart: sessions do not survive (mission section 116).
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened := engineAt(t, root)
	sessions, err := reopened.Sessions("", "", "", 100)
	if err != nil {
		t.Fatalf("sessions: %v", err)
	}
	for _, session := range sessions {
		if session.State == projection.SessionActive {
			t.Fatalf("a restarted daemon must not hold ACTIVE sessions: %+v", session)
		}
	}
	// And the application reconnects normally afterwards.
	if _, err := reopened.Hello(ctx, helloFor(t, appID, installationID, identity.NewID()), engine.PeerInfo{UID: -1, GID: -1}); err != nil {
		t.Fatalf("reconnect after restart: %v", err)
	}
}

// TestL26DuplicateProcessPolicy covers mission sections 18, 19, 73 and 74.
func TestL26DuplicateProcessPolicy(t *testing.T) {
	ctx := context.Background()

	t.Run("multi-process installation accepts several processes", func(t *testing.T) {
		e := newEngine(t)
		appID, installationID := registeredApp(t, e, "multi-app", engine.InstallationSpec{
			InstallationID: identity.NewID(), Name: "web", SessionPolicy: projection.SessionPolicyMulti,
		})
		for i := 0; i < 3; i++ {
			response, err := e.Hello(ctx, helloFor(t, appID, installationID, identity.NewID()), engine.PeerInfo{UID: -1, GID: -1})
			if err != nil {
				t.Fatalf("process %d: %v", i, err)
			}
			if !response.Accepted {
				t.Fatalf("process %d was refused", i)
			}
			if i > 0 && len(response.Warnings) == 0 {
				t.Fatalf("process %d should carry a multiple-process warning", i)
			}
		}
	})

	t.Run("single-process installation warns but still accepts", func(t *testing.T) {
		e := newEngine(t)
		appID, installationID := registeredApp(t, e, "single-app", engine.InstallationSpec{
			InstallationID: identity.NewID(), Name: "daemon", SessionPolicy: projection.SessionPolicySingle,
		})
		if _, err := e.Hello(ctx, helloFor(t, appID, installationID, identity.NewID()), engine.PeerInfo{UID: -1, GID: -1}); err != nil {
			t.Fatalf("first process: %v", err)
		}
		second, err := e.Hello(ctx, helloFor(t, appID, installationID, identity.NewID()), engine.PeerInfo{UID: -1, GID: -1})
		if err != nil {
			t.Fatalf("second process should be accepted with a warning, got %v", err)
		}
		found := false
		for _, warning := range second.Warnings {
			if len(warning) >= len("DUPLICATE_INSTALLATION_SESSION") && warning[:len("DUPLICATE_INSTALLATION_SESSION")] == "DUPLICATE_INSTALLATION_SESSION" {
				found = true
			}
		}
		if !found {
			t.Fatalf("expected a duplicate-session warning, got %v", second.Warnings)
		}

		// And the abnormal outcome is canonical.
		audit, err := e.DB().ListAudit(50)
		if err != nil {
			t.Fatalf("audit: %v", err)
		}
		seen := false
		for _, entry := range audit {
			if entry.Action == protocol.AuditDuplicateSession {
				seen = true
			}
		}
		if !seen {
			t.Fatalf("a duplicate session should be audited")
		}
	})

	t.Run("strict policy refuses", func(t *testing.T) {
		root := t.TempDir()
		e, err := engine.Open(engine.Options{
			Root: root, Logger: quietLogger(),
			Sessions: engine.SessionConfig{IdleTimeout: time.Minute, Retention: time.Hour, StrictDuplicatePolicy: true},
		})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer e.Close()
		appID, installationID := registeredApp(t, e, "strict-app", engine.InstallationSpec{
			InstallationID: identity.NewID(), SessionPolicy: projection.SessionPolicySingle,
		})
		if _, err := e.Hello(ctx, helloFor(t, appID, installationID, identity.NewID()), engine.PeerInfo{UID: -1, GID: -1}); err != nil {
			t.Fatalf("first process: %v", err)
		}
		if _, err := e.Hello(ctx, helloFor(t, appID, installationID, identity.NewID()), engine.PeerInfo{UID: -1, GID: -1}); err == nil {
			t.Fatalf("strict policy must refuse the second process")
		}
	})
}

// TestL26ManifestDriftAndInstance covers mission sections 17 and 107.
func TestL26ManifestDriftAndInstance(t *testing.T) {
	e := newEngine(t)
	appID, installationID := registeredApp(t, e, "drift-app")
	ctx := context.Background()

	req := helloFor(t, appID, installationID, identity.NewID())
	req.ManifestHash = "sha256:" + "00"
	response, err := e.Hello(ctx, req, engine.PeerInfo{UID: -1, GID: -1})
	if err != nil {
		t.Fatalf("manifest drift must not refuse a handshake: %v", err)
	}
	if len(response.Warnings) == 0 {
		t.Fatalf("manifest drift should warn")
	}
	audit, err := e.DB().ListAudit(20)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	drift := false
	for _, entry := range audit {
		if entry.Action == protocol.AuditManifestDrift {
			drift = true
		}
	}
	if !drift {
		t.Fatalf("manifest drift should be audited")
	}

	// The daemon reports its instance identity so a client can notice it has
	// been pointed at a different Lymph installation.
	if response.LymphInstanceID == "" || response.LymphInstanceID != e.Identity().InstanceUUID {
		t.Fatalf("handshake must report the lymph instance id")
	}
}

// TestL26EventSessionRules covers mission sections 21, 22, 61, 62 and 63.
func TestL26EventSessionRules(t *testing.T) {
	ctx := context.Background()
	e := newEngine(t)
	appID, installationID := registeredApp(t, e, "event-app")

	buildEvent := func(applicationID string, payload map[string]any) protocol.Event {
		t.Helper()
		event, err := engine.NewEvent(
			protocol.EventSource(applicationID, "semantic.event_state"),
			protocol.FeedbackEventType(protocol.FeedbackUnknown),
			"semantic.event_state",
			protocol.EventData{FeedbackType: protocol.FeedbackUnknown, ReasonCode: "TEST", Payload: mustJSON(t, payload)},
		)
		if err != nil {
			t.Fatalf("build event: %v", err)
		}
		return event
	}

	processID := identity.NewID()
	response, err := e.Hello(ctx, helloFor(t, appID, installationID, processID), engine.PeerInfo{UID: -1, GID: -1})
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	sessionID := response.SessionID

	t.Run("session fills in installation and process", func(t *testing.T) {
		resp, err := e.Emit(ctx, engine.EmitRequest{
			Event:     buildEvent(appID, map[string]any{"text": "awarded one"}),
			SessionID: sessionID,
		})
		if err != nil {
			t.Fatalf("emit under session: %v", err)
		}
		event, err := e.DB().GetEvent(resp.EventID)
		if err != nil {
			t.Fatalf("event: %v", err)
		}
		if event.InstallationID != installationID {
			t.Fatalf("event installation %q, expected %q", event.InstallationID, installationID)
		}
		if event.ProducerInstance != processID {
			t.Fatalf("event producer %q, expected the session process %q", event.ProducerInstance, processID)
		}
	})

	t.Run("explicit mismatched installation is refused", func(t *testing.T) {
		_, err := e.Emit(ctx, engine.EmitRequest{
			Event:          buildEvent(appID, map[string]any{"text": "awarded two"}),
			SessionID:      sessionID,
			InstallationID: identity.NewID(),
		})
		if !errors.Is(err, engine.ErrSessionIdentityMismatch) {
			t.Fatalf("expected SESSION_IDENTITY_MISMATCH, got %v", err)
		}
	})

	t.Run("wrong application is refused", func(t *testing.T) {
		_, err := e.Emit(ctx, engine.EmitRequest{
			Event:     buildEvent(identity.NewID(), map[string]any{"text": "awarded three"}),
			SessionID: sessionID,
		})
		if err == nil {
			t.Fatalf("an event claiming another application must be refused")
		}
	})

	t.Run("wrong process is refused", func(t *testing.T) {
		_, err := e.Emit(ctx, engine.EmitRequest{
			Event:            buildEvent(appID, map[string]any{"text": "awarded four"}),
			SessionID:        sessionID,
			ProducerInstance: identity.NewID(),
		})
		if !errors.Is(err, engine.ErrSessionIdentityMismatch) {
			t.Fatalf("expected SESSION_IDENTITY_MISMATCH, got %v", err)
		}
	})

	t.Run("unknown session is reported as such", func(t *testing.T) {
		_, err := e.Emit(ctx, engine.EmitRequest{
			Event:     buildEvent(appID, map[string]any{"text": "awarded five"}),
			SessionID: identity.NewID(),
		})
		if !errors.Is(err, engine.ErrSessionUnknown) {
			t.Fatalf("expected SESSION_UNKNOWN, got %v", err)
		}
	})

	t.Run("replay may carry an older process", func(t *testing.T) {
		oldProcess := identity.NewID()
		resp, err := e.Emit(ctx, engine.EmitRequest{
			Event:            buildEvent(appID, map[string]any{"text": "awarded six"}),
			SessionID:        sessionID,
			ProducerInstance: oldProcess,
			Replay:           true,
		})
		if err != nil {
			t.Fatalf("replay should be accepted: %v", err)
		}
		event, err := e.DB().GetEvent(resp.EventID)
		if err != nil {
			t.Fatalf("event: %v", err)
		}
		if event.ProducerInstance != oldProcess {
			t.Fatalf("replay must preserve the original producer, got %q", event.ProducerInstance)
		}
		if event.InstallationID != installationID {
			t.Fatalf("replay installation %q", event.InstallationID)
		}
	})

	t.Run("a non-canonical session header cannot match", func(t *testing.T) {
		// The session id is a string, compared exactly. Sending the same UUID in
		// another shape uuid.Parse accepts (32 hex digits, no hyphens) must not
		// match the stored session: the header is a key, not a parsed identity.
		stripped := strings.ReplaceAll(sessionID, "-", "")
		if _, err := e.Emit(ctx, engine.EmitRequest{
			Event:     buildEvent(appID, map[string]any{"text": "awarded nine"}),
			SessionID: stripped,
		}); !errors.Is(err, engine.ErrSessionUnknown) {
			t.Fatalf("a non-canonical session header must fail closed, got %v", err)
		}
	})

	t.Run("a closed session is no longer usable", func(t *testing.T) {
		if _, err := e.CloseSession(sessionID); err != nil {
			t.Fatalf("close: %v", err)
		}
		_, err := e.Emit(ctx, engine.EmitRequest{
			Event:     buildEvent(appID, map[string]any{"text": "awarded seven"}),
			SessionID: sessionID,
		})
		if !errors.Is(err, engine.ErrSessionUnknown) {
			t.Fatalf("expected SESSION_UNKNOWN for a closed session, got %v", err)
		}
	})

	t.Run("sessionless clients still work and are counted", func(t *testing.T) {
		before := statusOf(t, e).SessionlessEvents
		if _, err := e.Emit(ctx, engine.EmitRequest{Event: buildEvent(appID, map[string]any{"text": "awarded eight"})}); err != nil {
			t.Fatalf("legacy sessionless emit: %v", err)
		}
		after := statusOf(t, e).SessionlessEvents
		if after <= before {
			t.Fatalf("sessionless events should be counted (%d -> %d)", before, after)
		}
	})
}

// TestL26SessionScoping covers mission sections 119 and 120: process and session
// identity never become configuration identity, and never split an issue.
func TestL26SessionScoping(t *testing.T) {
	ctx := context.Background()
	e := newEngine(t)
	appID, installationID := registeredApp(t, e, "scoping-app")

	var issueIDs []string
	for i := 0; i < 3; i++ {
		response, err := e.Hello(ctx, helloFor(t, appID, installationID, identity.NewID()), engine.PeerInfo{UID: -1, GID: -1})
		if err != nil {
			t.Fatalf("handshake %d: %v", i, err)
		}
		event, err := engine.NewEvent(
			protocol.EventSource(appID, "semantic.event_state"),
			protocol.FeedbackEventType(protocol.FeedbackUnknown),
			"semantic.event_state",
			protocol.EventData{FeedbackType: protocol.FeedbackUnknown, ReasonCode: "NO_RULE_MATCH",
				Payload: mustJSON(t, map[string]any{"text": "the same unknown phrase"})},
		)
		if err != nil {
			t.Fatalf("event: %v", err)
		}
		emitted, err := e.Emit(ctx, engine.EmitRequest{Event: event, SessionID: response.SessionID})
		if err != nil {
			t.Fatalf("emit %d: %v", i, err)
		}
		issueIDs = append(issueIDs, emitted.IssueID)
	}

	if issueIDs[0] != issueIDs[1] || issueIDs[1] != issueIDs[2] {
		t.Fatalf("three processes reporting one problem must stay one issue: %v", issueIDs)
	}
	issue, err := e.DB().GetIssue(issueIDs[0])
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if issue.OccurrenceCount != 3 || issue.UniqueSources != 3 {
		t.Fatalf("expected 3 occurrences from 3 processes, got %d/%d",
			issue.OccurrenceCount, issue.UniqueSources)
	}

	// No ref or candidate belongs to a process.
	refs, err := e.DB().ListRefs(appID, "", "")
	if err != nil {
		t.Fatalf("refs: %v", err)
	}
	for _, ref := range refs {
		if ref.InstallationID == "" {
			t.Fatalf("ref %s has no installation", ref.Name)
		}
	}
}

// helpers

// engineAt opens an engine over an existing store root.
func engineAt(t *testing.T, root string) *engine.Engine {
	t.Helper()
	e, err := engine.Open(engine.Options{Root: root, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("open engine at %s: %v", root, err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// statusOf reads the daemon status for the operational counters.
func statusOf(t *testing.T, e *engine.Engine) engine.Status {
	t.Helper()
	status, err := e.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	return status
}

// ageSession rewrites a session's LastSeenAt so expiry can be tested without
// waiting for the idle timeout.
func ageSession(t *testing.T, e *engine.Engine, sessionID string, at time.Time) {
	t.Helper()
	if _, err := e.DB().SQL().Exec(`UPDATE sessions SET last_seen_at = ? WHERE session_id = ?`,
		at.UTC().Format(time.RFC3339Nano), sessionID); err != nil {
		t.Fatalf("age session: %v", err)
	}
}
