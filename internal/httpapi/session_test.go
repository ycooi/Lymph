package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
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

// unixRequest performs one raw HTTP request over the Unix socket, so the tests
// exercise the wire rather than a Go method.
func unixRequest(t *testing.T, socket, method, path string, body any, headers map[string]string) (int, []byte) {
	t.Helper()
	var payload []byte
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		payload = raw
	}

	conn, err := net.DialTimeout("unix", socket, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", socket, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	request := fmt.Sprintf("%s %s HTTP/1.1\r\nHost: lymph\r\nConnection: close\r\n", method, path)
	if payload != nil {
		request += "Content-Type: application/json\r\n"
		request += fmt.Sprintf("Content-Length: %d\r\n", len(payload))
	}
	for name, value := range headers {
		request += fmt.Sprintf("%s: %s\r\n", name, value)
	}
	request += "\r\n"

	// Write headers and body together. Authorization may reject a request as
	// soon as the headers arrive; sending the body separately lets the server
	// close the socket between writes and makes this wire-level helper flaky.
	wire := append([]byte(request), payload...)
	if _, err := conn.Write(wire); err != nil {
		t.Fatalf("write request: %v", err)
	}
	response, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	text := string(response)
	statusLine, rest, _ := strings.Cut(text, "\r\n")
	var status int
	if _, err := fmt.Sscanf(statusLine, "HTTP/1.1 %d", &status); err != nil {
		t.Fatalf("unparsable status line %q", statusLine)
	}
	_, bodyText, _ := strings.Cut(rest, "\r\n\r\n")
	return status, []byte(bodyText)
}

func TestL26HelloOverRealSocket(t *testing.T) {
	root, socket, stop := startDaemon(t)
	defer stop()

	e := engineAt(t, root)
	reg, err := e.RegisterApplication(context.Background(), engine.Manifest{
		Name: "socket-app",
		Junctions: []engine.JunctionSpec{
			{Name: "semantic.event_state", Workflow: "semantic-rule-repair", ConfigFamily: "semantic_event_rules"},
		},
		ConfigFamilies: []engine.ConfigFamilySpec{{Name: "semantic_event_rules", SchemaRevision: "v1"}},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	appID := reg.Application.ApplicationID
	installationID := reg.DefaultInstallationID

	processID := identity.NewID()
	status, body := unixRequest(t, socket, http.MethodPost, "/v1/hello", map[string]any{
		"protocol_version": protocol.Version,
		"application_id":   appID,
		"installation_id":  installationID,
		"process_id":       processID,
		"client_name":      "raw-http-test",
		"client_version":   "0.1.0",
		"pid":              os.Getpid(),
		"hostname":         "test-host",
	}, nil)
	if status != http.StatusOK {
		t.Fatalf("hello status %d: %s", status, body)
	}
	var hello struct {
		Accepted        bool               `json:"accepted"`
		SessionID       string             `json:"session_id"`
		ProtocolVersion string             `json:"protocol_version"`
		ServerVersion   string             `json:"server_version"`
		Capabilities    []string           `json:"capabilities"`
		Session         projection.Session `json:"session"`
	}
	if err := json.Unmarshal(body, &hello); err != nil {
		t.Fatalf("decode hello: %v (%s)", err, body)
	}
	if !hello.Accepted || hello.SessionID == "" {
		t.Fatalf("handshake not accepted: %s", body)
	}
	if hello.ProtocolVersion != protocol.Version || hello.ServerVersion == "" {
		t.Fatalf("handshake must report protocol and server versions: %s", body)
	}
	if len(hello.Capabilities) == 0 {
		t.Fatalf("handshake must advertise capabilities")
	}

	// An event carrying the session header is accepted, and the session's
	// installation and process fill in what the body omitted.
	event, err := engine.NewEvent(
		protocol.EventSource(appID, "semantic.event_state"),
		protocol.FeedbackEventType(protocol.FeedbackUnknown),
		"semantic.event_state",
		protocol.EventData{FeedbackType: protocol.FeedbackUnknown, ReasonCode: "NO_RULE_MATCH"},
	)
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	status, body = unixRequest(t, socket, http.MethodPost, "/v1/events", event, map[string]string{
		protocol.HeaderSession: hello.SessionID,
	})
	if status != http.StatusOK {
		t.Fatalf("emit under session: %d %s", status, body)
	}
	var ack struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(body, &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	stored, err := e.DB().GetEvent(ack.EventID)
	if err != nil {
		t.Fatalf("event: %v", err)
	}
	if stored.InstallationID != installationID || stored.ProducerInstance != processID {
		t.Fatalf("session must supply installation and process: %+v", stored)
	}

	// The session is queryable, and lists filter by installation.
	status, body = unixRequest(t, socket, http.MethodGet, "/v1/sessions?installation="+installationID, nil, nil)
	if status != http.StatusOK {
		t.Fatalf("list sessions: %d %s", status, body)
	}
	var listing struct {
		Sessions []projection.Session `json:"sessions"`
	}
	if err := json.Unmarshal(body, &listing); err != nil {
		t.Fatalf("decode sessions: %v", err)
	}
	if len(listing.Sessions) != 1 || listing.Sessions[0].SessionID != hello.SessionID {
		t.Fatalf("unexpected sessions: %s", body)
	}

	// Closing it makes it unusable, and the daemon says so in machine-readable
	// terms (mission section 61).
	status, body = unixRequest(t, socket, http.MethodPost,
		"/v1/sessions/"+hello.SessionID+"/close", map[string]any{}, nil)
	if status != http.StatusOK {
		t.Fatalf("close session: %d %s", status, body)
	}
	second, err := engine.NewEvent(
		protocol.EventSource(appID, "semantic.event_state"),
		protocol.FeedbackEventType(protocol.FeedbackUnknown),
		"semantic.event_state",
		protocol.EventData{FeedbackType: protocol.FeedbackUnknown, ReasonCode: "NO_RULE_MATCH"},
	)
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	status, body = unixRequest(t, socket, http.MethodPost, "/v1/events", second, map[string]string{
		protocol.HeaderSession: hello.SessionID,
	})
	if status != http.StatusUnauthorized {
		t.Fatalf("a closed session should answer 401, got %d %s", status, body)
	}
	var refused struct {
		ReasonCode string `json:"reason_code"`
	}
	if err := json.Unmarshal(body, &refused); err != nil || refused.ReasonCode != "SESSION_UNKNOWN" {
		t.Fatalf("expected SESSION_UNKNOWN, got %s", body)
	}
}

func TestL26HelloRejectionsOverSocket(t *testing.T) {
	_, socket, stop := startDaemon(t)
	defer stop()

	cases := []struct {
		name    string
		request map[string]any
		status  int
		code    string
	}{
		{
			name: "protocol incompatible",
			request: map[string]any{
				"protocol_version": "99", "application_id": identity.NewID(),
				"installation_id": identity.NewID(), "process_id": identity.NewID(),
			},
			status: http.StatusUpgradeRequired, code: "PROTOCOL_INCOMPATIBLE",
		},
		{
			name: "unknown application",
			request: map[string]any{
				"protocol_version": protocol.Version, "application_id": identity.NewID(),
				"installation_id": identity.NewID(), "process_id": identity.NewID(),
			},
			status: http.StatusNotFound, code: "UNKNOWN_APPLICATION",
		},
		{
			name: "identity is not a uuid",
			request: map[string]any{
				"protocol_version": protocol.Version, "application_id": "semantic-service",
				"installation_id": "mac", "process_id": "1",
			},
			status: http.StatusBadRequest, code: "INVALID_IDENTITY",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := unixRequest(t, socket, http.MethodPost, "/v1/hello", tc.request, nil)
			if status != tc.status {
				t.Fatalf("status %d, expected %d: %s", status, tc.status, body)
			}
			var payload struct {
				Accepted          bool     `json:"accepted"`
				ReasonCode        string   `json:"reason_code"`
				SupportedVersions []string `json:"supported_versions"`
			}
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("decode: %v (%s)", err, body)
			}
			if payload.Accepted || payload.ReasonCode != tc.code {
				t.Fatalf("expected %s, got %s", tc.code, body)
			}
			if tc.code == "PROTOCOL_INCOMPATIBLE" && len(payload.SupportedVersions) == 0 {
				t.Fatalf("an incompatible protocol must list what is supported")
			}
		})
	}

	// Malformed JSON is refused, not crashed on.
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte("POST /v1/hello HTTP/1.1\r\nHost: lymph\r\nContent-Length: 7\r\nConnection: close\r\n\r\n{not i"))
	raw, _ := io.ReadAll(conn)
	if !bytes.Contains(raw, []byte("400")) {
		t.Fatalf("malformed hello should be a 400, got %s", raw)
	}
}

func TestL26VersionAndStatusAdvertiseProtocol(t *testing.T) {
	_, socket, stop := startDaemon(t)
	defer stop()

	status, body := unixRequest(t, socket, http.MethodGet, "/v1/version", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("version: %d %s", status, body)
	}
	var version struct {
		DaemonVersion    string   `json:"daemon_version"`
		ProtocolVersions []string `json:"protocol_versions"`
		ProtocolVersion  string   `json:"protocol_version"`
		StoreFormat      string   `json:"store_format"`
		Capabilities     []string `json:"capabilities"`
	}
	if err := json.Unmarshal(body, &version); err != nil {
		t.Fatalf("decode version: %v", err)
	}
	if version.DaemonVersion == "" || len(version.ProtocolVersions) == 0 || version.StoreFormat == "" {
		t.Fatalf("version endpoint must report daemon, protocol and store versions: %s", body)
	}
	// The protocol version is not the daemon version (mission section 9).
	if version.ProtocolVersion == version.DaemonVersion {
		t.Fatalf("protocol and daemon versions must be independent: %s", body)
	}

	status, body = unixRequest(t, socket, http.MethodGet, "/v1/status", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("status: %d %s", status, body)
	}
	var daemonStatus struct {
		ProtocolVersions []string       `json:"protocol_versions"`
		ServerVersion    string         `json:"server_version"`
		Sessions         map[string]int `json:"sessions"`
	}
	if err := json.Unmarshal(body, &daemonStatus); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if len(daemonStatus.ProtocolVersions) == 0 || daemonStatus.ServerVersion == "" {
		t.Fatalf("status must advertise the protocol and server version: %s", body)
	}
}

func TestL26SocketCollisionAndStaleSocket(t *testing.T) {
	root, socket, stop := startDaemon(t)
	defer stop()

	// A second daemon on the same socket must refuse to start, and must not
	// remove the first one's socket (mission section 69).
	second := httpapi.New(httpapi.Options{
		Engine:     engineAt(t, t.TempDir()),
		SocketPath: socket,
		Logger:     quiet(),
	})
	if err := second.Start(); err == nil {
		t.Fatalf("a second daemon must not take an active socket")
	}

	// The first daemon is still reachable.
	status, body := unixRequest(t, socket, http.MethodGet, "/v1/health", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("the original daemon should still answer: %d %s", status, body)
	}

	// A stale socket file (no listener) is replaced (mission section 70).
	stop()
	_ = root
	stale := filepath.Join(t.TempDir(), "stale.sock")
	listener, err := net.Listen("unix", stale)
	if err != nil {
		t.Fatalf("create stale socket: %v", err)
	}
	if unixListener, ok := listener.(*net.UnixListener); ok {
		unixListener.SetUnlinkOnClose(false)
	}
	listener.Close()
	if _, err := os.Stat(stale); err != nil {
		t.Fatalf("stale socket should still exist on disk: %v", err)
	}

	fresh := httpapi.New(httpapi.Options{
		Engine:     engineAt(t, t.TempDir()),
		SocketPath: stale,
		Logger:     quiet(),
	})
	if err := fresh.Start(); err != nil {
		t.Fatalf("a stale socket should be replaced, got: %v", err)
	}
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = fresh.Serve()
	}()
	defer func() {
		_ = fresh.Shutdown(context.Background())
		<-serveDone
	}()
	status, body = unixRequest(t, stale, http.MethodGet, "/v1/health", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("the replacement daemon should answer on the same path: %d %s", status, body)
	}
}

func TestL26SocketModeIsDeliberate(t *testing.T) {
	root := t.TempDir()
	e := engineAt(t, root)
	socket := filepath.Join(root, "moded.sock")

	srv := httpapi.New(httpapi.Options{Engine: e, SocketPath: socket, Logger: quiet(), SocketMode: 0o600})
	defer srv.Shutdown(context.Background())
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode %v, expected 0600", info.Mode().Perm())
	}
}

// TestL26PythonClientHandshake runs the bundled Python client against a real
// socket, proving the protocol is language-neutral (mission sections 49, 67,
// 127). It is skipped when Python is unavailable.
func TestL26PythonClientHandshake(t *testing.T) {
	python := pythonBinary()
	if python == "" {
		t.Skip("python3 is not available; the cross-language proof is skipped with this reason rather than silently omitted")
	}

	root, socket, stop := startDaemon(t)
	defer stop()

	e := engineAt(t, root)
	reg, err := e.RegisterApplication(context.Background(), engine.Manifest{
		Name: "python-app",
		Junctions: []engine.JunctionSpec{
			{Name: "semantic.event_state", Workflow: "semantic-rule-repair", ConfigFamily: "semantic_event_rules"},
		},
		ConfigFamilies: []engine.ConfigFamilySpec{{Name: "semantic_event_rules", SchemaRevision: "v1"}},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	script := `
import json, sys
sys.path.insert(0, "examples/python")
from lymph_client import LymphClient

client = LymphClient(
    socket_path=sys.argv[1],
    application_id=sys.argv[2],
    installation_id=sys.argv[3],
    client_name="python-cross-language",
)
hello = client.hello()
assert hello["accepted"], hello
assert hello["session_id"], hello
response = client.emit(
    junction="semantic.event_state",
    feedback_type="UNKNOWN",
    reason_code="NO_RULE_MATCH",
    payload={"text": "awarded from python"},
)
assert response["accepted"], response
print(json.dumps({"session": hello["session_id"], "event": response["event_id"], "issue": response.get("issue_id")}))
`
	cmd := execCommand(t, python, "-c", script, socket, reg.Application.ApplicationID, reg.DefaultInstallationID)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python client failed: %v\n%s", err, out)
	}
	var result struct {
		Session string `json:"session"`
		Event   string `json:"event"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(out), &result); err != nil {
		t.Fatalf("decode python output: %v (%s)", err, out)
	}

	event, err := e.DB().GetEvent(result.Event)
	if err != nil {
		t.Fatalf("the event the Python client sent is not in the store: %v", err)
	}
	session, err := e.DB().GetSession(result.Session)
	if err != nil {
		t.Fatalf("the session the Python client opened is not in the store: %v", err)
	}
	if session.ClientName != "python-cross-language" {
		t.Fatalf("session client name %q", session.ClientName)
	}
	if event.SessionID == "" && event.ProducerInstance == "" {
		t.Fatalf("event has neither session nor producer")
	}

	// The same daemon serves the Go SDK, so one protocol served both.
	goClient := client.New(client.Options{
		Socket:         socket,
		ApplicationID:  reg.Application.ApplicationID,
		InstallationID: reg.DefaultInstallationID,
		ClientName:     "go-cross-language",
	})
	if _, err := goClient.EmitFeedback(context.Background(), client.Feedback{
		Junction: "semantic.event_state", FeedbackType: "UNKNOWN",
		ReasonCode: "NO_RULE_MATCH", Payload: map[string]any{"text": "awarded from go"},
	}); err != nil {
		t.Fatalf("go client emit: %v", err)
	}
}
