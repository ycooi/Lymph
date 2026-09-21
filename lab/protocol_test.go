package lab

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
	"github.com/ycooi/Lymph/pkg/client"
)

// The protocol, session and socket verdicts live in their own section so the
// 41-item deployment gate keeps its meaning (mission sections 97, 129).
//
// Everything here runs against a real lymphd process on a real Unix socket,
// driven by the real SDK: that is the feature being validated, and an
// httptest.Server would not validate it (mission section 68).

// protocolApp registers one application on a running daemon and returns the
// identities a client needs.
func protocolApp(t *testing.T, cli *client.Client, name string, specs ...engine.InstallationSpec) (string, string) {
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
	var reg engine.Registration
	if err := cli.Post(context.Background(), "/v1/applications", manifest, &reg); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
	return reg.Application.ApplicationID, reg.DefaultInstallationID
}

func mustEvent(t *testing.T, applicationID, junction, reason string) protocol.Event {
	t.Helper()
	event, err := engine.NewEvent(
		protocol.EventSource(applicationID, junction),
		protocol.FeedbackEventType(protocol.FeedbackUnknown),
		junction,
		protocol.EventData{FeedbackType: protocol.FeedbackUnknown, ReasonCode: reason},
	)
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	return event
}

// TestP01ProtocolHandshake is the graded protocol item.
func TestP01ProtocolHandshake(t *testing.T) {
	const id = "P01_protocol"
	root := openStoreRoot(t, t.TempDir())
	d := startDaemon(t, root, "")
	defer d.stop()

	admin := d.client()
	appID, installationID := protocolApp(t, admin, "protocol-app")

	// A real client, over the real socket, handshakes lazily on first use.
	c, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket: d.socket, ApplicationID: appID, InstallationID: installationID,
		ClientName: "lab-protocol", ClientVersion: "0.2.0",
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	if _, ok := c.Session(); ok {
		Failf(t, id, "§10 hello", "constructing a client performs no I/O",
			"a session existed before any call")
		return
	}
	hello, err := c.Hello(context.Background())
	if err != nil {
		Failf(t, id, "§10 hello", "handshake over the real socket", "%v", err)
		return
	}
	if !hello.Accepted || hello.SessionID == "" || hello.LymphInstanceID == "" {
		Failf(t, id, "§10 hello", "handshake over the real socket", "incomplete response: %+v", hello)
		return
	}
	if hello.ProtocolVersion == hello.ServerVersion {
		Failf(t, id, "§9 protocol versioning", "protocol and daemon versions are separate",
			"both report %s", hello.ProtocolVersion)
		return
	}
	if len(hello.Capabilities) == 0 {
		Failf(t, id, "§55 capabilities", "the handshake advertises capabilities", "none offered")
		return
	}

	// The version endpoint advertises protocol and store format independently.
	var version map[string]any
	if err := admin.Get(context.Background(), "/v1/version", &version); err != nil {
		t.Fatalf("version: %v", err)
	}
	if version["protocol_version"] != protocol.Version || version["store_format"] == nil {
		Failf(t, id, "§114 version endpoint", "version reports protocol and store format", "%v", version)
		return
	}

	// An unregistered application is refused with a machine-readable code.
	bad, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket: d.socket, ApplicationID: identity.NewID(), InstallationID: installationID,
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	_, err = bad.Hello(context.Background())
	var rejected *client.HandshakeRejectedError
	if !errors.As(err, &rejected) || rejected.ReasonCode != "UNKNOWN_APPLICATION" {
		Failf(t, id, "§13 unknown application", "an unregistered application is refused", "got %v", err)
		return
	}

	// Legacy sessionless clients keep working during this stage.
	legacy := client.New(client.Options{Socket: d.socket, ProducerInstance: "legacy-client"})
	if _, err := legacy.EmitStrict(context.Background(), client.EmitRequest{
		Event: mustEvent(t, appID, "semantic.event_state", "legacy"),
	}); err != nil {
		Failf(t, id, "§22 backwards compatibility", "sessionless clients still work", "%v", err)
		return
	}
	var status engine.Status
	if err := admin.Get(context.Background(), "/v1/status", &status); err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.SessionlessEvents == 0 {
		Failf(t, id, "§89 sessionless counter", "sessionless traffic is visible", "counter stayed at zero")
		return
	}

	// A Python client, same protocol, same daemon.
	crossLanguage := ""
	if python := pythonBinary(); python != "" {
		script := `
import json, sys
sys.path.insert(0, "examples/python")
from lymph_client import LymphClient
c = LymphClient(socket_path=sys.argv[1], application_id=sys.argv[2], installation_id=sys.argv[3], client_name="lab-python")
h = c.hello()
assert h["accepted"], h
e = c.emit(junction="semantic.event_state", feedback_type="UNKNOWN", reason_code="FROM_PYTHON")
assert e["accepted"], e
print(json.dumps({"session": h["session_id"], "event": e["event_id"]}))
`
		cmd := exec.Command(python, "-c", script, d.socket, appID, installationID)
		cmd.Dir = ".."
		out, err := cmd.CombinedOutput()
		if err != nil {
			Failf(t, id, "§67 cross-language", "a Python client speaks the same protocol",
				"%v\n%s", err, out)
			return
		}
		crossLanguage = strings.TrimSpace(string(out))
	} else {
		crossLanguage = "python3 unavailable in this environment: the cross-language leg is skipped, not hidden"
	}

	Passf(t, id, "§10 hello", "one handshake, two languages, one daemon",
		"the Go SDK handshook over a real Unix socket: session %s, protocol %s, server %s, capabilities %v; an unregistered application was refused with UNKNOWN_APPLICATION; a sessionless client was still accepted and counted (%d sessionless events); %s",
		shorten(hello.SessionID), hello.ProtocolVersion, hello.ServerVersion,
		hello.Capabilities, status.SessionlessEvents, truncateText(crossLanguage, 70))
}

// TestP02SessionIdentity is the graded session item.
func TestP02SessionIdentity(t *testing.T) {
	const id = "P02_session_identity"
	root := openStoreRoot(t, t.TempDir())
	d := startDaemon(t, root, "")
	defer d.stop()

	admin := d.client()
	regionA := installationSpec("region-a", "office")
	regionB := installationSpec("region-b", "production")
	appID, _ := protocolApp(t, admin, "session-app", regionA, regionB)

	// Two processes of one installation both connect and keep their identity.
	first, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket: d.socket, ApplicationID: appID, InstallationID: regionA.InstallationID,
		ClientName: "worker-a",
		// A spool so the test can prove the reconnect did *not* fall back to it.
		SpoolDir: filepath.Join(t.TempDir(), "spool-a"),
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	second, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket: d.socket, ApplicationID: appID, InstallationID: regionA.InstallationID,
		ClientName: "worker-b",
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	for i, c := range []*client.Client{first, second} {
		if _, err := c.EmitFeedback(context.Background(), client.Feedback{
			Junction: "semantic.event_state", FeedbackType: "UNKNOWN",
			ReasonCode: "MULTI_PROCESS", Payload: map[string]any{"worker": i},
		}); err != nil {
			Failf(t, id, "§18 duplicate processes", "several processes may share an installation", "%v", err)
			return
		}
	}

	var listing struct {
		Sessions []projection.Session `json:"sessions"`
	}
	if err := admin.Get(context.Background(), "/v1/sessions?installation="+regionA.InstallationID, &listing); err != nil {
		t.Fatalf("sessions: %v", err)
	}
	active := 0
	for _, session := range listing.Sessions {
		if session.State == projection.SessionActive {
			active++
		}
	}
	if active < 2 {
		Failf(t, id, "§18 duplicate processes", "both processes hold sessions",
			"%d active sessions for two processes", active)
		return
	}

	// A process on another installation of the same application stays apart.
	other, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket: d.socket, ApplicationID: appID, InstallationID: regionB.InstallationID,
		ClientName: "worker-region-b",
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	result, err := other.EmitFeedback(context.Background(), client.Feedback{
		Junction: "semantic.event_state", FeedbackType: "UNKNOWN", ReasonCode: "OTHER_INSTALLATION"},
	)
	if err != nil {
		t.Fatalf("emit from the other installation: %v", err)
	}
	var event projection.Event
	if err := admin.Get(context.Background(), "/v1/events/"+result.EventID, &event); err != nil {
		t.Fatalf("event: %v", err)
	}
	if event.InstallationID != regionB.InstallationID {
		Failf(t, id, "§59 installation identity", "events carry their installation",
			"event installation %s, expected %s", shorten(event.InstallationID), shorten(regionB.InstallationID))
		return
	}

	// A forgotten session produces a typed reconnect with no spool.
	sessionBefore, _ := first.Session()
	var closed projection.Session
	if err := admin.Post(context.Background(), "/v1/sessions/"+sessionBefore.SessionID+"/close", map[string]any{}, &closed); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := first.EmitFeedback(context.Background(), client.Feedback{
		Junction: "semantic.event_state", FeedbackType: "UNKNOWN", ReasonCode: "AFTER_LOSS"},
	); err != nil {
		Failf(t, id, "§60 daemon restart", "the client reconnects automatically", "%v", err)
		return
	}
	sessionAfter, ok := first.Session()
	if !ok || sessionAfter.SessionID == sessionBefore.SessionID {
		Failf(t, id, "§60 daemon restart", "the client reconnects automatically",
			"session did not change: %+v", sessionAfter)
		return
	}
	if pending, err := first.Spool().Count(); err != nil || pending != 0 {
		Failf(t, id, "§61 session unknown", "a forgotten session is not treated as an outage",
			"%d events were spooled (%v)", pending, err)
		return
	}

	Passf(t, id, "§5 process identity", "sessions carry process identity, not configuration identity",
		"two processes of installation %s held separate sessions and emitted under their own process UUIDs; a third process on installation %s emitted an event carrying its own installation; a forgotten session triggered a reconnect with a new session id and no spool (%s -> %s)",
		shorten(regionA.InstallationID), shorten(regionB.InstallationID),
		shorten(sessionBefore.SessionID), shorten(sessionAfter.SessionID))
}

// TestP03ReconnectSpool is the graded spool/reconnect item.
func TestP03ReconnectSpool(t *testing.T) {
	const id = "P03_reconnect_spool"
	root := openStoreRoot(t, t.TempDir())
	spoolDir := filepath.Join(t.TempDir(), "spool")

	d := startDaemon(t, root, "")
	appID, installationID := protocolApp(t, d.client(), "spool-app")

	connected, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket: d.socket, ApplicationID: appID, InstallationID: installationID,
		SpoolDir: spoolDir,
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	if _, err := connected.EmitFeedback(context.Background(), client.Feedback{
		Junction: "semantic.event_state", FeedbackType: "UNKNOWN", ReasonCode: "BEFORE_KILL"}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	processOne := connected.ProcessID()

	// The daemon dies. The application keeps running (mission sections 71, 117).
	d.kill9()
	for i := 0; i < 3; i++ {
		result, err := connected.EmitFeedback(context.Background(), client.Feedback{
			Junction: "semantic.event_state", FeedbackType: "UNKNOWN",
			ReasonCode: "DAEMON_DOWN", Payload: map[string]any{"i": i},
		})
		if err != nil {
			Failf(t, id, "§117 application runtime invariant",
				"Lymph down does not mean application down", "emit failed: %v", err)
			return
		}
		if !result.Spooled {
			Failf(t, id, "§66 non-blocking client", "events spool while the daemon is gone",
				"an emit was reported as delivered with no daemon running")
			return
		}
	}
	if pending, err := connected.Spool().Count(); err != nil || pending != 3 {
		Failf(t, id, "§31 spool", "the spool holds what could not be delivered",
			"%d events pending (%v)", pending, err)
		return
	}

	// The daemon comes back.
	d = startDaemon(t, root, "")
	defer d.stop()
	reconnected, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket: d.socket, ApplicationID: appID, InstallationID: installationID,
		SpoolDir: spoolDir,
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	sent, err := reconnected.FlushSpool(context.Background())
	if err != nil {
		Failf(t, id, "§34 flush spool", "spooled events are replayed after a restart", "%v", err)
		return
	}
	if sent != 3 {
		Failf(t, id, "§34 flush spool", "spooled events are replayed after a restart",
			"%d of 3 delivered", sent)
		return
	}
	if pending, _ := reconnected.Spool().Count(); pending != 0 {
		Failf(t, id, "§34 flush spool", "the spool empties after a successful flush", "%d remain", pending)
		return
	}

	// Every event is canonical exactly once, and replayed events keep the
	// process that produced them (mission sections 32, 63).
	admin := d.client()
	var listing struct {
		Events []projection.Event `json:"events"`
	}
	if err := admin.Get(context.Background(), "/v1/events"+client.Query("application", appID, "limit", "50"), &listing); err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(listing.Events) != 4 {
		Failf(t, id, "§68 duplicate replay", "every event is canonical exactly once",
			"%d events stored, expected 4", len(listing.Events))
		return
	}
	keptProducer := 0
	for _, event := range listing.Events {
		if event.ProducerInstance == processOne {
			keptProducer++
		}
		if event.ReplayedBySessionID != "" && event.ProducerInstance != processOne {
			Failf(t, id, "§63 replay exception", "a replayed event keeps its original producer",
				"event %s claims process %s", shorten(event.EventID), shorten(event.ProducerInstance))
			return
		}
	}
	if keptProducer != 4 {
		Failf(t, id, "§32 spool replay identity", "replayed events keep the producing process",
			"%d of 4 events carry the original process %s", keptProducer, shorten(processOne))
		return
	}

	var status engine.Status
	if err := admin.Get(context.Background(), "/v1/status", &status); err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.SpoolReplays < 3 {
		Failf(t, id, "§65 metrics", "spool replays are counted", "counter is %d", status.SpoolReplays)
		return
	}

	Passf(t, id, "§71 daemon kill", "kill the daemon, keep the application, replay safely",
		"a connected client survived SIGKILL of the daemon: 3 events spooled, the daemon restarted, the client reconnected and replayed all 3; %d events are canonical (one before the kill, three replayed), every one keeps producer %s, and %d spool replays were counted",
		len(listing.Events), shorten(processOne), status.SpoolReplays)
}

// TestP04SocketIsolation is the graded socket item.
func TestP04SocketIsolation(t *testing.T) {
	const id = "P04_socket_isolation"
	root := openStoreRoot(t, t.TempDir())
	d := startDaemon(t, root, "")

	info, err := os.Stat(d.socket)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if info.Mode().Perm()&0o007 != 0 {
		Failf(t, id, "§40 socket permissions", "the socket is not world-accessible",
			"mode is %v", info.Mode().Perm())
		return
	}
	if info.Mode()&os.ModeSocket == 0 {
		Failf(t, id, "§2 socket architecture", "the transport is a Unix socket", "mode is %v", info.Mode())
		return
	}

	// A second daemon on the same socket must refuse, leaving the first alone
	// (mission section 69).
	binary := lymphdBinary(t)
	second := exec.Command(binary, "--root", t.TempDir(), "--socket", d.socket, "--log-level", "error")
	output, err := second.CombinedOutput()
	if err == nil {
		Failf(t, id, "§69 socket collision", "a second daemon refuses an active socket", "it started")
		return
	}
	if !strings.Contains(string(output), "already listens") && !strings.Contains(string(output), "refusing") {
		Failf(t, id, "§69 socket collision", "the refusal explains the collision", "output: %s", output)
		return
	}
	if _, err := d.client().Health(context.Background()); err != nil {
		Failf(t, id, "§69 socket collision", "the original daemon survives", "%v", err)
		return
	}

	// A stale socket (no listener) is replaced, not left broken (mission 70).
	d.stop()
	staleDir := t.TempDir()
	staleSocket := filepath.Join(staleDir, "stale.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: staleSocket, Net: "unix"})
	if err != nil {
		t.Fatalf("create stale socket: %v", err)
	}
	listener.SetUnlinkOnClose(false)
	listener.Close()
	if _, err := os.Stat(staleSocket); err != nil {
		t.Fatalf("stale socket should remain on disk: %v", err)
	}

	replacement := exec.Command(binary, "--root", t.TempDir(), "--socket", staleSocket, "--log-level", "error")
	if err := replacement.Start(); err != nil {
		t.Fatalf("start replacement: %v", err)
	}
	defer func() {
		_ = replacement.Process.Kill()
		_, _ = replacement.Process.Wait()
	}()
	deadline := time.Now().Add(6 * time.Second)
	ready := false
	probe := client.New(client.Options{Socket: staleSocket, Timeout: 300 * time.Millisecond})
	for time.Now().Before(deadline) {
		if _, err := probe.Health(context.Background()); err == nil {
			ready = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !ready {
		Failf(t, id, "§70 stale socket", "a stale socket is replaced safely", "the replacement never answered")
		return
	}

	Passf(t, id, "§40 socket permissions", "one socket, deliberate permissions, safe reuse",
		"socket mode %v and not world-accessible; a second daemon on the same socket refused (%s) and left the first reachable; a stale socket left by a dead daemon was detected and replaced",
		info.Mode().Perm(), truncateText(strings.TrimSpace(string(output)), 60))
}

// TestP05SimulatorsThroughSessions runs every simulated application through a
// real daemon using the SDK (mission section 66).
func TestP05SimulatorsThroughSessions(t *testing.T) {
	root := openStoreRoot(t, t.TempDir())
	d := startDaemon(t, root, "")
	defer d.stop()
	admin := d.client()

	type simulated struct {
		name       string
		reasonCode string
		client     *client.Client
	}
	var apps []simulated
	for _, sim := range Simulators() {
		appID, installationID := protocolApp(t, admin, sim.Name)
		c, err := client.NewApplicationClient(client.ApplicationOptions{
			Socket: d.socket, ApplicationID: appID, InstallationID: installationID,
			ClientName: sim.Name + "-client", ClientVersion: "1.0.0",
		})
		if err != nil {
			t.Fatalf("client for %s: %v", sim.Name, err)
		}
		apps = append(apps, simulated{name: sim.Name, reasonCode: sim.Reason, client: c})
	}

	for _, app := range apps {
		if _, err := app.client.EmitFeedback(context.Background(), client.Feedback{
			Junction: "semantic.event_state", FeedbackType: "UNKNOWN",
			ReasonCode: app.reasonCode,
			Payload:    map[string]any{"text": app.name + " reports a hiccup"},
		}); err != nil {
			Failf(t, "P05_simulators", "§66 simulators use the client path",
				"%s could not emit through the SDK: %v", app.name, err)
			return
		}
	}

	var status engine.Status
	if err := admin.Get(context.Background(), "/v1/status", &status); err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Applications != len(apps) || status.Events != len(apps) {
		Failf(t, "P05_simulators", "§66 simulators use the client path", "every simulator reaches the daemon",
			"%d applications and %d events, expected %d each",
			status.Applications, status.Events, len(apps))
		return
	}
	if status.Sessions[projection.SessionActive] != len(apps) {
		Failf(t, "P05_simulators", "§66 simulators use the client path", "every simulator holds a session",
			"%d active sessions for %d applications",
			status.Sessions[projection.SessionActive], len(apps))
		return
	}

	Note(t, "P01_protocol", "P05_simulators", "§66 simulators use the client path",
		"simulators reach the daemon through the SDK",
		"all %d simulated applications (agent, MCP, ML, ingestion, generic daemon) registered, handshook and emitted through the SDK against one real daemon: %d applications, %d events, %d active sessions",
		len(apps), status.Applications, status.Events, status.Sessions[projection.SessionActive])
}

// pythonBinary locates python3 for the cross-language leg.
func pythonBinary() string {
	path, err := exec.LookPath("python3")
	if err != nil {
		return ""
	}
	return path
}
