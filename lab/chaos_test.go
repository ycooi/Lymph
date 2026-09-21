package lab

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/protocol"
	"github.com/ycooi/Lymph/pkg/client"
)

// The chaos tests drive a real lymphd process, because a process that is killed
// with SIGKILL cannot be simulated in-process: the point is that nothing in the
// daemon gets to run its own cleanup.

var (
	buildOnce   sync.Once
	builtBinary string
	buildErr    error
)

// lymphdBinary builds the daemon once per test binary run.
func lymphdBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "lymph-lab-bin")
		if err != nil {
			buildErr = err
			return
		}
		builtBinary = filepath.Join(dir, "lymphd")
		cmd := exec.Command("go", "build", "-o", builtBinary, "./cmd/lymphd")
		cmd.Dir = ".."
		cmd.Env = append(os.Environ(), "GOFLAGS="+envOr("GOFLAGS", "-mod=mod"), "GOSUMDB=off")
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("build lymphd: %v: %s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatalf("%v", buildErr)
	}
	return builtBinary
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// daemon is a running lymphd.
type daemon struct {
	root   string
	socket string
	cmd    *exec.Cmd
	log    *os.File
	t      *testing.T
}

// startDaemon runs lymphd on a store, optionally under a shell that applies
// process limits (used to inject write failures).
func startDaemon(t *testing.T, root, limitShell string) *daemon {
	t.Helper()
	binary := lymphdBinary(t)
	socket := filepath.Join(root, "lymph.sock")
	logPath := filepath.Join(t.TempDir(), "lymphd.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create daemon log: %v", err)
	}

	var cmd *exec.Cmd
	if limitShell == "" {
		cmd = exec.Command(binary, "--root", root, "--socket", socket, "--log-level", "warn")
	} else {
		cmd = exec.Command("bash", "-c",
			fmt.Sprintf("%s; exec %q --root %q --socket %q --log-level warn", limitShell, binary, root, socket))
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start lymphd: %v", err)
	}

	d := &daemon{root: root, socket: socket, cmd: cmd, log: logFile, t: t}
	t.Cleanup(d.stop)
	d.waitReady()
	return d
}

func (d *daemon) waitReady() {
	d.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("unix", d.socket, 250*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		if d.cmd.ProcessState != nil && d.cmd.ProcessState.Exited() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	d.dumpLog()
	d.t.Fatalf("lymphd never became ready on %s", d.socket)
}

func (d *daemon) dumpLog() {
	if d.log == nil {
		return
	}
	raw, err := os.ReadFile(d.log.Name())
	if err == nil && len(raw) > 0 {
		d.t.Logf("lymphd log:\n%s", raw)
	}
}

// kill9 terminates the daemon without giving it a chance to clean up.
func (d *daemon) kill9() {
	if d.cmd == nil || d.cmd.Process == nil {
		return
	}
	_ = d.cmd.Process.Signal(syscall.SIGKILL)
	_, _ = d.cmd.Process.Wait()
	d.cmd = nil
}

// stop shuts the daemon down politely.
func (d *daemon) stop() {
	if d.cmd != nil && d.cmd.Process != nil {
		_ = d.cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _, _ = d.cmd.Process.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = d.cmd.Process.Kill()
		}
		d.cmd = nil
	}
	if d.log != nil {
		d.log.Close()
		d.log = nil
	}
}

// client returns an SDK client bound to this daemon's socket.
func (d *daemon) client() *client.Client {
	return client.New(client.Options{Socket: d.socket, ProducerInstance: "lab", Timeout: 5 * time.Second})
}

// TestS10ChaosKill kills the daemon at random points during live traffic and
// checks the two acceptable outcomes from the test plan: the store comes back
// whole, and nothing that was acknowledged is missing.
func TestS10ChaosKill(t *testing.T) {
	const id = "S10_chaos"
	root := openStoreRoot(t, t.TempDir())

	d := startDaemon(t, root, "")
	cli := d.client()

	reg := registerViaClient(t, cli, "chaos-app")
	appID := reg.Application.ApplicationID
	junction := reg.JunctionIDs["semantic.event_state"]
	if junction == "" {
		t.Fatalf("chaos-app registered without a junction")
	}

	type ack struct {
		eventID string
	}
	var acknowledged []ack
	var mu sync.Mutex

	cycles := 6
	kills := 0
	workBatch := scaled(30, 120)

	for cycle := 0; cycle < cycles; cycle++ {
		// Background traffic: the daemon is killed while requests are in flight.
		stopTraffic := make(chan struct{})
		trafficDone := make(chan struct{})
		active := cli
		go func() {
			defer close(trafficDone)
			for i := 0; ; i++ {
				select {
				case <-stopTraffic:
					return
				default:
				}
				resp, err := emitViaClient(active, appID, junction, cycle*workBatch+i)
				if err != nil {
					// The daemon is gone; the client could not get an
					// acknowledgement, so this event must not be counted.
					continue
				}
				mu.Lock()
				acknowledged = append(acknowledged, ack{eventID: resp.EventID})
				mu.Unlock()
			}
		}()

		time.Sleep(time.Duration(20+cycle*5) * time.Millisecond)
		d.kill9()
		kills++
		close(stopTraffic)
		<-trafficDone

		if cycle == cycles-1 {
			break
		}
		d = startDaemon(t, root, "")
		cli = d.client()
	}

	// Restart one last time and let the store settle.
	d = startDaemon(t, root, "")
	cli = d.client()
	mu.Lock()
	acked := append([]ack(nil), acknowledged...)
	mu.Unlock()

	if len(acked) == 0 {
		Failf(t, id, "§10 chaos", "acknowledged events survive kill -9",
			"no event was acknowledged before the kills; the test proved nothing")
		return
	}

	// Every acknowledged event must be present, exactly once.
	missing := 0
	duplicated := 0
	for _, entry := range acked {
		var event struct {
			EventID string `json:"event_id"`
		}
		if err := cli.Get(context.Background(), "/v1/events/"+entry.eventID, &event); err != nil {
			missing++
		}
	}
	d.stop()

	e := openEngineAt(t, root)
	report, err := e.VerifyLedger()
	if err != nil {
		Failf(t, id, "§10 chaos", "acknowledged events survive kill -9",
			"ledger verification failed after %d kills: %v", kills, err)
		return
	}
	events, err := e.DB().CountEvents()
	if err != nil {
		t.Fatalf("count events: %v", err)
	}
	distinct := map[string]int{}
	eventList, err := e.DB().ListEvents(appID, 100000)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	for _, event := range eventList {
		distinct[event.EventID]++
	}
	for _, count := range distinct {
		if count > 1 {
			duplicated++
		}
	}

	if missing > 0 || duplicated > 0 {
		Failf(t, id, "§10 chaos", "acknowledged events survive kill -9",
			"%d acknowledged events missing, %d duplicated, after %d kills", missing, duplicated, kills)
		return
	}
	if events < len(acked) {
		Failf(t, id, "§10 chaos", "acknowledged events survive kill -9",
			"store holds %d events but %d were acknowledged", events, len(acked))
		return
	}

	// The projection is a rebuild away from the ledger, always.
	before := stateDigest(t, e)
	if _, err := e.Rebuild(); err != nil {
		Failf(t, id, "§10 chaos", "the store reopens whole", "rebuild failed: %v", err)
		return
	}
	after := stateDigest(t, e)
	if before != after {
		Failf(t, id, "§10 chaos", "the store reopens whole",
			"rebuild changed the state digest: %s vs %s", shorten(before), shorten(after))
		return
	}

	Passf(t, id, "§10 chaos", "kill -9 during live traffic",
		"%d SIGKILLs during in-flight appends; %d acknowledged events all present exactly once (%d canonical events, %d distinct ids); ledger verified with %d records (torn tail recovered: %d bytes); rebuild digest unchanged",
		kills, len(acked), events, len(distinct), report.Records, report.TornTailBytes)
}

// TestS11DeployKill records the deployment-recovery gap.
func TestS11DeployKill(t *testing.T) {
	Blockedf(t, "S11_deploy_kill", "§11 kill during deployment", "crash-safe deployment",
		"there is no deployment transaction to kill: no staging, no atomic rename, no install journal, so 'kill between stage/rename/ref-update' cannot be run")
}

// ---------- client helpers ----------

func registerViaClient(t *testing.T, c *client.Client, name string) engine.Registration {
	t.Helper()
	var reg engine.Registration
	err := c.Post(context.Background(), "/v1/applications", engine.Manifest{
		Name: name,
		Junctions: []engine.JunctionSpec{
			{Name: "semantic.event_state", Workflow: "semantic-rule-repair", ConfigFamily: "semantic_event_rules"},
		},
		ConfigFamilies: []engine.ConfigFamilySpec{
			{
				Name:           "semantic_event_rules",
				SchemaRevision: "semantic-rules-v3",
				Targets: []engine.TargetSpec{
					{Type: "SINGLE_FILE", Path: "/tmp/lymph-lab/" + name + "/rules.yaml", ReloadPolicy: "WATCH_FILE"},
				},
			},
		},
	}, &reg)
	if err != nil {
		t.Fatalf("register %s over socket: %v", name, err)
	}
	return reg
}

func emitViaClient(c *client.Client, appID, junction string, index int) (client.EmitResponse, error) {
	payload, err := json.Marshal(map[string]any{"text": fmt.Sprintf("chaos sample %d", index)})
	if err != nil {
		return client.EmitResponse{}, err
	}
	event, err := engine.NewEvent(
		"lymph://"+appID+"/"+junction,
		"lymph.feedback.unknown.v1",
		junction,
		protocol.EventData{
			FeedbackType: protocol.FeedbackUnknown,
			ReasonCode:   "UNKNOWN_EVENT_PHRASE",
			Payload:      payload,
		},
	)
	if err != nil {
		return client.EmitResponse{}, err
	}
	return c.EmitStrict(context.Background(), client.EmitRequest{Event: event})
}
