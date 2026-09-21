package lab

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
)

// quietLogger keeps the lab output readable.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// openEngine starts an in-process daemon over a fresh store.
func openEngine(t *testing.T) *engine.Engine {
	t.Helper()
	e, err := engine.Open(engine.Options{Root: t.TempDir(), Logger: quietLogger()})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// openEngineAt starts an in-process daemon over an existing store.
func openEngineAt(t *testing.T, root string) *engine.Engine {
	t.Helper()
	e, err := engine.Open(engine.Options{Root: root, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("open engine at %s: %v", root, err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// App is one registered simulator inside a lab store.
type App struct {
	Sim      *Simulator
	Reg      engine.Registration
	FamilyID string
	Junction string
	Baseline projection.ConfigRevision
}

// ApplicationID is the UUID the simulator emits under.
func (a App) ApplicationID() string { return a.Reg.Application.ApplicationID }

// InstallationID is the deployment this app's configuration state belongs to.
// For a simulator that declares one installation, it is that installation; the
// empty fallback keeps older fixtures working with the application default.
func (a App) InstallationID() string {
	if len(a.Reg.InstallationIDs) == 1 {
		return a.Reg.InstallationIDs[0]
	}
	return a.Reg.DefaultInstallationID
}

// Installations lists the deployments declared at registration.
func (a App) Installations() []string { return a.Reg.InstallationIDs }

// Installation returns the n-th declared installation.
func (a App) Installation(n int) string {
	if n < 0 || n >= len(a.Reg.InstallationIDs) {
		return ""
	}
	return a.Reg.InstallationIDs[n]
}

// setupAppWith declares installations for a simulator and imports a baseline
// for each one, so that two deployments of the same application each have their
// own configuration history from the start.
func setupAppWith(t *testing.T, e *engine.Engine, sim *Simulator, specs []engine.InstallationSpec, defaultID string) App {
	t.Helper()
	ctx := context.Background()

	manifest := sim.Manifest()
	manifest.Installations = specs
	manifest.InstallationID = defaultID
	// A managed target belongs to one installation, so a manifest that declares
	// several deployments declares a target for each of them, on its own path
	// (mission sections 43, 44).
	for i := range manifest.ConfigFamilies {
		targets := make([]engine.TargetSpec, 0, len(specs))
		for _, spec := range specs {
			targets = append(targets, engine.TargetSpec{
				InstallationID: spec.InstallationID,
				Type:           "SINGLE_FILE",
				Path:           "/tmp/lymph-lab/" + sim.Name + "/" + spec.Name + "/" + sim.File,
				ReloadPolicy:   "WATCH_FILE",
				Atomicity:      "FULL",
			})
		}
		manifest.ConfigFamilies[i].Targets = targets
	}

	reg, err := e.RegisterApplication(ctx, manifest)
	if err != nil {
		t.Fatalf("register %s with installations: %v", sim.Name, err)
	}
	familyID := reg.FamilyIDs[sim.Family]
	junctionID := reg.JunctionIDs[sim.Junction]
	if familyID == "" || junctionID == "" {
		t.Fatalf("registration of %s did not return its family and junction", sim.Name)
	}

	app := App{Sim: sim, Reg: reg, FamilyID: familyID, Junction: junctionID}
	for _, installation := range reg.InstallationIDs {
		baseline, err := e.ImportBaseline(ctx, engine.BaselineRequest{
			Application:    reg.Application.ApplicationID,
			ConfigFamily:   familyID,
			InstallationID: installation,
			Bundle:         sim.Baseline,
			Author:         "lab",
			Label:          "r1",
		})
		if err != nil {
			t.Fatalf("baseline for %s/%s: %v", sim.Name, installation, err)
		}
		if app.Baseline.RevisionID == "" {
			app.Baseline = baseline
		}
	}
	return app
}

// installationSpec mints an installation identity for a declaration, the way a
// real operator would name the deployments they run.
func installationSpec(name, environment string) engine.InstallationSpec {
	return engine.InstallationSpec{InstallationID: identity.NewID(), Name: name, Environment: environment}
}

// activeRevisionFor reads the active ref of one installation.
func activeRevisionFor(t *testing.T, e *engine.Engine, app App, installation string) projection.ConfigRevision {
	t.Helper()
	ref, err := e.DB().GetRef(app.ApplicationID(), app.FamilyID, installation, engine.RefActive)
	if err != nil {
		t.Fatalf("active ref for %s/%s: %v", app.Sim.Name, installation, err)
	}
	rev, err := e.DB().GetRevision(ref.RevisionID)
	if err != nil {
		t.Fatalf("revision %s: %v", ref.RevisionID, err)
	}
	return rev
}

// setupApp registers a simulator and imports its baseline config as r1.
func setupApp(t *testing.T, e *engine.Engine, sim *Simulator) App {
	t.Helper()
	ctx := context.Background()

	reg, err := e.RegisterApplication(ctx, sim.Manifest())
	if err != nil {
		t.Fatalf("register %s: %v", sim.Name, err)
	}
	familyID := reg.FamilyIDs[sim.Family]
	if familyID == "" {
		t.Fatalf("registration of %s did not return config family %s", sim.Name, sim.Family)
	}
	junctionID := reg.JunctionIDs[sim.Junction]
	if junctionID == "" {
		t.Fatalf("registration of %s did not return junction %s", sim.Name, sim.Junction)
	}

	baseline, err := e.ImportBaseline(ctx, engine.BaselineRequest{
		Application:  reg.Application.ApplicationID,
		ConfigFamily: familyID,
		Bundle:       sim.Baseline,
		Author:       "lab",
		Label:        "r1",
	})
	if err != nil {
		t.Fatalf("baseline for %s: %v", sim.Name, err)
	}

	return App{Sim: sim, Reg: reg, FamilyID: familyID, Junction: junctionID, Baseline: baseline}
}

// emit sends one feedback event for an app.
func emit(t *testing.T, e *engine.Engine, app App, feedback protocol.FeedbackType, reason string, payload map[string]any, producer, configRevision string) engine.EmitResponse {
	t.Helper()
	return emitAt(t, e, app, feedback, reason, payload, producer, configRevision, time.Now().UTC())
}

// emitAt sends one feedback event with an explicit event time, so the lab can
// reproduce clock skew.
func emitAt(t *testing.T, e *engine.Engine, app App, feedback protocol.FeedbackType, reason string, payload map[string]any, producer, configRevision string, at time.Time) engine.EmitResponse {
	t.Helper()
	return emitRaw(t, e, app.ApplicationID(), app.Sim.Junction, feedback, reason, payload, producer, configRevision, at, "")
}

// emitRaw is the lowest-level emitter used by the tests, and lets a junction
// supply its own fingerprint.
func emitRaw(t *testing.T, e *engine.Engine, applicationID, junction string, feedback protocol.FeedbackType, reason string, payload map[string]any, producer, configRevision string, at time.Time, fingerprint string) engine.EmitResponse {
	t.Helper()
	return emitFull(t, e, applicationID, junction, feedback, reason, payload, producer, configRevision, at, fingerprint, "")
}

// emitFull is emitRaw plus the installation identity (design section 79).
func emitFull(t *testing.T, e *engine.Engine, applicationID, junction string, feedback protocol.FeedbackType, reason string, payload map[string]any, producer, configRevision string, at time.Time, fingerprint, installation string) engine.EmitResponse {
	t.Helper()
	event, err := engine.NewEvent(
		protocol.EventSource(applicationID, junction),
		protocol.FeedbackEventType(feedback),
		junction,
		protocol.EventData{
			FeedbackType:   feedback,
			ReasonCode:     reason,
			ConfigRevision: configRevision,
			Payload:        mustJSON(t, payload),
		},
	)
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	event.Time = at
	resp, err := e.Emit(context.Background(), engine.EmitRequest{
		Event:            event,
		ProducerInstance: producer,
		InstallationID:   installation,
		Fingerprint:      fingerprint,
	})
	if err != nil {
		t.Fatalf("emit %s/%s: %v", feedback, reason, err)
	}
	return resp
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	if value == nil {
		return []byte("null")
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return raw
}

// stateDigest is a deterministic fingerprint of the whole logical state.
//
// It is the comparison used by the rebuild and disaster-recovery gates: same
// digest before and after means the projection is genuinely derived from the
// ledger and nothing was lost. The meta table is excluded on purpose — it
// records when a rebuild happened.
func stateDigest(t *testing.T, e *engine.Engine) string {
	t.Helper()
	details := stateDigestDetail(t, e)
	digest := sha256.New()
	names := make([]string, 0, len(details))
	for name := range details {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(digest, "%s=%s\n", name, details[name])
	}
	return "sha256:" + hex.EncodeToString(digest.Sum(nil))
}

// stateDigestDetail returns a per-table digest, so a mismatch can be localised
// instead of merely reported.
func stateDigestDetail(t *testing.T, e *engine.Engine) map[string]string {
	t.Helper()
	tables := []struct{ name, query string }{
		{"applications", `SELECT * FROM applications ORDER BY application_id`},
		{"app_registrations", `SELECT registration_id, application_id, revision, content_hash, ledger_sequence, created_at FROM app_registrations ORDER BY registration_id`},
		{"junctions", `SELECT * FROM junctions ORDER BY junction_id`},
		{"config_families", `SELECT * FROM config_families ORDER BY config_family_id`},
		{"managed_targets", `SELECT * FROM managed_targets ORDER BY target_id`},
		{"path_ownership", `SELECT * FROM path_ownership ORDER BY path`},
		{"events", `SELECT * FROM events ORDER BY event_id`},
		{"issues", `SELECT * FROM issues ORDER BY issue_id`},
		{"issue_occurrences", `SELECT * FROM issue_occurrences ORDER BY issue_id, event_id`},
		{"config_revisions", `SELECT * FROM config_revisions ORDER BY revision_id`},
		{"config_revision_blobs", `SELECT * FROM config_revision_blobs ORDER BY revision_id, path`},
		{"config_refs", `SELECT * FROM config_refs ORDER BY application_id, config_family_id, name`},
		{"config_reflog", `SELECT log_id, application_id, config_family_id, ref_name, old_revision_id, new_revision_id, reason, issue_ids, candidate_id, ledger_sequence, created_at FROM config_reflog ORDER BY log_id`},
		{"candidates", `SELECT candidate_id, application_id, config_family_id, base_revision_id, root_tree_hash, state, issue_ids, work_item_id, created_at FROM candidates ORDER BY candidate_id`},
		{"approvals", `SELECT * FROM approvals ORDER BY approval_id`},
		{"work_items", `SELECT work_id, workflow_type, application_id, junction_id, config_family_id, base_revision_id, issue_ids, state, attempts, created_at FROM work_items ORDER BY work_id`},
		{"validations", `SELECT * FROM validations ORDER BY validation_id`},
		{"audit_events", `SELECT audit_id, action, actor, application_id, subject_kind, subject_id, ledger_sequence, created_at FROM audit_events ORDER BY audit_id`},
		{"ledger_index", `SELECT sequence, kind, object_id, record_hash, created_at FROM ledger_index ORDER BY sequence`},
	}

	out := make(map[string]string, len(tables))
	for _, table := range tables {
		digest := sha256.New()
		if err := digestTable(e.DB().SQL(), digest, table.name, table.query); err != nil {
			t.Fatalf("digest %s: %v", table.name, err)
		}
		out[table.name] = hex.EncodeToString(digest.Sum(nil))[:16]
	}
	return out
}

func digestTable(db *sql.DB, w io.Writer, name, query string) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "table:%s(%s)\n", name, strings.Join(cols, ",")); err != nil {
		return err
	}

	for rows.Next() {
		values := make([]sql.RawBytes, len(cols))
		pointers := make([]any, len(cols))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return err
		}
		parts := make([]string, len(values))
		for i, value := range values {
			if value == nil {
				parts[i] = "<null>"
				continue
			}
			parts[i] = string(value)
		}
		if _, err := fmt.Fprintln(w, strings.Join(parts, "|")); err != nil {
			return err
		}
	}
	return rows.Err()
}

// issueCounts returns occurrence counts per fingerprint.
func issueCounts(t *testing.T, e *engine.Engine) map[string]int {
	t.Helper()
	issues, err := e.DB().ListIssues("", 10000, "count")
	if err != nil {
		t.Fatalf("list issues: %v", err)
	}
	out := map[string]int{}
	for _, issue := range issues {
		out[issue.Fingerprint] = issue.OccurrenceCount
	}
	return out
}

// activeRevision returns the revision the active ref points at.
func activeRevision(t *testing.T, e *engine.Engine, app App) projection.ConfigRevision {
	t.Helper()
	ref, err := e.DB().GetRef(app.ApplicationID(), app.FamilyID, app.InstallationID(), engine.RefActive)
	if err != nil {
		t.Fatalf("active ref for %s: %v", app.Sim.Name, err)
	}
	rev, err := e.DB().GetRevision(ref.RevisionID)
	if err != nil {
		t.Fatalf("revision %s: %v", ref.RevisionID, err)
	}
	return rev
}

// bundleAt reads a revision's stored bytes back out of the object store.
func bundleAt(t *testing.T, e *engine.Engine, rev projection.ConfigRevision) objectstore.Bundle {
	t.Helper()
	tree, err := e.Objects().GetTree(rev.RootTreeHash)
	if err != nil {
		t.Fatalf("tree %s: %v", rev.RootTreeHash, err)
	}
	bundle := objectstore.Bundle{}
	for _, entry := range tree.Entries {
		data, err := e.Objects().Get(entry.Hash)
		if err != nil {
			t.Fatalf("blob %s: %v", entry.Hash, err)
		}
		bundle[entry.Path] = data
	}
	return bundle
}

// openStoreRoot creates an empty store directory for daemon-process tests.
func openStoreRoot(t *testing.T, root string) string {
	t.Helper()
	for _, dir := range []string{"", "spool/incoming", "staging/deployments", "snapshots", "locks"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o750); err != nil {
			t.Fatalf("create store dirs: %v", err)
		}
	}
	return root
}

// newAppUUID gives a test an application identity it can name before the
// application ever reaches the daemon.
func newAppUUID() string { return identity.NewID() }

// sortedKeysOf renders a set of strings deterministically for evidence lines.
func sortedKeysOf(set map[string]bool) string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return strings.Join(keys, "+")
}
