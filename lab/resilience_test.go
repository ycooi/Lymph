package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/ledger"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
	"github.com/ycooi/Lymph/pkg/client"
)

// TestS12StaleCandidate is the hard failure the plan names: a candidate built
// on r10 must never overwrite r11.
func TestS12StaleCandidate(t *testing.T) {
	const id = "S12_stale"
	ctx := context.Background()
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	var issueIDs []string
	for i := 0; i < 6; i++ {
		resp := emit(t, e, app, sim.Feedback, sim.Reason,
			map[string]any{"text": fmt.Sprintf("awarded variant %d", i)}, "svc", app.Baseline.RevisionID)
		if i < 2 {
			issueIDs = append(issueIDs, resp.IssueID)
		}
	}

	makeCandidate := func(name string, phrase string, issueID string) projection.Candidate {
		t.Helper()
		work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{issueID}})
		if err != nil {
			t.Fatalf("queue %s: %v", name, err)
		}
		candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
			Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
			Bundle: objectstore.Bundle{"rules.yaml": bundle("# semantic rules", semanticDoc{Version: 2, Rules: []SemanticRule{
				{ID: "a", Phrase: phrase, State: "AWARDED"},
			}})},
			IssueIDs: []string{issueID}, WorkItemID: work.WorkID, Explanation: name,
		})
		if err != nil {
			t.Fatalf("candidate %s: %v", name, err)
		}
		for _, kind := range []string{"structural", "replay", "regression"} {
			if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
				CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS", SuiteID: "semantic." + kind,
			}); err != nil {
				t.Fatalf("%s validation: %v", name, err)
			}
		}
		if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
			CandidateID: candidate.CandidateID, Approver: "operator",
		}); err != nil {
			t.Fatalf("approve %s: %v", name, err)
		}
		return candidate
	}

	candidateA := makeCandidate("A", "purchased", issueIDs[0])
	candidateB := makeCandidate("B", "sold", issueIDs[1])
	if candidateA.BaseRevisionID != app.Baseline.RevisionID || candidateB.BaseRevisionID != app.Baseline.RevisionID {
		Failf(t, id, "§12 stale candidates", "both candidates branch from r1",
			"A base %s, B base %s", candidateA.BaseRevisionID, candidateB.BaseRevisionID)
		return
	}

	promotion, err := e.PromoteCandidate(ctx, candidateB.CandidateID, "lab")
	if err != nil {
		t.Fatalf("promote B: %v", err)
	}
	if promotion.Revision.Sequence != 2 {
		Failf(t, id, "§12 stale candidates", "B becomes r2", "promotion produced r%d", promotion.Revision.Sequence)
		return
	}

	if _, err := e.PromoteCandidate(ctx, candidateA.CandidateID, "lab"); !errors.Is(err, projection.ErrStaleCandidate) {
		Failf(t, id, "§12 stale candidates", "A is refused with STALE_CANDIDATE",
			"promoting a candidate based on r1 after r2 was active returned %v", err)
		return
	}
	stale, err := e.DB().GetCandidate(candidateA.CandidateID)
	if err != nil {
		t.Fatalf("reload A: %v", err)
	}
	if stale.State != string(engine.CandidateStale) {
		Failf(t, id, "§12 stale candidates", "A is preserved as STALE",
			"A is %s", stale.State)
		return
	}
	active := activeRevision(t, e, app)
	if active.RevisionID != promotion.Revision.RevisionID {
		Failf(t, id, "§12 stale candidates", "the newer revision survives",
			"active is %s, expected %s", active.RevisionID, promotion.Revision.RevisionID)
		return
	}
	reflog, err := e.DB().ListReflog(app.ApplicationID(), app.FamilyID, app.InstallationID(), 50)
	if err != nil {
		t.Fatalf("reflog: %v", err)
	}
	activeMoves := 0
	for _, entry := range reflog {
		if entry.RefName == engine.RefActive {
			activeMoves++
		}
	}
	if activeMoves != 2 { // baseline import + B's promotion
		Failf(t, id, "§12 stale candidates", "the refused promotion leaves no trace in refs",
			"active ref moved %d times", activeMoves)
		return
	}

	Passf(t, id, "§12 stale candidates", "STALE_CANDIDATE is enforced",
		"A and B both based on r1; B promoted to r2; A refused (STALE_CANDIDATE) and kept as STALE; active still r2; exactly %d active ref moves recorded",
		activeMoves)
}

// TestS13ConcurrentPromotion races twenty promotions and expects one winner.
func TestS13ConcurrentPromotion(t *testing.T) {
	const id = "S13_concurrent"
	ctx := context.Background()
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	var issueIDs []string
	for i := 0; i < 21; i++ {
		resp := emit(t, e, app, sim.Feedback, sim.Reason,
			map[string]any{"text": fmt.Sprintf("awarded variant %d", i)}, "svc", app.Baseline.RevisionID)
		issueIDs = append(issueIDs, resp.IssueID)
	}

	const racers = 20
	candidates := make([]projection.Candidate, 0, racers)
	for i := 0; i < racers; i++ {
		work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{issueIDs[i]}})
		if err != nil {
			t.Fatalf("queue %d: %v", i, err)
		}
		candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
			Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
			Bundle: objectstore.Bundle{"rules.yaml": bundle("# semantic rules", semanticDoc{Version: 2, Rules: []SemanticRule{
				{ID: "a", Phrase: "awarded", State: "AWARDED"},
				{ID: fmt.Sprintf("v%d", i), Phrase: fmt.Sprintf("awarded variant %d", i), State: "AWARDED"},
			}})},
			IssueIDs: []string{issueIDs[i]}, WorkItemID: work.WorkID, Explanation: fmt.Sprintf("racer %d", i),
		})
		if err != nil {
			t.Fatalf("candidate %d: %v", i, err)
		}
		for _, kind := range []string{"structural", "replay", "regression"} {
			if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
				CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS", SuiteID: "semantic." + kind,
			}); err != nil {
				t.Fatalf("validation %d: %v", i, err)
			}
		}
		if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
			CandidateID: candidate.CandidateID, Approver: "lab",
		}); err != nil {
			t.Fatalf("approve %d: %v", i, err)
		}
		candidates = append(candidates, candidate)
	}

	type outcome struct {
		index    int
		revision string
		err      error
	}
	results := make(chan outcome, racers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, candidate := range candidates {
		wg.Add(1)
		go func(index int, candidateID string) {
			defer wg.Done()
			<-start
			promotion, err := e.PromoteCandidate(ctx, candidateID, "lab")
			results <- outcome{index: index, revision: promotion.Revision.RevisionID, err: err}
		}(i, candidate.CandidateID)
	}
	close(start)
	wg.Wait()
	close(results)

	winners, stale, other := 0, 0, 0
	var winnerRevision string
	for result := range results {
		switch {
		case result.err == nil:
			winners++
			winnerRevision = result.revision
		case errors.Is(result.err, projection.ErrStaleCandidate):
			stale++
		default:
			other++
			t.Logf("racer %d failed with %v", result.index, result.err)
		}
	}

	if winners != 1 {
		Failf(t, id, "§13 concurrent promotion", "exactly one promotion wins",
			"%d promotions succeeded, %d went stale, %d errored", winners, stale, other)
		return
	}
	active, err := e.DB().GetRef(app.ApplicationID(), app.FamilyID, app.InstallationID(), engine.RefActive)
	if err != nil {
		t.Fatalf("active ref: %v", err)
	}
	if active.RevisionID != winnerRevision {
		Failf(t, id, "§13 concurrent promotion", "the ref holds the winner",
			"active is %s, winner was %s", active.RevisionID, winnerRevision)
		return
	}
	reflog, err := e.DB().ListReflog(app.ApplicationID(), app.FamilyID, app.InstallationID(), 100)
	if err != nil {
		t.Fatalf("reflog: %v", err)
	}
	activeMoves := 0
	for _, entry := range reflog {
		if entry.RefName == engine.RefActive {
			activeMoves++
		}
	}
	if activeMoves != 2 {
		Failf(t, id, "§13 concurrent promotion", "no lost updates in the reflog",
			"active ref moved %d times, expected 2 (baseline + one winner)", activeMoves)
		return
	}
	revisions, err := e.DB().ListRevisions(app.FamilyID, 100)
	if err != nil {
		t.Fatalf("list revisions: %v", err)
	}
	sequences := map[int]bool{}
	for _, revision := range revisions {
		if sequences[revision.Sequence] {
			Failf(t, id, "§13 concurrent promotion", "no duplicate revision sequences",
				"sequence r%d was issued twice", revision.Sequence)
			return
		}
		sequences[revision.Sequence] = true
	}

	Passf(t, id, "§13 concurrent promotion", "20 racers, one winner",
		"%d goroutines promoted concurrently: %d succeeded, %d refused as STALE, %d other errors; active ref = winner; %d active moves in the reflog; %d revisions with distinct sequences",
		racers, winners, stale, other, activeMoves, len(revisions))
}

// TestS18Unavailable runs all five simulators with no daemon at all.
func TestS18Unavailable(t *testing.T) {
	const id = "S18_unavailable"
	root := openStoreRoot(t, t.TempDir())
	deadSocket := filepath.Join(root, "not-running.sock")
	spoolRoot := filepath.Join(t.TempDir(), "spool")

	type spooled struct {
		sim    *Simulator
		appID  string
		client *client.Client
		count  int
	}
	const perApp = 10
	var apps []spooled

	for _, sim := range Simulators() {
		appID := newAppUUID()
		spoolDir := filepath.Join(spoolRoot, sim.Name)
		cli := client.New(client.Options{
			Socket:           deadSocket,
			SpoolDir:         spoolDir,
			ProducerInstance: sim.Name,
			InstallationID:   "offline-test",
			Timeout:          500 * time.Millisecond,
		})
		for i := 0; i < perApp; i++ {
			payload, err := json.Marshal(map[string]any{"text": fmt.Sprintf("%s sample %d", sim.Name, i)})
			if err != nil {
				t.Fatalf("payload: %v", err)
			}
			event, err := engine.NewEvent(
				protocol.EventSource(appID, sim.Junction),
				protocol.FeedbackEventType(sim.Feedback),
				sim.Junction,
				protocol.EventData{FeedbackType: sim.Feedback, ReasonCode: sim.Reason, Payload: payload},
			)
			if err != nil {
				t.Fatalf("event: %v", err)
			}
			resp, err := cli.Emit(context.Background(), client.EmitRequest{Event: event})
			if err != nil {
				Failf(t, id, "§18 Lymph unavailable", "applications keep running without Lymph",
					"%s could not emit while the daemon was down: %v", sim.Name, err)
				return
			}
			if !resp.Spooled {
				Failf(t, id, "§18 Lymph unavailable", "applications keep running without Lymph",
					"%s reported a delivered event with no daemon running", sim.Name)
				return
			}
		}
		count, err := cli.Spool().Count()
		if err != nil {
			t.Fatalf("spool count: %v", err)
		}
		apps = append(apps, spooled{sim: sim, appID: appID, client: cli, count: count})
	}

	// Lymph comes back.
	d := startDaemon(t, root, "")
	live := d.client()
	for _, entry := range apps {
		manifest := entry.sim.Manifest()
		manifest.ApplicationID = entry.appID
		if err := live.Post(context.Background(), "/v1/applications", manifest, nil); err != nil {
			t.Fatalf("register %s after restart: %v", entry.sim.Name, err)
		}
	}

	delivered := 0
	for _, entry := range apps {
		configured := client.New(client.Options{
			Socket:           d.socket,
			SpoolDir:         filepath.Join(spoolRoot, entry.sim.Name),
			ProducerInstance: entry.sim.Name,
			InstallationID:   "offline-test",
		})
		sent, err := configured.FlushSpool(context.Background())
		if err != nil {
			Failf(t, id, "§18 Lymph unavailable", "spooled events are delivered after restart",
				"%s flush failed: %v", entry.sim.Name, err)
			return
		}
		delivered += sent
		if pending, err := configured.Spool().Count(); err != nil || pending != 0 {
			Failf(t, id, "§18 Lymph unavailable", "spooled events are delivered after restart",
				"%s still has %d pending (err %v)", entry.sim.Name, pending, err)
			return
		}
	}
	d.stop()

	e := openEngineAt(t, root)
	total, err := e.DB().CountEvents()
	if err != nil {
		t.Fatalf("count events: %v", err)
	}
	if total != perApp*len(apps) {
		Failf(t, id, "§18 Lymph unavailable", "no duplicate canonical events",
			"%d events delivered but %d stored", perApp*len(apps), total)
		return
	}
	issues, err := e.DB().ListIssues("", 100, "count")
	if err != nil {
		t.Fatalf("list issues: %v", err)
	}
	byApp := map[string]int{}
	for _, issue := range issues {
		byApp[issue.ApplicationID] += issue.OccurrenceCount
	}
	if len(byApp) != len(apps) {
		Failf(t, id, "§18 Lymph unavailable", "each application's spool lands in its own history",
			"issues belong to %d applications, expected %d", len(byApp), len(apps))
		return
	}

	Passf(t, id, "§18 Lymph unavailable", "five applications, no daemon, no failures",
		"%d applications spooled %d events each with no daemon running; after restart all %d were delivered, zero duplicates, and each landed in its own application's history",
		len(apps), perApp, delivered)
}

// TestS19WorkerDeath covers lease expiry and the late worker.
func TestS19WorkerDeath(t *testing.T) {
	const id = "S19_worker_death"
	ctx := context.Background()
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	resp := emit(t, e, app, sim.Feedback, sim.Reason, map[string]any{"text": "awarded"}, "svc", app.Baseline.RevisionID)
	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{resp.IssueID}})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}

	first, err := e.ClaimWork(ctx, engine.ClaimRequest{Worker: "worker-A", TTL: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("claim by A: %v", err)
	}
	if first.Attempts != 1 {
		Failf(t, id, "§19 worker dies", "attempts are counted", "first lease has %d attempts", first.Attempts)
		return
	}

	// A dies without renewing.
	time.Sleep(40 * time.Millisecond)
	released, err := e.SweepLeases(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if released != 1 {
		Failf(t, id, "§19 worker dies", "an expired lease returns to the queue",
			"sweep released %d leases", released)
		return
	}
	requeued, err := e.DB().GetWorkItem(work.WorkID)
	if err != nil {
		t.Fatalf("work item: %v", err)
	}
	if requeued.State != string(engine.WorkQueued) {
		Failf(t, id, "§19 worker dies", "an expired lease returns to the queue",
			"state after expiry is %s", requeued.State)
		return
	}

	second, err := e.ClaimWork(ctx, engine.ClaimRequest{Worker: "worker-B", TTL: time.Minute})
	if err != nil {
		t.Fatalf("claim by B: %v", err)
	}
	if second.WorkID != work.WorkID || second.Attempts != 2 {
		Failf(t, id, "§19 worker dies", "another worker may take over",
			"second lease is %s attempt %d", shorten(second.WorkID), second.Attempts)
		return
	}

	// A comes back late and must not be able to finish B's attempt.
	if _, err := e.CompleteWork(ctx, engine.CompleteWorkRequest{
		WorkID: work.WorkID, Worker: "worker-A", State: engine.WorkReturned, Result: "{}",
	}); err == nil {
		Failf(t, id, "§19 worker dies", "the late worker cannot finish someone else's attempt",
			"worker-A completed a work item leased to worker-B")
		return
	}

	// B finishes it properly.
	if _, err := e.CompleteWork(ctx, engine.CompleteWorkRequest{
		WorkID: work.WorkID, Worker: "worker-B", State: engine.WorkRunning, Result: "{}",
	}); err != nil {
		t.Fatalf("B starts: %v", err)
	}
	done, err := e.CompleteWork(ctx, engine.CompleteWorkRequest{
		WorkID: work.WorkID, Worker: "worker-B", State: engine.WorkReturned, Result: `{"candidate":"x"}`,
	})
	if err != nil {
		t.Fatalf("B completes: %v", err)
	}
	if done.State != string(engine.WorkReturned) {
		Failf(t, id, "§19 worker dies", "the reassigned attempt completes", "final state %s", done.State)
		return
	}

	// Attempt history is preserved.
	history := attemptTrail(t, e, work.WorkID)
	if len(history) < 4 {
		Failf(t, id, "§19 worker dies", "attempt history is preserved",
			"only %d ledger records describe the work item", len(history))
		return
	}

	Passf(t, id, "§19 worker dies", "lease expiry and the late worker",
		"worker-A leased attempt 1 and died; sweep requeued it; worker-B leased attempt 2; worker-A's late return was refused; worker-B completed it; %d ledger records describe the history",
		len(history))
}

// TestS20BadWorkerOutput walks the four rejection cases in order.
func TestS20BadWorkerOutput(t *testing.T) {
	const id = "S20_bad_output"
	ctx := context.Background()
	e := openEngine(t)
	sim := IngestSim()
	app := setupApp(t, e, sim)

	var issueID string
	for i := 0; i < 5; i++ {
		resp := emit(t, e, app, sim.Feedback, sim.Reason,
			map[string]any{"table": "cru", "columns": []string{"price", "volume"}}, "ingest", app.Baseline.RevisionID)
		issueID = resp.IssueID
	}
	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{issueID}})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}

	candidatesBefore := countCandidates(t, e)

	// 1. invalid YAML is refused before a candidate exists at all.
	if _, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
		Bundle:   objectstore.Bundle{"config.yaml": []byte("aliases: {price: price_usd\n  bad indent: [")},
		IssueIDs: []string{issueID}, WorkItemID: work.WorkID, Explanation: "invalid yaml",
	}); err == nil {
		Failf(t, id, "§20 bad worker output", "invalid YAML never becomes a candidate",
			"an unparseable bundle was accepted")
		return
	} else if !errors.Is(err, engine.ErrStructuralInvalid) {
		Failf(t, id, "§20 bad worker output", "invalid YAML never becomes a candidate",
			"rejected for the wrong reason: %v", err)
		return
	}
	if after := countCandidates(t, e); after != candidatesBefore {
		Failf(t, id, "§20 bad worker output", "invalid YAML never becomes a candidate",
			"candidate count moved from %d to %d", candidatesBefore, after)
		return
	}

	// 2. valid YAML, missing a mandatory field: parses, but fails its own tests.
	missingField := objectstore.Bundle{"config.yaml": bundle("# mapping", ingestMapping{
		Aliases:  map[string]string{"price_usd": "price_usd"},
		Required: []string{},
	})}
	candidate2, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID, Bundle: missingField,
		IssueIDs: []string{issueID}, WorkItemID: work.WorkID, Explanation: "missing mandatory field",
	})
	if err != nil {
		t.Fatalf("candidate 2 should parse: %v", err)
	}
	if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
		CandidateID: candidate2.CandidateID, Kind: "structural", Result: "PASS", SuiteID: "ingest.validate",
	}); err != nil {
		t.Fatalf("structural: %v", err)
	}
	rejected, err := e.RecordValidation(ctx, engine.ValidationRequest{
		CandidateID: candidate2.CandidateID, Kind: "replay", Result: "FAIL", SuiteID: "ingest.replay",
		Metrics: `{"fixtures_failed":2}`,
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if rejected.State != string(engine.CandidateRejected) {
		Failf(t, id, "§20 bad worker output", "a missing mandatory field is rejected",
			"candidate state is %s", rejected.State)
		return
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: candidate2.CandidateID, Approver: "lab",
	}); err == nil {
		Failf(t, id, "§20 bad worker output", "a rejected candidate cannot be approved",
			"approval succeeded on a REJECTED candidate")
		return
	}

	// 3. valid schema, regression test fails.
	regressionBundle := objectstore.Bundle{"config.yaml": bundle("# mapping", ingestMapping{
		Aliases: map[string]string{
			"price_usd": "price_usd", "quantity_mt": "quantity_mt",
			"price": "price_usd", "volume": "quantity_mt", "currency": "currency",
		},
		Required: []string{"price_usd", "quantity_mt"},
	})}
	candidate3, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID, Bundle: regressionBundle,
		IssueIDs: []string{issueID}, WorkItemID: work.WorkID, Explanation: "regression case",
	})
	if err != nil {
		t.Fatalf("candidate 3: %v", err)
	}
	if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
		CandidateID: candidate3.CandidateID, Kind: "structural", Result: "PASS", SuiteID: "ingest.validate",
	}); err != nil {
		t.Fatalf("structural: %v", err)
	}
	if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
		CandidateID: candidate3.CandidateID, Kind: "replay", Result: "PASS", SuiteID: "ingest.replay",
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if err := approveStatus(t, e, candidate3.CandidateID); err == nil {
		Failf(t, id, "§20 bad worker output", "approval requires every gate",
			"a candidate with only a replay pass was approved")
		return
	}
	failed, err := e.RecordValidation(ctx, engine.ValidationRequest{
		CandidateID: candidate3.CandidateID, Kind: "regression", Result: "FAIL", SuiteID: "ingest.regression",
	})
	if err != nil {
		t.Fatalf("regression: %v", err)
	}
	if failed.State != string(engine.CandidateRejected) {
		Failf(t, id, "§20 bad worker output", "a failed regression rejects the candidate",
			"candidate state is %s", failed.State)
		return
	}

	// 4. everything passes: only now can approval happen.
	candidate4, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID, Bundle: regressionBundle,
		IssueIDs: []string{issueID}, WorkItemID: work.WorkID, Explanation: "clean candidate",
	})
	if err != nil {
		t.Fatalf("candidate 4: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate4.CandidateID, Kind: kind, Result: "PASS", SuiteID: "ingest." + kind,
		}); err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
	}
	approved, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: candidate4.CandidateID, Approver: "operator",
	})
	if err != nil {
		t.Fatalf("approve candidate 4: %v", err)
	}
	if approved.State != string(engine.CandidateApproved) {
		Failf(t, id, "§20 bad worker output", "a fully validated candidate can be approved",
			"state is %s", approved.State)
		return
	}
	if active := activeRevision(t, e, app); active.RevisionID != app.Baseline.RevisionID {
		Failf(t, id, "§20 bad worker output", "nothing reaches production without promotion",
			"active moved to %s before promotion", active.RevisionID)
		return
	}

	Passf(t, id, "§20 bad worker output", "four gates, four verdicts",
		"invalid YAML refused at creation (no candidate row created); missing mandatory field rejected at replay; failed regression rejected before approval; only the fully validated candidate was approvable, and production is still r1 until promotion")
}

// TestS21ManualEdit records the drift gap.
func TestS21ManualEdit(t *testing.T) {
	Blockedf(t, "S21_manual_edit", "§21 manual config editing", "drift detection",
		"Lymph never reads a managed target back, so an operator editing the file by hand produces no CONFIG_DRIFT event and no import/restore choice: target hashing and reconciliation are part of L3")
}

// TestS23DiskFull injects real write failures under a live daemon.
//
// ENOSPC cannot be created portably without extra privileges, so the daemon runs
// under RLIMIT_FSIZE: the kernel refuses writes past the limit with EFBIG, which
// exercises exactly the same code path as a full disk — a write that fails
// halfway through a mutation.
func TestS23DiskFull(t *testing.T) {
	const id = "S23_disk_full"
	root := openStoreRoot(t, t.TempDir())

	// 4096 blocks of 512 bytes = 2 MiB: enough for SQLite to create its schema,
	// far too little for the traffic that follows.
	d := startDaemon(t, root, "ulimit -f 4096")
	cli := d.client()
	reg := registerViaClient(t, cli, "disk-app")
	appID := reg.Application.ApplicationID
	junction := reg.JunctionIDs["semantic.event_state"]

	var acknowledged []string
	var writeErr error
	attempts := 0
	for attempts = 0; attempts < 20000; attempts++ {
		resp, err := emitViaClient(cli, appID, junction, attempts)
		if err != nil {
			writeErr = err
			break
		}
		acknowledged = append(acknowledged, resp.EventID)
	}

	if writeErr == nil {
		// Either the limit was never reached (test inconclusive) or the daemon
		// ignored a failed write (a defect).
		d.stop()
		e := openEngineAt(t, root)
		events, _ := e.DB().CountEvents()
		if events > len(acknowledged) {
			Failf(t, id, "§23 disk full", "a failed write is never reported as success",
				"%d acknowledged but %d stored", len(acknowledged), events)
			return
		}
		Skippedf(t, id, "§23 disk full", "write-failure injection",
			"the 64 KiB limit was not reached within 400 events; the store needs a bigger workload to exhaust it")
		return
	}
	t.Logf("write failure at event %d: %v", attempts, writeErr)

	// Everything acknowledged before the wall must still be there.
	missing := 0
	for _, eventID := range acknowledged {
		var event struct {
			EventID string `json:"event_id"`
		}
		if err := cli.Get(context.Background(), "/v1/events/"+eventID, &event); err != nil {
			missing++
		}
	}
	d.stop()

	// The store must reopen without manual repair.
	e, err := engine.Open(engine.Options{Root: root, Logger: quietLogger()})
	if err != nil {
		Failf(t, id, "§23 disk full", "the store survives a full disk",
			"the daemon could not reopen the store after the write failures: %v", err)
		return
	}
	defer e.Close()

	report, err := e.VerifyLedger()
	if err != nil {
		Failf(t, id, "§23 disk full", "no partial record becomes canonical",
			"ledger verification failed after the write failures: %v", err)
		return
	}
	if missing > 0 {
		Failf(t, id, "§23 disk full", "acknowledged work is never lost",
			"%d acknowledged events are missing after the write failures", missing)
		return
	}

	// Objects are complete or absent, never truncated.
	revisions, err := e.DB().ListRevisions("", 1000)
	if err != nil {
		t.Fatalf("list revisions: %v", err)
	}
	for _, revision := range revisions {
		if err := e.Objects().VerifyRevision(revision.ContentHash); err != nil {
			Failf(t, id, "§23 disk full", "objects are complete or absent",
				"revision r%d is broken: %v", revision.Sequence, err)
			return
		}
	}

	before := stateDigest(t, e)
	if _, err := e.Rebuild(); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if after := stateDigest(t, e); after != before {
		Failf(t, id, "§23 disk full", "the projection still matches the ledger",
			"rebuild changed the digest: %s vs %s", shorten(before), shorten(after))
		return
	}

	Passf(t, id, "§23 disk full", "a failing write is refused, never half-applied",
		"writes refused with %v after %d events; %d acknowledged events intact; ledger verified with %d records (%d torn bytes recovered); every revision's objects intact; rebuild digest unchanged",
		shorten(writeErr.Error()), attempts, len(acknowledged), report.Records, report.TornTailBytes)
}

// TestS24SQLiteRebuild destroys the projection and rebuilds it.
func TestS24SQLiteRebuild(t *testing.T) {
	const id = "S24_sqlite_rebuild"
	ctx := context.Background()
	root := openStoreRoot(t, t.TempDir())
	e := openEngineAt(t, root)

	// A store with real history: three applications, issues, work, a promotion.
	var apps []App
	for _, sim := range Simulators()[:3] {
		app := setupApp(t, e, sim)
		apps = append(apps, app)
	}
	for i := 0; i < 30; i++ {
		sim := apps[i%len(apps)]
		emit(t, e, sim, sim.Sim.Feedback, sim.Sim.Reason,
			map[string]any{"text": fmt.Sprintf("rebuild sample %d", i)}, "svc", sim.Baseline.RevisionID)
	}
	issues, err := e.DB().ListIssues("", 10, "count")
	if err != nil {
		t.Fatalf("issues: %v", err)
	}
	// Pick an issue that belongs to the application whose family we are about
	// to modify; issues come back ordered by occurrence count, not by app.
	var targetIssue string
	for _, issue := range issues {
		if issue.ApplicationID == apps[0].ApplicationID() {
			targetIssue = issue.IssueID
			break
		}
	}
	if targetIssue == "" {
		t.Fatalf("no issue found for the first application")
	}
	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{targetIssue}})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: apps[0].ApplicationID(), ConfigFamily: apps[0].FamilyID,
		Bundle:   objectstore.Bundle{"rules.yaml": bundle("# semantic rules", semanticDoc{Version: 2, Rules: []SemanticRule{{ID: "a", Phrase: "awarded", State: "AWARDED"}}})},
		IssueIDs: []string{targetIssue}, WorkItemID: work.WorkID, Explanation: "rebuild fixture",
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS", SuiteID: "semantic." + kind,
		}); err != nil {
			t.Fatalf("validation: %v", err)
		}
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{CandidateID: candidate.CandidateID, Approver: "lab"}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if _, err := e.PromoteCandidate(ctx, candidate.CandidateID, "lab"); err != nil {
		t.Fatalf("promote: %v", err)
	}

	wantDigest := stateDigest(t, e)
	beforeDetail := stateDigestDetail(t, e)
	ledgerReport, err := e.VerifyLedger()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 1. The database file simply disappears.
	dbPath := filepath.Join(root, "db", "lymph.sqlite")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(dbPath + suffix); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove %s: %v", dbPath+suffix, err)
		}
	}
	rebuilt := openEngineAt(t, root)
	status, err := rebuilt.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Applications != 3 {
		Failf(t, id, "§24 SQLite rebuild", "a deleted projection is rebuilt from the ledger",
			"after rebuild the store holds %d applications, expected 3", status.Applications)
		return
	}
	if got := stateDigest(t, rebuilt); got != wantDigest {
		Failf(t, id, "§24 SQLite rebuild", "the rebuilt projection is identical",
			"digest differs: %s vs %s; differing tables: %s",
			shorten(wantDigest), shorten(got), diffTables(beforeDetail, stateDigestDetail(t, rebuilt)))
		return
	}
	rebuilt.Close()

	// 2. The database file is present but corrupted.
	if err := os.WriteFile(dbPath, []byte("this is not a database, it is noise"), 0o640); err != nil {
		t.Fatalf("corrupt db: %v", err)
	}
	if _, err := engine.Open(engine.Options{Root: root, Logger: quietLogger()}); err == nil {
		Failf(t, id, "§24 SQLite rebuild", "corruption is reported, not silently ignored",
			"the daemon started against a corrupt projection file")
		return
	} else {
		t.Logf("corrupt projection refused as expected: %v", err)
	}
	// The operator path: preserve the corrupt file for diagnosis, start clean.
	if err := os.Rename(dbPath, dbPath+".corrupt"); err != nil {
		t.Fatalf("preserve corrupt db: %v", err)
	}
	recovered := openEngineAt(t, root)
	applied, err := recovered.Rebuild()
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if got := stateDigest(t, recovered); got != wantDigest {
		Failf(t, id, "§24 SQLite rebuild", "canonical truth survives projection loss",
			"digest after corruption recovery differs: %s vs %s", shorten(wantDigest), shorten(got))
		return
	}

	Passf(t, id, "§24 SQLite rebuild", "delete it, corrupt it, rebuild it",
		"deleted projection rebuilt automatically to an identical state digest (%s); a corrupt file was refused with an error; after preserving it, a rebuild replayed %d ledger records (%d) to the same digest",
		shorten(wantDigest), applied, ledgerReport.Records)
}

// TestS25CorruptLedger damages canonical history, which is not recoverable.
func TestS25CorruptLedger(t *testing.T) {
	const id = "S25_corrupt_ledger"
	root := openStoreRoot(t, t.TempDir())
	e := openEngineAt(t, root)
	sim := MCPSim()
	app := setupApp(t, e, sim)
	for i := 0; i < 20; i++ {
		emit(t, e, app, sim.Feedback, sim.Reason,
			map[string]any{"text": fmt.Sprintf("sample %d", i)}, "svc", app.Baseline.RevisionID)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	segments, err := filepath.Glob(filepath.Join(root, "ledger", "*", "*", "segment-*.log"))
	if err != nil || len(segments) == 0 {
		t.Fatalf("find segments: %v", err)
	}
	raw, err := os.ReadFile(segments[0])
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	lines := strings.SplitAfter(string(raw), "\n")
	if len(lines) < 4 {
		t.Fatalf("expected several records, got %d lines", len(lines))
	}
	// Damaging one character inside an event payload keeps the line valid JSON
	// and breaks only the record hash — which is exactly the case the chain is
	// meant to catch.
	damagedLine := -1
	const marker = "sample 1"
	for i, line := range lines {
		if i == len(lines)-1 {
			break // leave the tail alone; damage something in the middle
		}
		if idx := strings.Index(line, marker); idx >= 0 {
			damaged := []byte(line)
			damaged[idx] = 'X'
			lines[i] = string(damaged)
			damagedLine = i
			break
		}
	}
	if damagedLine < 0 {
		t.Fatalf("could not find %q in any record; the fixture did not exercise corruption", marker)
	}
	if err := os.WriteFile(segments[0], []byte(strings.Join(lines, "")), 0o640); err != nil {
		t.Fatalf("write damaged segment: %v", err)
	}

	_, err = engine.Open(engine.Options{Root: root, Logger: quietLogger()})
	if err == nil {
		Failf(t, id, "§25 corrupt ledger", "damaged canon stops the daemon",
			"the daemon started against a ledger whose record hash no longer matches")
		return
	}
	message := err.Error()
	if !strings.Contains(message, "segment-") || !strings.Contains(message, "line") {
		Failf(t, id, "§25 corrupt ledger", "the failure names the damaged place",
			"error does not identify the segment and record: %s", message)
		return
	}
	if !strings.Contains(message, "recomputed") {
		Failf(t, id, "§25 corrupt ledger", "the failure names expected and observed hashes",
			"error does not report the hash mismatch: %s", message)
		return
	}

	// Nothing was auto-repaired: the damaged bytes are still on disk, and the
	// only recovery is an operator restoring real history from backup.
	after, err := os.ReadFile(segments[0])
	if err != nil {
		t.Fatalf("re-read segment: %v", err)
	}
	if !strings.Contains(string(after), "Xample 1") {
		Failf(t, id, "§25 corrupt ledger", "corruption is never silently repaired",
			"the damaged bytes were rewritten behind the operator's back")
		return
	}

	Passf(t, id, "§25 corrupt ledger", "canonical corruption is loud",
		"one byte changed in an old record; the daemon refused to start with: %s", shorten(message))
}

// TestS26Rollback records the rollback gap.
func TestS26Rollback(t *testing.T) {
	Blockedf(t, "S26_rollback", "§26 config rollback", "deployment rollback",
		"there is no deployment to fail: last_good and deployed move only at baseline import, and DEPLOYMENT_FAILED is never produced, because health checks and the deployment transaction are L3")
}

// TestS33Soak runs every simulator together for a long stretch, then restarts.
func TestS33Soak(t *testing.T) {
	const id = "S33_soak"
	ctx := context.Background()
	root := openStoreRoot(t, t.TempDir())
	e := openEngineAt(t, root)

	appCount := scaled(10, 50)
	eventsPerApp := scaled(300, 20000)
	revisions := scaled(20, 1000)

	runtimeBefore := runtimeStats()

	apps := make([]App, 0, appCount)
	for i := 0; i < appCount; i++ {
		sim := Simulators()[i%len(Simulators())]
		clone := *sim
		clone.Name = fmt.Sprintf("%s-%d", sim.Name, i)
		clone.Family = fmt.Sprintf("%s_%d", sim.Family, i) // still colliding across apps
		app := setupApp(t, e, &clone)
		apps = append(apps, app)
	}

	start := time.Now()
	total := 0
	for _, app := range apps {
		for i := 0; i < eventsPerApp; i++ {
			emit(t, e, app, app.Sim.Feedback, app.Sim.Reason,
				map[string]any{"text": fmt.Sprintf("soak %s %d", app.Sim.Name, i%17)}, app.Sim.Name, app.Baseline.RevisionID)
			total++
		}
	}
	eventDuration := time.Since(start)

	// Config churn: revisions on top of revisions.
	revisionCount := 0
	for i := 0; i < revisions; i++ {
		app := apps[i%len(apps)]
		base := activeRevision(t, e, app)
		body := []byte(fmt.Sprintf("version: %d\nrules:\n  - id: soak%d\n    phrase: awarded\n    state: AWARDED\n", i+2, i))
		bundle := objectstore.Bundle{"rules.yaml": body}
		revision, err := e.CreateRevision(ctx, engine.RevisionRequest{
			Application: app.ApplicationID(), ConfigFamily: app.FamilyID, Bundle: bundle,
			Message: fmt.Sprintf("soak revision %d", i), Author: "soak",
			Parent: base.RevisionID,
		})
		if err != nil {
			t.Fatalf("revision %d: %v", i, err)
		}
		expected := base.RevisionID
		if _, err := e.SetRef(ctx, engine.SetRefRequest{
			Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
			RefName: engine.RefActive, RevisionID: revision.RevisionID,
			ExpectedOld: &expected, Reason: "soak", Actor: "soak",
		}); err != nil {
			t.Fatalf("ref move %d: %v", i, err)
		}
		revisionCount++
	}

	status, err := e.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	wantDigest := stateDigest(t, e)
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	restartStart := time.Now()
	reopened := openEngineAt(t, root)
	restartDuration := time.Since(restartStart)

	restarted, err := reopened.Status(ctx)
	if err != nil {
		t.Fatalf("status after restart: %v", err)
	}
	if restarted.Events != status.Events || restarted.Applications != status.Applications {
		Failf(t, id, "§33 soak", "the daemon comes back after a long run",
			"before: %d events / %d apps, after: %d events / %d apps",
			status.Events, status.Applications, restarted.Events, restarted.Applications)
		return
	}
	if got := stateDigest(t, reopened); got != wantDigest {
		Failf(t, id, "§33 soak", "nothing is lost across a restart",
			"digest changed across restart: %s vs %s", shorten(wantDigest), shorten(got))
		return
	}
	report, err := reopened.VerifyLedger()
	if err != nil {
		Failf(t, id, "§33 soak", "the chain survives a long run", "%v", err)
		return
	}

	runtimeAfter := runtimeStats()
	goroutinesLeaked := runtimeAfter.Goroutines - runtimeBefore.Goroutines
	if goroutinesLeaked > 20 {
		Failf(t, id, "§33 soak", "no goroutine leak",
			"goroutines went from %d to %d across the soak", runtimeBefore.Goroutines, runtimeAfter.Goroutines)
		return
	}
	if runtimeAfter.OpenFiles > runtimeBefore.OpenFiles+50 {
		Failf(t, id, "§33 soak", "no file descriptor leak",
			"open descriptors went from %d to %d", runtimeBefore.OpenFiles, runtimeAfter.OpenFiles)
		return
	}

	Passf(t, id, "§33 soak", "many applications, many events, restart",
		"%d applications and %d events in %.1fs (%.0f events/s), %d revisions and ref moves, restart in %.2fs; %d ledger records verified; goroutines %d->%d, descriptors %d->%d; digest unchanged",
		appCount, total, eventDuration.Seconds(), float64(total)/eventDuration.Seconds(),
		revisionCount, restartDuration.Seconds(), report.Records,
		runtimeBefore.Goroutines, runtimeAfter.Goroutines, runtimeBefore.OpenFiles, runtimeAfter.OpenFiles)
}

// TestS34Performance checks the provisional sanity gates.
func TestS34Performance(t *testing.T) {
	const id = "S34_performance"
	ctx := context.Background()
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	samples := scaled(300, 3000)
	async := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		start := time.Now()
		emit(t, e, app, sim.Feedback, sim.Reason,
			map[string]any{"text": fmt.Sprintf("perf sample %d", i)}, "perf", app.Baseline.RevisionID)
		async = append(async, time.Since(start))
	}

	durable := make([]time.Duration, 0, samples/10+1)
	for i := 0; i < samples/10+1; i++ {
		start := time.Now()
		if _, err := e.CreateRevision(ctx, engine.RevisionRequest{
			Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
			Bundle:  objectstore.Bundle{"rules.yaml": []byte(fmt.Sprintf("version: 2\nrules:\n  - id: perf%d\n    phrase: awarded\n    state: AWARDED\n", i))},
			Message: "performance sample", Author: "perf",
		}); err != nil {
			t.Fatalf("durable revision: %v", err)
		}
		durable = append(durable, time.Since(start))
	}

	burstStart := time.Now()
	burst := scaled(200, 1000)
	for i := 0; i < burst; i++ {
		emit(t, e, app, sim.Feedback, sim.Reason,
			map[string]any{"text": fmt.Sprintf("burst %d", i)}, "burst", app.Baseline.RevisionID)
	}
	sustained := float64(burst) / time.Since(burstStart).Seconds()

	runtimeStats() // settle
	stats := runtimeStats()

	asyncP95 := percentile(async, 0.95)
	durableP95 := percentile(durable, 0.95)

	// The race detector multiplies every measurement; the gates stay in place,
	// the expectations move.
	slack := 1.0
	instrumented := ""
	if raceEnabled {
		slack = 10
		instrumented = " (measured under -race; gates relaxed 10x)"
	}
	asyncLimit := time.Duration(float64(10*time.Millisecond) * slack)
	durableLimit := time.Duration(float64(50*time.Millisecond) * slack)
	sustainedLimit := 200.0 / slack

	var breaches []string
	if asyncP95 > asyncLimit {
		breaches = append(breaches, fmt.Sprintf("async p95 %s > 10ms", asyncP95))
	}
	if durableP95 > durableLimit {
		breaches = append(breaches, fmt.Sprintf("durable p95 %s > 50ms", durableP95))
	}
	if sustained < sustainedLimit {
		breaches = append(breaches, fmt.Sprintf("sustained %.0f/s < 200/s", sustained))
	}
	if stats.HeapMB > 100 {
		breaches = append(breaches, fmt.Sprintf("heap %.0f MB > 100 MB", stats.HeapMB))
	}
	if len(breaches) > 0 {
		Failf(t, id, "§34 performance", "provisional sanity gates",
			"%s", strings.Join(breaches, "; "))
		return
	}

	Passf(t, id, "§34 performance", "provisional sanity gates",
		"async accept p95 %s over %d samples; durable mutation p95 %s over %d samples; sustained %.0f events/s; heap %.0f MB, %d goroutines%s",
		asyncP95.Round(time.Microsecond), len(async), durableP95.Round(time.Microsecond), len(durable),
		sustained, stats.HeapMB, stats.Goroutines, instrumented)
}

// TestS35Zeroes is the correctness-metric gate: the numbers that must be zero.
func TestS35Zeroes(t *testing.T) {
	const id = "S35_zeroes"
	ctx := context.Background()
	e := openEngine(t)

	// A busy store with three unrelated applications.
	var apps []App
	for _, sim := range Simulators()[:3] {
		app := setupApp(t, e, sim)
		apps = append(apps, app)
		for i := 0; i < 40; i++ {
			emit(t, e, app, sim.Feedback, sim.Reason,
				map[string]any{"text": fmt.Sprintf("zeroes %d", i%5)}, "svc", app.Baseline.RevisionID)
		}
	}

	metrics := map[string]int{}

	// cross-application corruption
	issues, err := e.DB().ListIssues("", 1000, "count")
	if err != nil {
		t.Fatalf("issues: %v", err)
	}
	junctions, err := e.DB().ListJunctions("")
	if err != nil {
		t.Fatalf("junctions: %v", err)
	}
	junctionApp := map[string]string{}
	for _, junction := range junctions {
		junctionApp[junction.JunctionID] = junction.ApplicationID
	}
	for _, issue := range issues {
		if junctionApp[issue.JunctionID] != issue.ApplicationID {
			metrics["cross_application_corruption"]++
		}
	}
	events, err := e.DB().ListEvents("", 10000)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	for _, event := range events {
		if junctionApp[event.JunctionID] != event.ApplicationID {
			metrics["cross_application_corruption"]++
		}
	}

	// duplicate canonical events from retries
	seen := map[string]int{}
	for _, event := range events {
		seen[event.EventID]++
	}
	for _, count := range seen {
		if count > 1 {
			metrics["duplicate_canonical_events"]++
		}
	}

	// untracked ref movement: every ref value must be explained by the reflog
	refs, err := e.DB().ListRefs("", "", "")
	if err != nil {
		t.Fatalf("refs: %v", err)
	}
	reflog, err := e.DB().ListReflog("", "", "", 1000)
	if err != nil {
		t.Fatalf("reflog: %v", err)
	}
	explained := map[string]bool{}
	for _, entry := range reflog {
		explained[entry.ApplicationID+"|"+entry.ConfigFamilyID+"|"+entry.RefName+"|"+entry.NewRevisionID] = true
	}
	for _, ref := range refs {
		if !explained[ref.ApplicationID+"|"+ref.ConfigFamilyID+"|"+ref.Name+"|"+ref.RevisionID] {
			metrics["untracked_ref_movement"]++
		}
	}

	// stale candidate accidental promotion
	candidates, err := e.DB().ListCandidates("", "", 1000)
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	for _, candidate := range candidates {
		if candidate.State != string(engine.CandidateStale) {
			continue
		}
		revisions, err := e.DB().ListRevisions(candidate.ConfigFamilyID, 1000)
		if err != nil {
			t.Fatalf("revisions: %v", err)
		}
		for _, revision := range revisions {
			if revision.CandidateID == candidate.CandidateID {
				metrics["stale_candidate_promoted"]++
			}
		}
	}

	// production writes before approval
	approvals := map[string]bool{}
	for _, candidate := range candidates {
		if _, err := e.DB().LatestApproval(candidate.CandidateID); err == nil {
			approvals[candidate.CandidateID] = true
		}
	}
	revisions, err := e.DB().ListRevisions("", 1000)
	if err != nil {
		t.Fatalf("revisions: %v", err)
	}
	for _, revision := range revisions {
		if revision.CandidateID != "" && !approvals[revision.CandidateID] {
			metrics["production_writes_before_approval"]++
		}
	}

	// failure to reconstruct config history
	for _, app := range apps {
		active := activeRevision(t, e, app)
		chain, err := e.Lineage(ctx, active.RevisionID, 100)
		if err != nil || len(chain) == 0 || chain[len(chain)-1].Sequence != 1 {
			metrics["history_not_reconstructible"]++
		}
	}

	// projection rebuild mismatch
	before := stateDigest(t, e)
	if _, err := e.Rebuild(); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if stateDigest(t, e) != before {
		metrics["projection_rebuild_mismatch"]++
	}

	total := 0
	for _, value := range metrics {
		total += value
	}
	if total > 0 {
		Failf(t, id, "§35 correctness zeroes", "every correctness metric is zero",
			"non-zero metrics: %v", metrics)
		return
	}

	Passf(t, id, "§35 correctness zeroes", "hard zeroes",
		"0 cross-application corruption (%d events, %d issues, %d junctions checked); 0 duplicate canonical events; 0 untracked ref movements (%d refs, %d reflog entries); 0 stale-candidate promotions; 0 production writes before approval; 0 unreconstructible histories; 0 projection rebuild mismatches. Partial deployment and deployment history are not applicable: no deployer exists.",
		len(events), len(issues), len(junctions), len(refs), len(reflog))
}

// TestS38Versioning builds a long history with branches and asks for lineage
// and diffs, the two questions an operator actually asks.
func TestS38Versioning(t *testing.T) {
	const id = "S38_versioning"
	ctx := context.Background()
	e := openEngine(t)
	sim := ServiceSim()
	app := setupApp(t, e, sim)

	revisions := scaled(30, 300)
	var promoted []projection.ConfigRevision
	promoted = append(promoted, app.Baseline)

	for i := 0; i < revisions; i++ {
		base := activeRevision(t, e, app)
		body := []byte(fmt.Sprintf("retry_count: %d\ntimeout_ms: %d\nnote: revision %d\n",
			1+i%4, 5000+i*100, i+2))
		candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
			Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
			Bundle: objectstore.Bundle{sim.File: body}, BaseRevisionID: base.RevisionID,
			Explanation: fmt.Sprintf("revision %d", i+2),
		})
		if err != nil {
			t.Fatalf("candidate %d: %v", i, err)
		}
		for _, kind := range []string{"structural", "replay", "regression"} {
			if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
				CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS", SuiteID: "svc." + kind,
			}); err != nil {
				t.Fatalf("validation: %v", err)
			}
		}
		if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
			CandidateID: candidate.CandidateID, Approver: "ops",
		}); err != nil {
			t.Fatalf("approve: %v", err)
		}
		promotion, err := e.PromoteCandidate(ctx, candidate.CandidateID, "lab")
		if err != nil {
			t.Fatalf("promote %d: %v", i, err)
		}
		promoted = append(promoted, promotion.Revision)

		// Every few revisions, also branch from an older revision and abandon
		// the candidate, so history has dead branches in it.
		if i%7 == 3 && i > 5 {
			older := promoted[len(promoted)-4]
			if _, err := e.CreateCandidate(ctx, engine.CandidateRequest{
				Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
				Bundle:         objectstore.Bundle{sim.File: []byte("retry_count: 9\ntimeout_ms: 1000\n")},
				BaseRevisionID: older.RevisionID,
				Explanation:    "abandoned branch",
			}); err != nil {
				t.Fatalf("branch candidate: %v", err)
			}
		}
	}

	active := activeRevision(t, e, app)
	chain, err := e.Lineage(ctx, active.RevisionID, 1000)
	if err != nil {
		t.Fatalf("lineage: %v", err)
	}
	if len(chain) != len(promoted) {
		Failf(t, id, "§38 versioning", "lineage reconstructs the whole promoted chain",
			"lineage is %d revisions long, expected %d", len(chain), len(promoted))
		return
	}
	for i := 0; i < len(chain)-1; i++ {
		if chain[i].ParentRevisionID != chain[i+1].RevisionID {
			Failf(t, id, "§38 versioning", "lineage reconstructs the whole promoted chain",
				"r%d's parent is not r%d", chain[i].Sequence, chain[i+1].Sequence)
			return
		}
	}
	linear := true
	for i := range chain {
		if chain[i].Sequence != len(promoted)-i {
			linear = false
		}
	}
	if !linear {
		Failf(t, id, "§38 versioning", "sequences are dense and ordered",
			"lineage sequences are not the expected descending run")
		return
	}

	// Diff two revisions and check the patch is real.
	from := promoted[len(promoted)-4]
	to := promoted[len(promoted)-1]
	diff, err := e.Diff(ctx, from.RevisionID, to.RevisionID)
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(diff.Files) != 1 || diff.Files[0].Status != "MODIFIED" {
		Failf(t, id, "§38 versioning", "diff reports the changed file",
			"diff has %d file entries: %+v", len(diff.Files), diff.Files)
		return
	}
	if !strings.Contains(diff.Files[0].Patch, "--- a/"+sim.File) || !strings.Contains(diff.Files[0].Patch, "+note: revision") {
		Failf(t, id, "§38 versioning", "diff renders a usable patch",
			"unexpected patch:\n%s", diff.Files[0].Patch)
		return
	}
	if same, err := e.Diff(ctx, to.RevisionID, to.RevisionID); err != nil || len(same.Files) != 0 {
		Failf(t, id, "§38 versioning", "diff of a revision with itself is empty",
			"files %d, err %v", len(same.Files), err)
		return
	}

	// A candidate can still branch from an old revision, and promoting it is
	// refused because production moved on.
	oldBase := promoted[len(promoted)-5].RevisionID
	late, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
		Bundle:         objectstore.Bundle{sim.File: []byte("retry_count: 1\ntimeout_ms: 9000\n")},
		BaseRevisionID: oldBase,
		Explanation:    "late candidate on an old base",
	})
	if err != nil {
		t.Fatalf("late candidate: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: late.CandidateID, Kind: kind, Result: "PASS", SuiteID: "svc." + kind,
		}); err != nil {
			t.Fatalf("validation: %v", err)
		}
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{CandidateID: late.CandidateID, Approver: "ops"}); err != nil {
		t.Fatalf("approve late: %v", err)
	}
	if _, err := e.PromoteCandidate(ctx, late.CandidateID, "lab"); !errors.Is(err, projection.ErrStaleCandidate) {
		Failf(t, id, "§38 versioning", "old bases cannot be promoted late",
			"promotion returned %v", err)
		return
	}

	Passf(t, id, "§38 versioning", "long history, branches, lineage and diff",
		"promoted chain of %d revisions reconstructs exactly (dense sequences, parents linked); abandoned branch candidates preserved; diff r%d->r%d reports 1 MODIFIED file with a real patch; a late candidate on an old base was refused as STALE",
		len(promoted), from.Sequence, to.Sequence)
}

// TestS39Reproducibility keeps everything a future engineer needs to re-run an
// evaluation.
func TestS39Reproducibility(t *testing.T) {
	const id = "S39_reproducibility"
	ctx := context.Background()
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	var issueID string
	for i := 0; i < 5; i++ {
		resp := emit(t, e, app, sim.Feedback, sim.Reason, map[string]any{"text": "awarded"}, "svc", app.Baseline.RevisionID)
		issueID = resp.IssueID
	}
	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{issueID}})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	candidateBundle := objectstore.Bundle{"rules.yaml": bundle("# semantic rules", semanticDoc{Version: 2, Rules: []SemanticRule{
		{ID: "awarded", Phrase: "awarded", State: "AWARDED"},
		{ID: "purchased", Phrase: "purchased", State: "AWARDED"},
		{ID: "sold", Phrase: "sold", State: "SOLD"},
		{ID: "offered", Phrase: "offered", State: "OFFERED"},
		{ID: "shipped", Phrase: "shipped", State: "SHIPPED"},
		{ID: "booked", Phrase: "business was booked with", State: "AWARDED"},
	}})}
	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID, Bundle: candidateBundle,
		IssueIDs: []string{issueID}, WorkItemID: work.WorkID, Explanation: "reproducibility fixture",
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}

	ok, detail := sim.Replay(candidateTree(t, e, candidate))
	if !ok {
		t.Fatalf("replay: %s", detail)
	}
	if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
		CandidateID: candidate.CandidateID, Kind: "replay", Result: "PASS",
		SuiteID: "semantic-service.semantic-rules.validate.v2", SuiteVersion: "v2",
		InputsHash: objectstore.HashOf([]byte("fixtures-v1")), Environment: "lab", Worker: "semantic-worker",
		Metrics: fmt.Sprintf(`{"fixtures":%d}`, len(SemanticFixtures)),
	}); err != nil {
		t.Fatalf("validation: %v", err)
	}

	// Everything needed to re-run is retained.
	storedCandidate, err := e.DB().GetCandidate(candidate.CandidateID)
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}
	if storedCandidate.RootTreeHash != candidate.RootTreeHash {
		Failf(t, id, "§39 reproducibility", "candidate bytes are retained by hash",
			"tree hash changed: %s vs %s", candidate.RootTreeHash, storedCandidate.RootTreeHash)
		return
	}
	if storedCandidate.BaseRevisionID != app.Baseline.RevisionID {
		Failf(t, id, "§39 reproducibility", "the base revision is recorded",
			"base is %s", storedCandidate.BaseRevisionID)
		return
	}
	validations, err := e.DB().ListValidations(candidate.CandidateID)
	if err != nil {
		t.Fatalf("validations: %v", err)
	}
	if len(validations) != 1 {
		t.Fatalf("expected 1 validation, got %d", len(validations))
	}
	recorded := validations[0]
	if recorded.SuiteVersion != "v2" || recorded.InputsHash == "" || recorded.Worker == "" || recorded.Environment == "" {
		Failf(t, id, "§39 reproducibility", "the evaluation records its inputs",
			"validation record is missing provenance: %+v", recorded)
		return
	}

	// Re-running the same deterministic evaluation gives the same answer.
	replayAgain, detailAgain := sim.Replay(candidateTree(t, e, candidate))
	if !replayAgain || detailAgain != detail {
		Failf(t, id, "§39 reproducibility", "a deterministic evaluation reproduces",
			"first run %q, second run %q (%v)", detail, detailAgain, replayAgain)
		return
	}

	// And the stored bytes are exactly what was evaluated.
	stored := candidateTree(t, e, candidate)
	if digestBundle(stored) != digestBundle(candidateBundle) {
		Failf(t, id, "§39 reproducibility", "the stored bytes are the evaluated bytes",
			"bundle digests differ: %s vs %s", digestBundle(candidateBundle), digestBundle(stored))
		return
	}

	Passf(t, id, "§39 reproducibility", "an evaluation can be re-run",
		"candidate bytes retained by tree hash %s, base r%d recorded, suite %s@%s with inputs hash %s and worker %s; re-running the deterministic replay reproduced %q",
		shorten(candidate.RootTreeHash), 1, recorded.SuiteID, recorded.SuiteVersion,
		shorten(recorded.InputsHash), recorded.Worker, detail)
}

// TestS40DisasterRecovery restores only identity, ledger and objects.
func TestS40DisasterRecovery(t *testing.T) {
	const id = "S40_disaster"
	ctx := context.Background()
	original := openStoreRoot(t, t.TempDir())
	e := openEngineAt(t, original)

	var apps []App
	for _, sim := range Simulators()[:3] {
		apps = append(apps, setupApp(t, e, sim))
	}
	for i := 0; i < 60; i++ {
		app := apps[i%len(apps)]
		emit(t, e, app, app.Sim.Feedback, app.Sim.Reason,
			map[string]any{"text": fmt.Sprintf("disaster %d", i)}, "svc", app.Baseline.RevisionID)
	}
	issues, err := e.DB().ListIssues("", 10, "count")
	if err != nil {
		t.Fatalf("issues: %v", err)
	}
	var disasterIssue string
	for _, issue := range issues {
		if issue.ApplicationID == apps[0].ApplicationID() {
			disasterIssue = issue.IssueID
			break
		}
	}
	if disasterIssue == "" {
		t.Fatalf("no issue found for the first application")
	}
	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{disasterIssue}})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: apps[0].ApplicationID(), ConfigFamily: apps[0].FamilyID,
		Bundle:   objectstore.Bundle{"rules.yaml": bundle("# semantic rules", semanticDoc{Version: 2, Rules: []SemanticRule{{ID: "a", Phrase: "awarded", State: "AWARDED"}}})},
		IssueIDs: []string{disasterIssue}, WorkItemID: work.WorkID, Explanation: "disaster fixture",
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS", SuiteID: "semantic." + kind,
		}); err != nil {
			t.Fatalf("validation: %v", err)
		}
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{CandidateID: candidate.CandidateID, Approver: "lab"}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if _, err := e.PromoteCandidate(ctx, candidate.CandidateID, "lab"); err != nil {
		t.Fatalf("promote: %v", err)
	}
	wantDigest := stateDigest(t, e)
	identityUUID := e.Identity().InstanceUUID
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Backup only the canonical three, as the test plan specifies.
	restored := openStoreRoot(t, t.TempDir())
	for _, item := range []struct{ from, to string }{
		{filepath.Join(original, "identity.json"), filepath.Join(restored, "identity.json")},
		{filepath.Join(original, "ledger"), filepath.Join(restored, "ledger")},
		{filepath.Join(original, "objects"), filepath.Join(restored, "objects")},
	} {
		if err := copyTree(item.from, item.to); err != nil {
			t.Fatalf("restore %s: %v", item.from, err)
		}
	}

	recovered := openEngineAt(t, restored)
	if recovered.Identity().InstanceUUID != identityUUID {
		Failf(t, id, "§40 disaster recovery", "the instance identity is restored",
			"instance uuid changed to %s", recovered.Identity().InstanceUUID)
		return
	}
	status, err := recovered.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Applications != len(apps) {
		Failf(t, id, "§40 disaster recovery", "applications return",
			"%d applications after restore, expected %d", status.Applications, len(apps))
		return
	}
	if status.Candidates != 1 {
		Failf(t, id, "§40 disaster recovery", "work results return",
			"%d candidates after restore", status.Candidates)
		return
	}
	if got := stateDigest(t, recovered); got != wantDigest {
		Failf(t, id, "§40 disaster recovery", "the restored store is the same store",
			"digest differs: %s vs %s", shorten(wantDigest), shorten(got))
		return
	}

	Passf(t, id, "§40 disaster recovery", "identity, ledger and objects are enough",
		"restored only identity.json, ledger/ and objects/ into an empty store: %d applications, %d events, %d candidates and the original refs came back with an identical state digest (%s)",
		status.Applications, status.Events, status.Candidates, shorten(wantDigest))
}

// ---------- helpers ----------

func countCandidates(t *testing.T, e *engine.Engine) int {
	t.Helper()
	candidates, err := e.DB().ListCandidates("", "", 1000)
	if err != nil {
		t.Fatalf("list candidates: %v", err)
	}
	return len(candidates)
}

func approveStatus(t *testing.T, e *engine.Engine, candidateID string) error {
	t.Helper()
	_, err := e.ApproveCandidate(context.Background(), engine.ApprovalRequest{
		CandidateID: candidateID, Approver: "lab",
	})
	return err
}

// attemptTrail returns the ledger records describing one work item.
func attemptTrail(t *testing.T, e *engine.Engine, workID string) []ledger.Record {
	t.Helper()
	var trail []ledger.Record
	err := e.Ledger().ForEach(func(record ledger.Record) error {
		if record.Kind == ledger.KindWorkItem && record.ObjectID == workID {
			trail = append(trail, record)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan ledger: %v", err)
	}
	return trail
}
