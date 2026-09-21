package lab

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
)

// TestS41Organism runs the long integrated scenario: five unrelated
// applications living through "weeks" of drift, improvement, failure and
// restart on one daemon, and ends by checking that every change has a complete
// lineage and that the applications that should not have moved did not.
//
// It is graded PARTIAL when the scenario's deployment-shaped steps cannot run,
// because the plan's version of this day-7 story includes one health-check
// failure, one rollback and one manual edit.
func TestS41Organism(t *testing.T) {
	const id = "S41_organism"
	ctx := context.Background()
	root := openStoreRoot(t, t.TempDir())
	e := openEngineAt(t, root)

	// Start each application at a different point in its own history, as an
	// established deployment would be.
	startRevisions := map[string]int{
		"agent-sim":   1,
		"mcp-sim":     5,
		"ml-sim":      3,
		"ingest-sim":  9,
		"service-sim": 2,
	}

	apps := map[string]App{}
	for _, sim := range Simulators() {
		app := setupApp(t, e, sim)
		for revision := 2; revision <= startRevisions[sim.Name]; revision++ {
			created, err := e.CreateRevision(ctx, engine.RevisionRequest{
				Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
				Bundle:  objectstore.Bundle{sim.File: []byte(fmt.Sprintf("# %s revision %d\nversion: %d\n", sim.Name, revision, revision))},
				Message: fmt.Sprintf("history before the lab: r%d", revision),
				Author:  "history",
				Parent:  activeRevision(t, e, app).RevisionID,
			})
			if err != nil {
				t.Fatalf("%s r%d: %v", sim.Name, revision, err)
			}
			expected := activeRevision(t, e, app).RevisionID
			if _, err := e.SetRef(ctx, engine.SetRefRequest{
				Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
				RefName: engine.RefActive, RevisionID: created.RevisionID,
				ExpectedOld: &expected, Reason: "history", Actor: "history",
			}); err != nil {
				t.Fatalf("%s ref move: %v", sim.Name, err)
			}
		}
		apps[sim.Name] = app
	}

	// An improvement helper that runs the whole pipeline for one application.
	improve := func(simName, explanation string, bundle objectstore.Bundle, pass bool) bool {
		t.Helper()
		app := apps[simName]
		responses := make([]engine.EmitResponse, 0, 3)
		for i := 0; i < 3; i++ {
			responses = append(responses, emit(t, e, app, app.Sim.Feedback, app.Sim.Reason,
				map[string]any{"text": fmt.Sprintf("%s day sample %d", simName, i)}, "production", app.Baseline.RevisionID))
		}
		issueID := responses[0].IssueID
		work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{issueID}, Actor: "organism"})
		if err != nil {
			t.Fatalf("%s queue: %v", simName, err)
		}
		if _, err := e.ClaimWork(ctx, engine.ClaimRequest{
			Worker: simName + "-worker", WorkflowTypes: []string{work.WorkflowType}, TTL: time.Minute,
		}); err != nil {
			t.Fatalf("%s claim: %v", simName, err)
		}
		claimed, err := e.DB().GetWorkItem(work.WorkID)
		if err != nil {
			t.Fatalf("%s reload work: %v", simName, err)
		}
		// Workers return typed artifacts now, not bare bundles (mission 83).
		returned, err := e.ReturnImprovement(ctx, engine.ReturnImprovementRequest{
			WorkID: work.WorkID, Worker: simName + "-worker", Attempt: claimed.Attempts,
			Summary: explanation,
			Artifacts: []engine.ImprovementArtifactRequest{{
				Kind: engine.ArtifactConfigBundle,
				ConfigBundle: &engine.ConfigBundleArtifactRequest{
					Bundle: bundle, Explanation: explanation,
				},
			}},
		})
		if err != nil {
			t.Fatalf("%s return improvement: %v", simName, err)
		}
		if len(returned.Candidates) != 1 {
			t.Fatalf("%s return produced %d candidates", simName, len(returned.Candidates))
		}
		candidate := returned.Candidates[0]
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: "structural", Result: "PASS",
			SuiteID: simName + ".validate", InputsHash: "organism-fixtures",
		}); err != nil {
			t.Fatalf("%s structural: %v", simName, err)
		}
		replayResult := "PASS"
		if !pass {
			replayResult = "FAIL"
		}
		validated, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: "replay", Result: replayResult,
			SuiteID: simName + ".replay", InputsHash: "organism-fixtures", Worker: simName + "-worker",
		})
		if err != nil {
			t.Fatalf("%s replay: %v", simName, err)
		}
		if !pass {
			if validated.State != string(engine.CandidateRejected) {
				t.Fatalf("%s should have been rejected, is %s", simName, validated.State)
			}
			return false
		}
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: "regression", Result: "PASS",
			SuiteID: simName + ".regression", Worker: simName + "-worker",
		}); err != nil {
			t.Fatalf("%s regression: %v", simName, err)
		}
		if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
			CandidateID: candidate.CandidateID, Approver: "operator", Comment: explanation,
		}); err != nil {
			t.Fatalf("%s approve: %v", simName, err)
		}
		if _, err := e.PromoteCandidate(ctx, candidate.CandidateID, "organism"); err != nil {
			t.Fatalf("%s promote: %v", simName, err)
		}
		return true
	}

	// Day 1: MCP hits an unknown phrase.
	improve("mcp-sim", "add the booked-phrase rule", objectstore.Bundle{"rules.yaml": bundle("# semantic rules", semanticDoc{Version: 2, Rules: []SemanticRule{
		{ID: "awarded", Phrase: "awarded", State: "AWARDED"},
		{ID: "purchased", Phrase: "purchased", State: "AWARDED"},
		{ID: "sold", Phrase: "sold", State: "SOLD"},
		{ID: "offered", Phrase: "offered", State: "OFFERED"},
		{ID: "shipped", Phrase: "shipped", State: "SHIPPED"},
		{ID: "booked", Phrase: "business was booked with", State: "AWARDED"},
	}})}, true)

	// Day 2: the ingestion feed changes shape.
	improve("ingest-sim", "accept the new column layout", objectstore.Bundle{"config.yaml": bundle("# mapping", ingestMapping{
		Aliases: map[string]string{
			"price_usd": "price_usd", "quantity_mt": "quantity_mt",
			"price": "price_usd", "volume": "quantity_mt", "currency": "currency",
		},
		Required: []string{"price_usd", "quantity_mt"},
	})}, true)

	// A worker dies mid-week: its lease expires and another worker takes over.
	agentApp := apps["agent-sim"]
	stranded := emit(t, e, agentApp, agentApp.Sim.Feedback, agentApp.Sim.Reason,
		map[string]any{"text": "track vessel arrival"}, "agent", agentApp.Baseline.RevisionID)
	strandedWork, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{stranded.IssueID}})
	if err != nil {
		t.Fatalf("stranded queue: %v", err)
	}
	if _, err := e.ClaimWork(ctx, engine.ClaimRequest{Worker: "doomed-worker", TTL: 20 * time.Millisecond}); err != nil {
		t.Fatalf("claim by doomed worker: %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	if _, err := e.SweepLeases(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	recovered, err := e.ClaimWork(ctx, engine.ClaimRequest{Worker: "replacement-worker", TTL: time.Minute})
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if recovered.WorkID != strandedWork.WorkID || recovered.Attempts != 2 {
		Failf(t, id, "§41 organism", "a crashed worker does not lose the work",
			"reclaimed work is %s attempt %d", shorten(recovered.WorkID), recovered.Attempts)
		return
	}

	// Day 3: the agent learns a new intent.
	improve("agent-sim", "add the vessel intent", objectstore.Bundle{"rules.yaml": bundle("# agent policy", agentDoc{Policies: []agentPolicy{
		{Intent: "weather", Tool: "weather_tool"},
		{Intent: "market price", Tool: "price_tool"},
		{Intent: "email", Tool: "email_tool"},
		{Intent: "vessel", Tool: "vessel_tracker"},
	}})}, true)

	// Day 5: the model drifts.
	improve("ml-sim", "switch to model-v2", objectstore.Bundle{
		"config.yaml":    bundle("# serving", mlDoc{ActiveModel: "model-v2", Threshold: 0.35}),
		"model-ref.yaml": []byte("artifact_uri: s3://models/classifier/model-v2.bin\nartifact_sha256: sha256:deadbeef\n"),
	}, true)

	// Day 7: the service candidate does not pass its benchmark, so it stays put.
	improve("service-sim", "tune retries", objectstore.Bundle{"config.yaml": bundle("# settings", serviceDoc{
		RetryCount: 3, TimeoutMS: 5000,
	})}, false)

	// A stale candidate appears while all this is going on.
	mcpApp := apps["mcp-sim"]
	staleBase := apps["mcp-sim"].Baseline.RevisionID
	staleCandidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: mcpApp.ApplicationID(), ConfigFamily: mcpApp.FamilyID,
		Bundle:         objectstore.Bundle{"rules.yaml": []byte("version: 9\nrules:\n  - id: old\n    phrase: old\n    state: OLD\n")},
		BaseRevisionID: staleBase,
		Explanation:    "candidate built against the original baseline",
	})
	if err != nil {
		t.Fatalf("stale candidate: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: staleCandidate.CandidateID, Kind: kind, Result: "PASS", SuiteID: "mcp." + kind,
		}); err != nil {
			t.Fatalf("stale validation: %v", err)
		}
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: staleCandidate.CandidateID, Approver: "operator",
	}); err != nil {
		t.Fatalf("stale approve: %v", err)
	}
	if _, err := e.PromoteCandidate(ctx, staleCandidate.CandidateID, "organism"); !errors.Is(err, projection.ErrStaleCandidate) {
		Failf(t, id, "§41 organism", "the stale candidate is refused", "promotion returned %v", err)
		return
	}

	// Two restarts during the week.
	for restart := 0; restart < 2; restart++ {
		eventsBefore, err := e.DB().CountEvents()
		if err != nil {
			t.Fatalf("count events: %v", err)
		}
		digestBefore := stateDigest(t, e)
		detailBefore := stateDigestDetail(t, e)
		if err := e.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		e = openEngineAt(t, root)
		status, err := e.Status(ctx)
		if err != nil {
			t.Fatalf("status: %v", err)
		}
		if status.Events != eventsBefore || stateDigest(t, e) != digestBefore {
			Failf(t, id, "§41 organism", "restarts are uneventful",
				"restart %d changed the store: %d -> %d events; differing tables: %s",
				restart+1, eventsBefore, status.Events,
				diffTables(detailBefore, stateDigestDetail(t, e)))
			return
		}
	}

	// Expected final state, application by application.
	expectedFinal := map[string]int{
		"agent-sim":   2,
		"mcp-sim":     6,
		"ml-sim":      4,
		"ingest-sim":  10,
		"service-sim": 2, // the candidate failed, so the service stays where it was
	}
	var wrong []string
	for name, want := range expectedFinal {
		got := activeRevision(t, e, apps[name]).Sequence
		if got != want {
			wrong = append(wrong, fmt.Sprintf("%s is r%d, expected r%d", name, got, want))
		}
	}
	if len(wrong) > 0 {
		Failf(t, id, "§41 organism", "each application ends where the story says",
			"%v", wrong)
		return
	}

	// Every promoted revision has a complete lineage back to its baseline.
	complete := 0
	for _, app := range apps {
		active := activeRevision(t, e, app)
		chain, err := e.Lineage(ctx, active.RevisionID, 100)
		if err != nil {
			t.Fatalf("lineage: %v", err)
		}
		if chain[len(chain)-1].Sequence != 1 {
			Failf(t, id, "§41 organism", "every change has a complete lineage",
				"%s's chain stops at r%d", app.Sim.Name, chain[len(chain)-1].Sequence)
			return
		}
		complete++
	}

	// And the whole week ends in a store that still verifies and rebuilds.
	report, err := e.VerifyLedger()
	if err != nil {
		Failf(t, id, "§41 organism", "the store survives the week", "%v", err)
		return
	}
	before := stateDigest(t, e)
	if _, err := e.Rebuild(); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if after := stateDigest(t, e); after != before {
		Failf(t, id, "§41 organism", "the projection still matches canon",
			"digest changed: %s vs %s", shorten(before), shorten(after))
		return
	}

	issues, err := e.DB().ListIssues("", 1000, "count")
	if err != nil {
		t.Fatalf("issues: %v", err)
	}
	byApp := map[string]int{}
	for _, issue := range issues {
		byApp[issue.ApplicationID] += issue.OccurrenceCount
	}
	if len(byApp) != len(apps) {
		Failf(t, id, "§41 organism", "five applications stay separate",
			"issues belong to %d applications", len(byApp))
		return
	}

	Partialf(t, id, "§41 organism", "one daemon, five applications, one week",
		"final state: agent r%d, mcp r%d, ml r%d, ingest r%d, service r%d (its candidate failed); a worker died and was replaced (attempt 2); a stale candidate was refused; two restarts changed nothing; %d lineages complete back to r1; %d ledger records verify and the rebuild digest is unchanged; %d issues across %d applications",
		"the week also includes one manual config edit, one failed health check and one rollback; none of those steps can run without L3",
		expectedFinal["agent-sim"], expectedFinal["mcp-sim"], expectedFinal["ml-sim"],
		expectedFinal["ingest-sim"], expectedFinal["service-sim"], complete, report.Records,
		len(issues), len(byApp))
}
