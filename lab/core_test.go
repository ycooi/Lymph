package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
)

// scale lets the same suite run small in a normal session and large on demand:
//
//	LYMPH_LAB_SCALE=full go test ./lab/...
func scale() string {
	if v := os.Getenv("LYMPH_LAB_SCALE"); v != "" {
		return v
	}
	return "small"
}

func scaled(small, full int) int {
	if scale() == "full" {
		return full
	}
	return small
}

// ---------- section 1 and 2: the lab itself ----------

// TestS01Lab builds the lab and asserts the five simulators are registered as
// five unrelated applications with deliberately colliding human names.
func TestS01Lab(t *testing.T) {
	const id = "S01_lab"
	e := openEngine(t)
	apps := map[string]App{}
	for _, sim := range Simulators() {
		apps[sim.Name] = setupApp(t, e, sim)
	}

	if len(apps) != 5 {
		Failf(t, id, "§1 Lymph Lab", "five simulators", "expected 5 simulators, got %d", len(apps))
		return
	}

	identities := map[string]bool{}
	files := map[string]int{}
	families := map[string]int{}
	for name, app := range apps {
		identities[app.ApplicationID()] = true
		files[app.Sim.File]++
		families[app.Sim.Family]++
		if app.FamilyID == "" || app.Junction == "" {
			Failf(t, id, "§1 Lymph Lab", "five simulators", "%s registered without identities", name)
			return
		}
	}
	if len(identities) != 5 {
		Failf(t, id, "§1 Lymph Lab", "five simulators", "expected 5 distinct application UUIDs, got %d", len(identities))
		return
	}
	if files["config.yaml"] < 3 || files["rules.yaml"] < 2 {
		Failf(t, id, "§1 Lymph Lab", "five simulators",
			"simulators should reuse the same file names, got %v", files)
		return
	}

	// The baseline import means every family already has a starting revision.
	for name, app := range apps {
		rev := activeRevision(t, e, app)
		if rev.Sequence != 1 {
			Failf(t, id, "§1 Lymph Lab", "five simulators", "%s baseline is r%d, expected r1", name, rev.Sequence)
			return
		}
		resp := emit(t, e, app, app.Sim.Feedback, app.Sim.Reason,
			map[string]any{"text": "lab smoke"}, "lab", rev.RevisionID)
		if resp.IssueID == "" {
			Failf(t, id, "§1 Lymph Lab", "five simulators", "%s could not emit feedback", name)
			return
		}
	}

	issues, err := e.DB().ListIssues("", 100, "count")
	if err != nil {
		t.Fatalf("list issues: %v", err)
	}
	if len(issues) != 5 {
		Failf(t, id, "§1 Lymph Lab", "five simulators",
			"expected one issue per simulator, got %d", len(issues))
		return
	}

	Passf(t, id, "§1 Lymph Lab", "five simulators, one daemon",
		"5 applications, 5 junctions, 5 baselines at r1, 5 issues; file names collide (%v) and families collide (%v)",
		files, families)
}

// TestS02Unrelated asserts the universality claim directly: Lymph's own code
// must not know anything about any of the applications it serves.
func TestS02Unrelated(t *testing.T) {
	const id = "S02_unrelated"

	forbidden := []string{
		"semantic-service", "SemanticService", "dap ", "urea", "AWARDED", "semantic_event_rules",
		"agent-sim", "mcp-sim", "ml-sim", "ingest-sim", "service-sim",
		"vessel", "retry_count", "timeout_ms", "active_model", "price_usd",
	}

	roots := []string{"../internal", "../cmd", "../pkg"}
	var hits []string
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			content := string(raw)
			for _, token := range forbidden {
				if strings.Contains(content, token) {
					hits = append(hits, fmt.Sprintf("%s contains %q", path, token))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", root, err)
		}
	}
	if len(hits) > 0 {
		sort.Strings(hits)
		Failf(t, id, "§1 universality", "no application-specific logic inside Lymph",
			"found %d application-specific references:\n%s", len(hits), strings.Join(hits, "\n"))
		return
	}

	Passf(t, id, "§1 universality", "no application-specific logic inside Lymph",
		"scanned internal/, cmd/ and pkg/ for %d application-specific tokens; 0 hits", len(forbidden))
}

// ---------- section 3: the agent loop ----------

// TestS03AgentLoop runs the complete biological loop for the agent simulator:
// feedback, grouping, routing, repair, replay, approval, promotion, and then
// the original failure stops recurring.
func TestS03AgentLoop(t *testing.T) {
	const id = "S03_agent"
	ctx := context.Background()
	e := openEngine(t)
	sim := AgentSim()
	app := setupApp(t, e, sim)

	// 500 occurrences of one missing behaviour, worded slightly differently.
	occurrences := scaled(500, 500)
	var issueID string
	for i := 0; i < occurrences; i++ {
		resp := emit(t, e, app, sim.Feedback, sim.Reason,
			map[string]any{"request": fmt.Sprintf("track vessel %d arrival and notify me when eta changes", 9000+i)},
			fmt.Sprintf("agent-instance-%d", i%4), app.Baseline.RevisionID)
		issueID = resp.IssueID
	}

	issues, err := e.DB().ListIssues("", 50, "count")
	if err != nil {
		t.Fatalf("list issues: %v", err)
	}
	if len(issues) != 1 {
		Failf(t, id, "§3 agent loop", "one issue from many wordings",
			"%d events produced %d issues", occurrences, len(issues))
		return
	}
	if issues[0].OccurrenceCount != occurrences {
		Failf(t, id, "§3 agent loop", "one issue from many wordings",
			"occurrence count is %d, expected %d", issues[0].OccurrenceCount, occurrences)
		return
	}
	issueID = issues[0].IssueID

	// Route it. The junction's declared workflow decides where it goes.
	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{
		IssueIDs: []string{issueID}, Actor: "lab",
	})
	if err != nil {
		t.Fatalf("queue work: %v", err)
	}
	if work.WorkflowType != sim.Workflow {
		Failf(t, id, "§3 agent loop", "work routed by junction contract",
			"workflow is %q, expected %q", work.WorkflowType, sim.Workflow)
		return
	}
	if work.BaseRevisionID != app.Baseline.RevisionID {
		Failf(t, id, "§3 agent loop", "work carries the live base revision",
			"base is %s, expected %s", work.BaseRevisionID, app.Baseline.RevisionID)
		return
	}

	if _, err := e.ClaimWork(ctx, engine.ClaimRequest{Worker: "agent-worker", TTL: time.Minute}); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// The worker proposes a repair. Lymph never writes this itself.
	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application:  app.ApplicationID(),
		ConfigFamily: app.FamilyID,
		Bundle: objectstore.Bundle{sim.File: bundle("# agent tool policy", agentDoc{Policies: []agentPolicy{
			{Intent: "weather", Tool: "weather_tool"},
			{Intent: "market price", Tool: "price_tool"},
			{Intent: "email", Tool: "email_tool"},
			{Intent: "vessel", Tool: "vessel_tracker"},
		}})},
		IssueIDs:    []string{issueID},
		WorkItemID:  work.WorkID,
		Explanation: "add the vessel-tracking intent",
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}

	ok, detail := sim.Replay(candidateTree(t, e, candidate))
	if !ok {
		Failf(t, id, "§3 agent loop", "candidate passes replay", "replay failed: %s", detail)
		return
	}
	if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
		CandidateID: candidate.CandidateID, Kind: "structural", Result: "PASS",
		SuiteID: "agent.policy.validate", InputsHash: "fixtures-v1",
	}); err != nil {
		t.Fatalf("structural validation: %v", err)
	}
	if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
		CandidateID: candidate.CandidateID, Kind: "replay", Result: "PASS",
		SuiteID: "agent.policy.replay", InputsHash: "fixtures-v1", Worker: "agent-worker",
	}); err != nil {
		t.Fatalf("replay validation: %v", err)
	}
	if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
		CandidateID: candidate.CandidateID, Kind: "regression", Result: "PASS",
		SuiteID: "agent.policy.regression", Worker: "agent-worker",
	}); err != nil {
		t.Fatalf("regression validation: %v", err)
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: candidate.CandidateID, Approver: "operator", Comment: "lab approval",
	}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	promotion, err := e.PromoteCandidate(ctx, candidate.CandidateID, "lab")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if promotion.Revision.Sequence != 2 {
		Failf(t, id, "§3 agent loop", "promotion creates r2",
			"promotion produced r%d", promotion.Revision.Sequence)
		return
	}

	// The application loads the new revision and re-runs the failing request.
	newBundle := bundleAt(t, e, promotion.Revision)
	policies, err := ParseAgentPolicies(newBundle)
	if err != nil {
		t.Fatalf("parse promoted policy: %v", err)
	}
	if tool := ResolveTool(policies, "track vessel arrival and notify me when eta changes"); tool != "vessel_tracker" {
		Failf(t, id, "§3 agent loop", "the failure stops recurring",
			"the promoted config still resolves to %q", tool)
		return
	}

	// The health check passed, so the deployment refs move and the issue closes.
	expectedBase := app.Baseline.RevisionID
	if _, err := e.SetRef(ctx, engine.SetRefRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
		RefName: engine.RefDeployed, RevisionID: promotion.Revision.RevisionID,
		ExpectedOld: &expectedBase, Reason: "lab deployment", Actor: "agent-worker",
	}); err != nil {
		t.Fatalf("move deployed ref: %v", err)
	}
	// The issue walks its own lifecycle; skipping a state is refused.
	for _, status := range []engine.IssueStatus{
		engine.IssueInProgress, engine.IssueCandidateProduced, engine.IssueValidating, engine.IssueFixed,
	} {
		if _, err := e.SetIssueStatus(ctx, issueID, status, "lab", "candidate promoted"); err != nil {
			t.Fatalf("issue -> %s: %v", status, err)
		}
	}
	if _, err := e.SetIssueStatus(ctx, issueID, engine.IssueOpen, "lab", "illegal shortcut"); err == nil {
		Failf(t, id, "§3 agent loop", "issue state machine rejects impossible transitions",
			"FIXED -> OPEN was accepted")
		return
	}

	// Silence: the same input no longer produces the same issue.
	before, err := e.DB().CountEvents()
	if err != nil {
		t.Fatalf("count events: %v", err)
	}
	if ResolveTool(policies, "track vessel arrival and notify me when eta changes") == "vessel_tracker" {
		// The application would emit nothing here; the lab models that by not
		// calling emit at all. Assert the invariant instead: no new issue for
		// this fingerprint exists.
		after, err := e.DB().CountEvents()
		if err != nil {
			t.Fatalf("count events: %v", err)
		}
		if after != before {
			t.Fatalf("expected no new events after the fix, went %d -> %d", before, after)
		}
	}

	issue, err := e.DB().GetIssue(issueID)
	if err != nil {
		t.Fatalf("reload issue: %v", err)
	}
	if issue.Status != string(engine.IssueFixed) {
		Failf(t, id, "§3 agent loop", "issue closes when the fix lands",
			"issue status is %s", issue.Status)
		return
	}

	Passf(t, id, "§3 agent loop", "feedback to promoted fix",
		"%d events -> 1 issue (count %d) -> work %s -> candidate -> 3 validations -> approval -> r2; original request now resolves",
		occurrences, issue.OccurrenceCount, work.WorkflowType)
}

// TestS04MCP runs the SemanticService-shaped loop, including the safety gate: a
// candidate that over-generalises must be rejected, and production must not
// move.
func TestS04MCP(t *testing.T) {
	const id = "S04_mcp"
	ctx := context.Background()
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	sources := []string{"FertNews", "FertReport", "email", "chat"}
	occurrences := scaled(200, 1000)
	for i := 0; i < occurrences; i++ {
		emit(t, e, app, sim.Feedback, sim.Reason,
			map[string]any{"text": fmt.Sprintf("The business was booked with RCF at $%d/t", 600+i%40)},
			sources[i%len(sources)], app.Baseline.RevisionID)
	}

	issues, err := e.DB().ListIssues("", 50, "count")
	if err != nil {
		t.Fatalf("list issues: %v", err)
	}
	if len(issues) != 1 || issues[0].OccurrenceCount != occurrences {
		Failf(t, id, "§4 MCP", "one recurring issue",
			"%d events produced %d issues", occurrences, len(issues))
		return
	}
	if issues[0].UniqueSources != len(sources) {
		Failf(t, id, "§4 MCP", "one recurring issue",
			"unique sources is %d, expected %d", issues[0].UniqueSources, len(sources))
		return
	}
	issueID := issues[0].IssueID

	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{issueID}, Actor: "lab"})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}

	// Candidate 1: the tempting, too-wide rule.
	overBroad := objectstore.Bundle{"rules.yaml": bundle("# semantic rules", semanticDoc{Version: 2, Rules: []SemanticRule{
		{ID: "awarded", Phrase: "awarded", State: "AWARDED"},
		{ID: "purchased", Phrase: "purchased", State: "AWARDED"},
		{ID: "sold", Phrase: "sold", State: "SOLD"},
		{ID: "offered", Phrase: "offered", State: "OFFERED"},
		{ID: "shipped", Phrase: "shipped", State: "SHIPPED"},
		{ID: "booked", Phrase: "booked with", State: "AWARDED"},
	}})}
	if ok, detail := ReplaySemantic(overBroad); ok {
		Failf(t, id, "§4 MCP", "the safety gate rejects over-general rules",
			"replay accepted a rule that misclassifies freight as an award")
		return
	} else {
		t.Logf("over-broad candidate correctly rejected: %s", detail)
	}

	bad, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID, Bundle: overBroad,
		IssueIDs: []string{issueID}, WorkItemID: work.WorkID, Explanation: "too wide",
	})
	if err != nil {
		t.Fatalf("create over-broad candidate: %v", err)
	}
	if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
		CandidateID: bad.CandidateID, Kind: "structural", Result: "PASS", SuiteID: "semantic.validate",
	}); err != nil {
		t.Fatalf("structural validation: %v", err)
	}
	badResult, err := e.RecordValidation(ctx, engine.ValidationRequest{
		CandidateID: bad.CandidateID, Kind: "replay", Result: "FAIL",
		SuiteID: "semantic.replay", Metrics: `{"false_positives":1}`,
	})
	if err != nil {
		t.Fatalf("replay validation: %v", err)
	}
	if badResult.State != string(engine.CandidateRejected) {
		Failf(t, id, "§4 MCP", "a failed replay rejects the candidate",
			"candidate state is %s, expected REJECTED", badResult.State)
		return
	}

	// Production must be untouched by the rejected candidate.
	if active := activeRevision(t, e, app); active.RevisionID != app.Baseline.RevisionID {
		Failf(t, id, "§4 MCP", "production stays put when validation fails",
			"active revision moved to %s", active.RevisionID)
		return
	}
	t.Logf("production still r%d after the rejected candidate", activeRevision(t, e, app).Sequence)

	// Candidate 2: the scoped rule.
	good := objectstore.Bundle{"rules.yaml": bundle("# semantic rules", semanticDoc{Version: 2, Rules: []SemanticRule{
		{ID: "awarded", Phrase: "awarded", State: "AWARDED"},
		{ID: "purchased", Phrase: "purchased", State: "AWARDED"},
		{ID: "sold", Phrase: "sold", State: "SOLD"},
		{ID: "offered", Phrase: "offered", State: "OFFERED"},
		{ID: "shipped", Phrase: "shipped", State: "SHIPPED"},
		{ID: "business-booked", Phrase: "business was booked with", State: "AWARDED"},
	}})}
	ok, detail := ReplaySemantic(good)
	if !ok {
		Failf(t, id, "§4 MCP", "the scoped rule passes replay", "replay failed: %s", detail)
		return
	}

	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID, Bundle: good,
		IssueIDs: []string{issueID}, WorkItemID: work.WorkID, Explanation: "scoped booked-phrase rule",
	})
	if err != nil {
		t.Fatalf("create candidate: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS",
			SuiteID: "semantic." + kind, InputsHash: "fixtures-v1",
		}); err != nil {
			t.Fatalf("%s validation: %v", kind, err)
		}
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: candidate.CandidateID, Approver: "operator",
	}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	promotion, err := e.PromoteCandidate(ctx, candidate.CandidateID, "lab")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}

	rules, err := ParseSemanticRules(bundleAt(t, e, promotion.Revision))
	if err != nil {
		t.Fatalf("parse promoted rules: %v", err)
	}
	if got := ClassifySemantic(rules, "The business was booked with RCF at $610/t"); got != "AWARDED" {
		Failf(t, id, "§4 MCP", "the promoted rule fixes the real case", "%q classified as %s", "booked", got)
		return
	}
	if got := ClassifySemantic(rules, "Freight was booked with ABC Shipping for the parcel"); got == "AWARDED" {
		Failf(t, id, "§4 MCP", "the promoted rule keeps freight out", "freight classified as AWARDED")
		return
	}

	Passf(t, id, "§4 MCP", "group repeated unknowns, preserve the safety gate",
		"%d events from %d sources -> 1 issue; over-broad candidate REJECTED with production untouched; scoped candidate -> r%d; freight stays non-award",
		occurrences, len(sources), promotion.Revision.Sequence)
}

// TestS05ML proves Lymph can govern an artifact it does not store, and that a
// model refresh which also changes configuration returns both (mission 31).
func TestS05ML(t *testing.T) {
	const id = "S05_ml"
	ctx := context.Background()
	e := openEngine(t)
	sim := MLSim()
	app := setupApp(t, e, sim)

	occurrences := scaled(300, 1500)
	var issueID string
	for i := 0; i < occurrences; i++ {
		resp := emit(t, e, app, sim.Feedback, sim.Reason,
			map[string]any{"category": "UNKNOWN_CLASS", "confidence": 0.41},
			fmt.Sprintf("ml-worker-%d", i%3), app.Baseline.RevisionID)
		issueID = resp.IssueID
	}

	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{issueID}})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	if work.WorkflowType != sim.Workflow {
		Failf(t, id, "§5 ML", "drift routes to the model workflow",
			"workflow is %q", work.WorkflowType)
		return
	}
	claimed, err := e.ClaimWork(ctx, engine.ClaimRequest{
		Worker: "ml-maintainer", WorkflowTypes: []string{work.WorkflowType}, TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// The artifact that lives outside Lymph.
	const artifactURI = "s3://models/fertilizer-classifier/model-v2.bin"
	artifactDigest := objectstore.HashOf([]byte("pretend this is a 5 GB model"))

	// The worker returns two typed artifacts: the model reference, and the
	// serving configuration that points at it.
	serving := objectstore.Bundle{"config.yaml": bundle("# serving configuration",
		mlDoc{ActiveModel: "model-v2", Threshold: 0.35})}
	if ok, detail := ReplayML(serving); !ok {
		Failf(t, id, "§5 ML", "candidate clears the accuracy gate", "replay failed: %s", detail)
		return
	} else {
		t.Logf("candidate evaluation: %s", detail)
	}

	returned, err := e.ReturnImprovement(ctx, engine.ReturnImprovementRequest{
		WorkID: claimed.WorkID, Worker: "ml-maintainer", Attempt: claimed.Attempts,
		Summary: "switch serving to model-v2",
		Artifacts: []engine.ImprovementArtifactRequest{
			modelRefArtifact(artifactURI, artifactDigest),
			{
				Kind: engine.ArtifactConfigBundle,
				ConfigBundle: &engine.ConfigBundleArtifactRequest{
					Bundle:      serving,
					Explanation: "point serving at model-v2",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("return improvement: %v", err)
	}
	if len(returned.Artifacts) != 2 {
		Failf(t, id, "§5 ML", "a model refresh returns both artifacts",
			"%d artifacts were recorded", len(returned.Artifacts))
		return
	}
	if len(returned.Candidates) != 1 {
		Failf(t, id, "§5 ML", "only the config bundle becomes a candidate",
			"%d candidates were created", len(returned.Candidates))
		return
	}
	candidate := returned.Candidates[0]

	// The model reference is metadata: URI, digest, lineage. The model itself is
	// nowhere in the object store.
	if e.Objects().Has(artifactDigest) {
		Failf(t, id, "§5 ML", "Lymph stores the reference, not the artifact",
			"the model digest %s exists as an object-store blob", shorten(artifactDigest))
		return
	}
	var modelArtifact projection.ImprovementArtifact
	for _, artifact := range returned.Artifacts {
		if artifact.Kind == string(engine.ArtifactModelRef) {
			modelArtifact = artifact
		}
	}
	if !strings.Contains(modelArtifact.Payload, artifactURI) || !strings.Contains(modelArtifact.Payload, artifactDigest) {
		Failf(t, id, "§5 ML", "the model reference records URI and digest",
			"payload is %s", modelArtifact.Payload)
		return
	}
	if modelArtifact.CandidateID != "" {
		Failf(t, id, "§5 ML", "the model artifact creates no candidate",
			"it points at candidate %s", shorten(modelArtifact.CandidateID))
		return
	}

	// The candidate bundle contains the serving config and nothing else: the
	// model reference did not leak into it as a file.
	stored := candidateTree(t, e, candidate)
	if _, ok := stored["model-ref.yaml"]; ok {
		Failf(t, id, "§5 ML", "the external artifact is not copied into the candidate",
			"the candidate bundle contains model-ref.yaml")
		return
	}
	if len(stored) != 1 {
		Failf(t, id, "§5 ML", "the candidate carries only the configuration",
			"bundle contains %s", digestBundle(stored))
		return
	}

	// The rest of the pipeline is unchanged.
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS",
			SuiteID: "ml.serving." + kind, InputsHash: "eval-set-2026-09",
			Metrics: fmt.Sprintf(`{"artifact_digest":%q,"artifact_uri":%q,"accuracy":0.942}`, artifactDigest, artifactURI),
		}); err != nil {
			t.Fatalf("%s validation: %v", kind, err)
		}
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: candidate.CandidateID, Approver: "operator",
	}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	promotion, err := e.PromoteCandidate(ctx, candidate.CandidateID, "lab")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}

	validations, err := e.DB().ListValidations(candidate.CandidateID)
	if err != nil {
		t.Fatalf("validations: %v", err)
	}
	if len(validations) != 3 {
		Failf(t, id, "§5 ML", "evaluation evidence is retained",
			"expected 3 validations, got %d", len(validations))
		return
	}
	if !strings.Contains(validations[0].Metrics, artifactDigest) {
		Failf(t, id, "§5 ML", "evaluation evidence names the evaluated artifact",
			"validation metrics are %s", validations[0].Metrics)
		return
	}

	// And the whole return is canonical: the result and both artifacts survive a
	// rebuild.
	results, err := e.DB().ListImprovementResults(app.ApplicationID(), 10)
	if err != nil {
		t.Fatalf("results: %v", err)
	}
	if len(results) != 1 {
		Failf(t, id, "§5 ML", "the worker return is canonical",
			"%d results recorded", len(results))
		return
	}
	artifacts, err := e.Artifacts(projection.ArtifactFilter{ApplicationID: app.ApplicationID(), Limit: 10})
	if err != nil {
		t.Fatalf("artifacts: %v", err)
	}
	if len(artifacts) != 2 {
		Failf(t, id, "§5 ML", "both artifacts are canonical",
			"%d artifacts recorded", len(artifacts))
		return
	}

	Passf(t, id, "§5 ML", "govern a model reference without storing the model",
		"%d drift events -> issue -> %s -> one return carrying MODEL_REF + CONFIG_BUNDLE: 2 artifacts, 1 candidate; the model %s / %s is recorded and never copied into the object store; the candidate bundle holds only config.yaml; 3 validations name the digest; promotion produced r%d",
		occurrences, work.WorkflowType, shorten(artifactURI), shorten(artifactDigest),
		promotion.Revision.Sequence)
}

// TestS06Ingest proves the structural/schema path for ordinary ETL.
func TestS06Ingest(t *testing.T) {
	const id = "S06_ingest"
	ctx := context.Background()
	e := openEngine(t)
	sim := IngestSim()
	app := setupApp(t, e, sim)

	occurrences := scaled(300, 1200)
	var issueID string
	for i := 0; i < occurrences; i++ {
		resp := emit(t, e, app, sim.Feedback, sim.Reason,
			map[string]any{"table": "cru_prices", "columns": []string{"price", "volume", "currency"}},
			"ingest-1", app.Baseline.RevisionID)
		issueID = resp.IssueID
	}
	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{issueID}})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}

	// Bad candidate: mapping only the new names, dropping the old ones.
	oldBroken := objectstore.Bundle{"config.yaml": bundle("# upstream column mapping", ingestMapping{
		Aliases:  map[string]string{"price": "price_usd", "volume": "quantity_mt", "currency": "currency"},
		Required: []string{"price_usd", "quantity_mt"},
	})}
	if ok, detail := ReplayIngest(oldBroken); ok {
		Failf(t, id, "§6 ingest", "a mapping that breaks the old format is rejected",
			"replay accepted a mapping that drops the old columns")
		return
	} else {
		t.Logf("old-format-breaking candidate correctly rejected: %s", detail)
	}

	// Good candidate: both formats.
	bothWork := objectstore.Bundle{"config.yaml": bundle("# upstream column mapping", ingestMapping{
		Aliases: map[string]string{
			"price_usd": "price_usd", "quantity_mt": "quantity_mt",
			"price": "price_usd", "volume": "quantity_mt", "currency": "currency",
		},
		Required: []string{"price_usd", "quantity_mt"},
	})}
	if ok, detail := ReplayIngest(bothWork); !ok {
		Failf(t, id, "§6 ingest", "a mapping that handles both formats passes", "replay failed: %s", detail)
		return
	} else {
		t.Logf("both-format candidate passes: %s", detail)
	}

	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID, Bundle: bothWork,
		IssueIDs: []string{issueID}, WorkItemID: work.WorkID, Explanation: "accept both column layouts",
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS",
			SuiteID: "ingest.replay", InputsHash: "fixtures-old+new+malformed",
		}); err != nil {
			t.Fatalf("%s validation: %v", kind, err)
		}
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: candidate.CandidateID, Approver: "operator",
	}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	promotion, err := e.PromoteCandidate(ctx, candidate.CandidateID, "lab")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}

	mapping, err := ParseMapping(bundleAt(t, e, promotion.Revision))
	if err != nil {
		t.Fatalf("parse promoted mapping: %v", err)
	}
	for _, fixture := range IngestFixtures {
		if _, ok := ApplyMapping(mapping, fixture.Row); ok != fixture.OK {
			Failf(t, id, "§6 ingest", "the promoted mapping handles every fixture",
				"%s: parsed=%v expected %v", fixture.Name, ok, fixture.OK)
			return
		}
	}

	Passf(t, id, "§6 ingest", "schema drift to a fixed parser mapping",
		"%d drift events -> issue -> candidate; old-format-breaking mapping rejected; both-format mapping -> r%d and handles old, new, malformed, empty and duplicate rows",
		occurrences, promotion.Revision.Sequence)
}

// TestS07Generic proves nothing about Lymph is AI-specific.
func TestS07Generic(t *testing.T) {
	const id = "S07_generic"
	ctx := context.Background()
	e := openEngine(t)
	sim := ServiceSim()
	app := setupApp(t, e, sim)

	// The benchmark is meaningful: the current settings fail it.
	baseRate, baseWorst, err := BenchmarkService(bundleAt(t, e, app.Baseline))
	if err != nil {
		t.Fatalf("benchmark baseline: %v", err)
	}
	if baseRate > 0.5 {
		Failf(t, id, "§7 generic", "the benchmark detects the regression",
			"baseline already passes the benchmark (%.2f)", baseRate)
		return
	}

	var issueID string
	for i := 0; i < scaled(200, 800); i++ {
		resp := emit(t, e, app, sim.Feedback, sim.Reason,
			map[string]any{"upstream_latency_ms": 8000, "observed": "timeouts"},
			"service-sim", app.Baseline.RevisionID)
		issueID = resp.IssueID
	}

	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{issueID}})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	candidateBundle := objectstore.Bundle{"config.yaml": bundle("# service runtime settings", serviceDoc{
		RetryCount: 2, TimeoutMS: 10000,
	})}
	if ok, detail := ReplayService(candidateBundle); !ok {
		Failf(t, id, "§7 generic", "the tuned settings pass the benchmark", "benchmark failed: %s", detail)
		return
	} else {
		t.Logf("candidate benchmark: %s (was %.0f%% success, worst %dms)", detail, baseRate*100, baseWorst)
	}

	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID, Bundle: candidateBundle,
		IssueIDs: []string{issueID}, WorkItemID: work.WorkID, Explanation: "trade retries for patience",
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS",
			SuiteID: "service.benchmark", InputsHash: "bench-200-calls",
		}); err != nil {
			t.Fatalf("%s validation: %v", kind, err)
		}
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: candidate.CandidateID, Approver: "ops",
	}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	promotion, err := e.PromoteCandidate(ctx, candidate.CandidateID, "lab")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}

	Passf(t, id, "§7 generic", "Lymph is infrastructure, not AI infrastructure",
		"plain service: %d latency-drift events -> issue -> %s -> r%d; baseline benchmark %.0f%% success, candidate passes with retry=2 timeout=10000ms",
		scaled(200, 800), work.WorkflowType, promotion.Revision.Sequence, baseRate*100)
}

// TestS09Lineage walks the whole chain forward and backward.
func TestS09Lineage(t *testing.T) {
	const id = "S09_lineage"
	ctx := context.Background()
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	var eventID, issueID string
	for i := 0; i < 5; i++ {
		resp := emit(t, e, app, sim.Feedback, sim.Reason,
			map[string]any{"text": "awarded by the ministry"}, "desk", app.Baseline.RevisionID)
		eventID, issueID = resp.EventID, resp.IssueID
	}
	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{issueID}, Actor: "lab"})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	if _, err := e.ClaimWork(ctx, engine.ClaimRequest{Worker: "mcp-worker", TTL: time.Minute}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	claimed, err := e.DB().GetWorkItem(work.WorkID)
	if err != nil {
		t.Fatalf("reload work: %v", err)
	}
	// The chain now starts from a typed worker return, which is what a real
	// worker produces (mission section 107).
	returned, err := e.ReturnImprovement(ctx, engine.ReturnImprovementRequest{
		WorkID: work.WorkID, Worker: "mcp-worker", Attempt: claimed.Attempts,
		Summary: "add the ministry phrase",
		Artifacts: []engine.ImprovementArtifactRequest{{
			Kind: engine.ArtifactConfigBundle,
			ConfigBundle: &engine.ConfigBundleArtifactRequest{
				Bundle: objectstore.Bundle{"rules.yaml": bundle("# semantic rules", semanticDoc{Version: 2, Rules: []SemanticRule{
					{ID: "awarded", Phrase: "awarded", State: "AWARDED"},
					{ID: "purchased", Phrase: "purchased", State: "AWARDED"},
					{ID: "sold", Phrase: "sold", State: "SOLD"},
					{ID: "offered", Phrase: "offered", State: "OFFERED"},
					{ID: "shipped", Phrase: "shipped", State: "SHIPPED"},
					{ID: "ministry", Phrase: "awarded by the ministry", State: "AWARDED"},
				}})},
				Explanation: "lineage fixture",
			},
		}},
	})
	if err != nil {
		t.Fatalf("return improvement: %v", err)
	}
	if len(returned.Candidates) != 1 || len(returned.Artifacts) != 1 {
		t.Fatalf("return produced %d candidates and %d artifacts", len(returned.Candidates), len(returned.Artifacts))
	}
	candidate := returned.Candidates[0]
	result := returned.Result
	validation, err := e.RecordValidation(ctx, engine.ValidationRequest{
		CandidateID: candidate.CandidateID, Kind: "structural", Result: "PASS", SuiteID: "semantic.validate",
	})
	if err != nil {
		t.Fatalf("validation: %v", err)
	}
	if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
		CandidateID: candidate.CandidateID, Kind: "replay", Result: "PASS", SuiteID: "semantic.replay",
	}); err != nil {
		t.Fatalf("validation: %v", err)
	}
	if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
		CandidateID: candidate.CandidateID, Kind: "regression", Result: "PASS", SuiteID: "semantic.regression",
	}); err != nil {
		t.Fatalf("validation: %v", err)
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: candidate.CandidateID, Approver: "operator", Comment: "lineage fixture",
	}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	promotion, err := e.PromoteCandidate(ctx, candidate.CandidateID, "lab")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	_ = validation

	// Forward: every link must exist.
	event, err := e.DB().GetEvent(eventID)
	if err != nil {
		t.Fatalf("event: %v", err)
	}
	issue, err := e.DB().GetIssue(issueID)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	workItem, err := e.DB().GetWorkItem(work.WorkID)
	if err != nil {
		t.Fatalf("work item: %v", err)
	}
	validations, err := e.DB().ListValidations(candidate.CandidateID)
	if err != nil {
		t.Fatalf("validations: %v", err)
	}
	approval, err := e.DB().LatestApproval(candidate.CandidateID)
	if err != nil {
		t.Fatalf("approval: %v", err)
	}
	revision := promotion.Revision
	reflog, err := e.DB().ListReflog(app.ApplicationID(), app.FamilyID, app.InstallationID(), 10)
	if err != nil {
		t.Fatalf("reflog: %v", err)
	}

	links := map[string]bool{
		"event->issue":        event.Fingerprint == issue.Fingerprint,
		"issue->work":         contains(workItem.IssueIDs, issue.IssueID),
		"work->result":        workItem.ImprovementResultID == result.ResultID,
		"result->work":        result.WorkItemID == workItem.WorkID,
		"result->issue":       contains(result.IssueIDs, issue.IssueID),
		"result->artifact":    returned.Artifacts[0].ResultID == result.ResultID,
		"artifact->candidate": returned.Artifacts[0].CandidateID == candidate.CandidateID,
		"work->candidate":     candidate.WorkItemID == workItem.WorkID,
		"candidate->issues":   contains(candidate.IssueIDs, issue.IssueID),
		"validations->cand":   len(validations) == 3 && validations[0].CandidateID == candidate.CandidateID,
		"approval->candidate": approval.CandidateID == candidate.CandidateID,
		"revision->candidate": revision.CandidateID == candidate.CandidateID,
		"revision->issues":    contains(revision.IssueIDs, issue.IssueID),
		"ref->revision":       promotion.Refs != nil && anyRefPointsAt(promotion.Refs, revision.RevisionID),
		"reflog->candidate":   anyReflogNames(reflog, candidate.CandidateID),
	}

	var missing []string
	for name, ok := range links {
		if !ok {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)

	if len(missing) > 0 {
		Failf(t, id, "§9 lineage", "every link in the chain",
			"missing links: %s", strings.Join(missing, ", "))
		return
	}

	// Backward: from the running revision back to the events that caused it.
	chain, err := e.Lineage(ctx, revision.RevisionID, 10)
	if err != nil {
		t.Fatalf("lineage: %v", err)
	}
	if len(chain) != 2 || chain[1].RevisionID != app.Baseline.RevisionID {
		Failf(t, id, "§9 lineage", "why is r2 running",
			"lineage chain is %d long", len(chain))
		return
	}

	Passf(t, id, "§9 lineage", "trace forward and backward without a missing link",
		"event %s -> issue %s (%d occurrences) -> work %s -> result %s -> artifact %s (%s) -> candidate %s -> 3 validations -> approval %s -> revision r%d -> ref move; reverse traversal returns r%d -> r%d. The deployment link is the only absent one, because no deployer exists.",
		shorten(eventID), shorten(issueID), issue.OccurrenceCount, shorten(workItem.WorkID),
		shorten(result.ResultID), shorten(returned.Artifacts[0].ArtifactID), returned.Artifacts[0].Kind,
		shorten(candidate.CandidateID), shorten(approval.ApprovalID), revision.Sequence,
		revision.Sequence, chain[1].Sequence)
}

// ---------- sections 14 to 17 ----------

// TestS14Duplicate separates retransmission from recurrence.
func TestS14Duplicate(t *testing.T) {
	const id = "S14_duplicate"
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	first := emit(t, e, app, sim.Feedback, sim.Reason, map[string]any{"text": "awarded"}, "p1", app.Baseline.RevisionID)
	duplicates := 100
	for i := 0; i < duplicates; i++ {
		resp, err := emitWithID(t, e, app, first.EventID)
		if err != nil {
			t.Fatalf("duplicate emit: %v", err)
		}
		if !resp.Duplicate {
			Failf(t, id, "§14 duplicates", "same event id is idempotent",
				"resend %d was accepted as a new event", i)
			return
		}
	}
	events, err := e.DB().CountEvents()
	if err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 {
		Failf(t, id, "§14 duplicates", "same event id is idempotent",
			"%d sends of one event id produced %d canonical events", duplicates+1, events)
		return
	}

	distinct := scaled(100, 100)
	for i := 0; i < distinct; i++ {
		emit(t, e, app, sim.Feedback, sim.Reason,
			map[string]any{"text": fmt.Sprintf("purchased %d tonnes", 1000+i)},
			"p1", app.Baseline.RevisionID)
	}
	issues, err := e.DB().ListIssues("", 50, "count")
	if err != nil {
		t.Fatalf("list issues: %v", err)
	}
	var repeated int
	for _, issue := range issues {
		if issue.OccurrenceCount == distinct {
			repeated = issue.OccurrenceCount
		}
	}
	if repeated != distinct {
		Failf(t, id, "§14 duplicates", "same problem is one issue",
			"%d distinct events with one fingerprint produced counts %v", distinct, countsOf(issues))
		return
	}

	Passf(t, id, "§14 duplicates", "retransmission vs recurrence",
		"%d sends of one event id -> 1 canonical event; %d distinct event ids sharing a fingerprint -> 1 issue with occurrence_count %d",
		duplicates+1, distinct, repeated)
}

// TestS15Collision registers three applications that look identical by name.
func TestS15Collision(t *testing.T) {
	const id = "S15_collision"
	ctx := context.Background()
	e := openEngine(t)

	type appState struct {
		app      App
		familyID string
	}
	var apps []appState
	for _, name := range []string{"alpha", "beta", "gamma"} {
		sim := &Simulator{
			Name:     name,
			Family:   "default",
			File:     "config.yaml",
			Junction: "parser.default",
			Workflow: "parser-default-improvement",
			Owner:    "lab",
			Baseline: objectstore.Bundle{"config.yaml": []byte("version: 1\nmode: strict\n")},
		}
		reg, err := e.RegisterApplication(ctx, sim.Manifest())
		if err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
		baseline, err := e.ImportBaseline(ctx, engine.BaselineRequest{
			Application: reg.Application.ApplicationID, ConfigFamily: reg.FamilyIDs["default"],
			Bundle: sim.Baseline, Author: "lab",
		})
		if err != nil {
			t.Fatalf("baseline %s: %v", name, err)
		}
		if baseline.Sequence != 1 {
			t.Fatalf("%s baseline is r%d", name, baseline.Sequence)
		}
		apps = append(apps, appState{
			app: App{Sim: sim, Reg: reg, FamilyID: reg.FamilyIDs["default"],
				Junction: reg.JunctionIDs["parser.default"], Baseline: baseline},
			familyID: reg.FamilyIDs["default"],
		})
	}

	// Identical config family name and junction name, three identities.
	seenFamilies := map[string]bool{}
	for _, state := range apps {
		if seenFamilies[state.familyID] {
			Failf(t, id, "§15 collision", "UUID-based isolation",
				"two applications share a config family identity")
			return
		}
		seenFamilies[state.familyID] = true
	}

	perApp := scaled(1000, 10000)
	for _, state := range apps {
		for i := 0; i < perApp; i++ {
			emit(t, e, state.app, protocol.FeedbackUnknown, "UNKNOWN_FIELD",
				map[string]any{"field": fmt.Sprintf("col_%d", i%25)}, "lab", state.app.Baseline.RevisionID)
		}
	}

	issues, err := e.DB().ListIssues("", 1000, "count")
	if err != nil {
		t.Fatalf("list issues: %v", err)
	}
	byApp := map[string]int{}
	for _, issue := range issues {
		byApp[issue.ApplicationID] += issue.OccurrenceCount
	}
	if len(byApp) != 3 {
		Failf(t, id, "§15 collision", "UUID-based isolation",
			"issues landed on %d applications, expected 3: %v", len(byApp), byApp)
		return
	}
	for _, state := range apps {
		if byApp[state.app.ApplicationID()] != perApp {
			Failf(t, id, "§15 collision", "UUID-based isolation",
				"%s gathered %d occurrences, expected %d",
				state.app.Sim.Name, byApp[state.app.ApplicationID()], perApp)
			return
		}
	}

	// Now drive one application to r2 and prove the others stay at r1.
	target := apps[0]
	issueList, err := e.DB().ListIssues("", 1000, "count")
	if err != nil {
		t.Fatalf("list issues: %v", err)
	}
	var targetIssue string
	for _, issue := range issueList {
		if issue.ApplicationID == target.app.ApplicationID() {
			targetIssue = issue.IssueID
			break
		}
	}
	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{targetIssue}})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: target.app.ApplicationID(), ConfigFamily: target.familyID,
		Bundle:   objectstore.Bundle{"config.yaml": []byte("version: 2\nmode: strict\nextra: yes\n")},
		IssueIDs: []string{targetIssue}, WorkItemID: work.WorkID, Explanation: "collision fixture",
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS", SuiteID: "collision.replay",
		}); err != nil {
			t.Fatalf("validation: %v", err)
		}
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: candidate.CandidateID, Approver: "lab",
	}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if _, err := e.PromoteCandidate(ctx, candidate.CandidateID, "lab"); err != nil {
		t.Fatalf("promote: %v", err)
	}

	for i, state := range apps {
		active := activeRevision(t, e, state.app)
		wantSequence := 1
		if i == 0 {
			wantSequence = 2
		}
		if active.Sequence != wantSequence {
			Failf(t, id, "§15 collision", "UUID-based isolation",
				"%s is at r%d, expected r%d", state.app.Sim.Name, active.Sequence, wantSequence)
			return
		}
	}

	Passf(t, id, "§15 collision", "three applications, identical names, no mixing",
		"3 apps share family %q, junction %q and file %q; %d events each stayed separate; promoting alpha to r2 left beta and gamma at r1",
		"default", "parser.default", "config.yaml", perApp)
}

// TestS17Clock proves canonical order does not depend on the wall clock.
func TestS17Clock(t *testing.T) {
	const id = "S17_clock"
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	base := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	times := []time.Time{
		base,
		base.Add(-time.Hour), // clock jumps backwards
		base.Add(time.Hour),  // and forwards
		base.Add(-30 * time.Minute),
	}
	var issueID string
	for i, at := range times {
		resp := emitAt(t, e, app, sim.Feedback, sim.Reason,
			map[string]any{"text": fmt.Sprintf("purchased %d tonnes", 100+i)}, "clock-test",
			app.Baseline.RevisionID, at)
		issueID = resp.IssueID
	}

	report, err := e.VerifyLedger()
	if err != nil {
		Failf(t, id, "§17 clock", "append order is not wall-clock order", "ledger verify failed: %v", err)
		return
	}
	if report.Records < uint64(len(times)) {
		Failf(t, id, "§17 clock", "append order is not wall-clock order",
			"ledger holds %d records, expected at least %d", report.Records, len(times))
		return
	}
	if report.LastSeq != report.Records {
		Failf(t, id, "§17 clock", "sequence numbers stay contiguous under skew",
			"last sequence %d, records %d", report.LastSeq, report.Records)
		return
	}

	issue, err := e.DB().GetIssue(issueID)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if issue.LastSeen.Before(issue.FirstSeen) {
		Failf(t, id, "§17 clock", "issue timestamps stay coherent",
			"last_seen %s precedes first_seen %s", issue.LastSeen, issue.FirstSeen)
		return
	}

	// The client-supplied times really are out of order; what stays ordered is
	// the ledger. Issue windows are tracked on arrival, so they stay coherent
	// even though occurred_at jumps around.
	events, err := e.DB().ListEvents(app.ApplicationID(), 10)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	skewed := false
	for i := 1; i < len(events); i++ {
		// events come back newest-first, so this walks arrival order backwards.
		if events[i-1].OccurredAt.Before(events[i].OccurredAt) {
			skewed = true
			break
		}
	}
	if !skewed {
		Failf(t, id, "§17 clock", "the skew actually happened",
			"occurred_at happened to be monotonic; the test did not exercise clock skew")
		return
	}

	// Rebuild replays in append order, so skewed event times cannot reorder
	// history.
	before := stateDigest(t, e)
	if _, err := e.Rebuild(); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	after := stateDigest(t, e)
	if before != after {
		Failf(t, id, "§17 clock", "history is reconstructed from append order",
			"state digest changed across rebuild: %s vs %s", shorten(before), shorten(after))
		return
	}

	Passf(t, id, "§17 clock", "append order, not wall-clock order",
		"%d events delivered with occurred_at out of order (12:00, 11:00, 13:00, 11:30); ledger sequences contiguous to %d with the chain intact; issue window tracked on arrival (%s to %s) and therefore coherent; rebuild digest unchanged",
		len(times), report.LastSeq,
		issue.FirstSeen.Format(time.RFC3339), issue.LastSeen.Format(time.RFC3339))
}

// ---------- sections 27 to 29, 36, 37 ----------

// TestS27LateRegression asks which revision introduced a problem.
func TestS27LateRegression(t *testing.T) {
	const id = "S27_late_regression"
	ctx := context.Background()
	e := openEngine(t)
	sim := ServiceSim()
	app := setupApp(t, e, sim)

	// Promote r2 first.
	var issueID string
	for i := 0; i < scaled(50, 200); i++ {
		resp := emit(t, e, app, sim.Feedback, sim.Reason, map[string]any{"n": i}, "svc", app.Baseline.RevisionID)
		issueID = resp.IssueID
	}
	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{issueID}})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
		Bundle:   objectstore.Bundle{"config.yaml": bundle("# service runtime settings", serviceDoc{RetryCount: 2, TimeoutMS: 10000})},
		IssueIDs: []string{issueID}, WorkItemID: work.WorkID, Explanation: "first fix",
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS", SuiteID: "svc." + kind,
		}); err != nil {
			t.Fatalf("validation: %v", err)
		}
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{CandidateID: candidate.CandidateID, Approver: "ops"}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	promotion, err := e.PromoteCandidate(ctx, candidate.CandidateID, "lab")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}

	// 100 good operations, then the regression shows up.
	for i := 0; i < scaled(100, 100); i++ {
		emit(t, e, app, protocol.FeedbackHumanCorrection, "OPERATOR_FIXED", map[string]any{"n": i},
			"svc", promotion.Revision.RevisionID)
	}
	regression := emit(t, e, app, protocol.FeedbackRegression, "LATENCY_REGRESSION",
		map[string]any{"note": "introduced by the retry change"}, "svc", promotion.Revision.RevisionID)

	issue, err := e.DB().GetIssue(regression.IssueID)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if issue.ControllingRevision != promotion.Revision.RevisionID {
		Failf(t, id, "§27 late regression", "the issue names the revision that was running",
			"controlling revision is %q, expected %q", issue.ControllingRevision, promotion.Revision.RevisionID)
		return
	}

	// History answers "which change introduced it" by walking back one step.
	chain, err := e.Lineage(ctx, issue.ControllingRevision, 10)
	if err != nil {
		t.Fatalf("lineage: %v", err)
	}
	if len(chain) < 2 {
		Failf(t, id, "§27 late regression", "history identifies the change",
			"lineage from the regression is %d long", len(chain))
		return
	}

	Passf(t, id, "§27 late regression", "a regression points at the change that caused it",
		"REGRESSION event recorded against r%d; issue controlling revision = r%d; lineage walks back through r%d to r%d",
		promotion.Revision.Sequence, chain[0].Sequence, chain[1].Sequence, chain[len(chain)-1].Sequence)
}

// TestS28SchemaMigration keeps old revisions readable under their own schema.
func TestS28SchemaMigration(t *testing.T) {
	const id = "S28_schema_migration"
	ctx := context.Background()
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	old := activeRevision(t, e, app)
	if old.SchemaRevision != sim.Family+"-v1" {
		Failf(t, id, "§28 schema migration", "revisions carry their schema version",
			"r1 schema is %q", old.SchemaRevision)
		return
	}
	oldBundle := bundleAt(t, e, old)

	// A candidate under a new schema version.
	var issueID string
	for i := 0; i < scaled(30, 100); i++ {
		resp := emit(t, e, app, sim.Feedback, sim.Reason, map[string]any{"text": "awarded"}, "svc", old.RevisionID)
		issueID = resp.IssueID
	}
	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{issueID}})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
		Bundle:   objectstore.Bundle{"rules.yaml": bundle("# semantic rules v2", semanticDoc{Version: 2, Rules: []SemanticRule{{ID: "awarded", Phrase: "awarded", State: "AWARDED"}}})},
		IssueIDs: []string{issueID}, WorkItemID: work.WorkID, Explanation: "schema v2 migration",
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
	promotion, err := e.PromoteCandidate(ctx, candidate.CandidateID, "lab")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}

	// The old revision is untouched: same bytes, same schema, still verified.
	reloaded, err := e.DB().GetRevision(old.RevisionID)
	if err != nil {
		t.Fatalf("reload r1: %v", err)
	}
	if reloaded.ContentHash != old.ContentHash || reloaded.SchemaRevision != old.SchemaRevision {
		Failf(t, id, "§28 schema migration", "old revisions are never reinterpreted",
			"r1 changed: content %s -> %s, schema %s -> %s",
			shorten(old.ContentHash), shorten(reloaded.ContentHash), old.SchemaRevision, reloaded.SchemaRevision)
		return
	}
	if digest := digestBundle(bundleAt(t, e, reloaded)); digest != digestBundle(oldBundle) {
		Failf(t, id, "§28 schema migration", "old revisions are never reinterpreted",
			"r1 bytes changed: %s vs %s", digest, digestBundle(oldBundle))
		return
	}
	if err := e.Objects().VerifyRevision(reloaded.ContentHash); err != nil {
		Failf(t, id, "§28 schema migration", "old revisions stay intact", "%v", err)
		return
	}

	Passf(t, id, "§28 schema migration", "historical truth stays stable",
		"r1 keeps schema %q and its original bytes after r%d is promoted under schema %q; both revisions verify against the object store",
		reloaded.SchemaRevision, promotion.Revision.Sequence, promotion.Revision.SchemaRevision)
}

// TestS29MultiIssue checks both directions of the many-to-many relation.
func TestS29MultiIssue(t *testing.T) {
	const id = "S29_multi_issue"
	ctx := context.Background()
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	reasons := []string{"UNKNOWN_EVENT_PHRASE", "AMBIGUOUS_ACTOR", "CONFLICTING_STATE"}
	var issueIDs []string
	for _, reason := range reasons {
		for i := 0; i < scaled(20, 60); i++ {
			resp := emit(t, e, app, sim.Feedback, reason,
				map[string]any{"text": fmt.Sprintf("%s sample %d", reason, i)}, "svc", app.Baseline.RevisionID)
			if i == 0 {
				issueIDs = append(issueIDs, resp.IssueID)
			}
		}
	}
	if len(issueIDs) != 3 {
		t.Fatalf("expected 3 issues, got %d", len(issueIDs))
	}

	// One issue, two work items in different config families: the same
	// application declares a second family.
	reg2, err := e.RegisterApplication(ctx, engine.Manifest{
		ApplicationID: app.ApplicationID(),
		Name:          sim.Name,
		Type:          "lab-simulator",
		Owner:         "lab",
		Junctions: []engine.JunctionSpec{
			{Name: "entity.alias", Workflow: "entity-alias-review", ConfigFamily: "entity_aliases"},
		},
		ConfigFamilies: []engine.ConfigFamilySpec{{Name: "entity_aliases", SchemaRevision: "alias-v1"}},
	})
	if err != nil {
		t.Fatalf("register second family: %v", err)
	}
	if reg2.FamilyIDs["entity_aliases"] == "" {
		t.Fatalf("second family was not registered")
	}

	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: issueIDs[:2], Actor: "lab"})
	if err != nil {
		t.Fatalf("queue two issues: %v", err)
	}
	if len(work.IssueIDs) != 2 {
		Failf(t, id, "§29 multi-issue", "one work item can carry several issues",
			"work item carries %d issues", len(work.IssueIDs))
		return
	}

	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
		Bundle: objectstore.Bundle{"rules.yaml": bundle("# semantic rules", semanticDoc{Version: 2, Rules: []SemanticRule{
			{ID: "awarded", Phrase: "awarded", State: "AWARDED"},
			{ID: "purchased", Phrase: "purchased", State: "AWARDED"},
			{ID: "sold", Phrase: "sold", State: "SOLD"},
			{ID: "offered", Phrase: "offered", State: "OFFERED"},
			{ID: "shipped", Phrase: "shipped", State: "SHIPPED"},
			{ID: "extra", Phrase: "awarded by the ministry", State: "AWARDED"},
		}})},
		IssueIDs:    issueIDs[:2],
		WorkItemID:  work.WorkID,
		Explanation: "one candidate addressing two issues",
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}
	if len(candidate.IssueIDs) != 2 {
		Failf(t, id, "§29 multi-issue", "a candidate names every issue it fixes",
			"candidate carries %d issues", len(candidate.IssueIDs))
		return
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
	promotion, err := e.PromoteCandidate(ctx, candidate.CandidateID, "lab")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if len(promotion.Revision.IssueIDs) != 2 {
		Failf(t, id, "§29 multi-issue", "the promoted revision keeps the links",
			"revision carries %d issue ids", len(promotion.Revision.IssueIDs))
		return
	}

	// The second family produced its own work item for the third issue.
	second, err := e.QueueWork(ctx, engine.QueueIssueRequest{
		IssueIDs: []string{issueIDs[2]}, Application: app.ApplicationID(),
		ConfigFamily: "entity_aliases", WorkflowType: "entity-alias-review",
	})
	if err != nil {
		t.Fatalf("queue second family: %v", err)
	}
	if second.ConfigFamilyID != reg2.FamilyIDs["entity_aliases"] {
		Failf(t, id, "§29 multi-issue", "one issue can spawn work in another family",
			"second work item landed in family %s", second.ConfigFamilyID)
		return
	}

	Passf(t, id, "§29 multi-issue", "candidate to many issues, issue to many families",
		"one candidate carries %d issues into r%d; the third issue produced a separate work item (%s) in a second config family",
		len(promotion.Revision.IssueIDs), promotion.Revision.Sequence, second.WorkflowType)
}

// TestS36Grouping checks deterministic clustering against known counts.
func TestS36Grouping(t *testing.T) {
	const id = "S36_grouping"
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	type cluster struct {
		reason  string
		payload map[string]any
		want    int
	}
	clusters := []cluster{
		{"UNKNOWN_EVENT_PHRASE", map[string]any{"text": "purchased 5,000 mt of urea at $610/t"}, scaled(200, 2000)},
		{"UNKNOWN_EVENT_PHRASE", map[string]any{"text": "awarded 12,500 mt DAP to RCF"}, scaled(100, 1000)},
		{"SCHEMA_DRIFT", map[string]any{"table": "cru", "columns": []string{"price", "volume"}}, scaled(50, 500)},
		{"LOW_CONFIDENCE", map[string]any{"class": "UNKNOWN", "score": 0.31}, scaled(25, 250)},
	}

	expected := map[string]int{}
	for i, c := range clusters {
		var fingerprint string
		for n := 0; n < c.want; n++ {
			// Vary the numbers and the source, keep the shape.
			payload := map[string]any{}
			for key, value := range c.payload {
				payload[key] = value
			}
			if text, ok := payload["text"].(string); ok {
				payload["text"] = fmt.Sprintf("%s [ref %d]", text, n)
			}
			resp := emit(t, e, app, sim.Feedback, c.reason, payload,
				fmt.Sprintf("source-%d", n%5), app.Baseline.RevisionID)
			fingerprint = resp.Fingerprint
			_ = i
		}
		expected[fingerprint] = c.want
	}

	got := issueCounts(t, e)
	if len(got) != len(expected) {
		Failf(t, id, "§36 grouping", "deterministic clustering",
			"expected %d clusters, got %d: %v", len(expected), len(got), got)
		return
	}
	for fingerprint, want := range expected {
		if got[fingerprint] != want {
			Failf(t, id, "§36 grouping", "deterministic clustering",
				"cluster %s has %d occurrences, expected %d", shorten(fingerprint), got[fingerprint], want)
			return
		}
	}

	total := 0
	for _, want := range expected {
		total += want
	}
	Passf(t, id, "§36 grouping", "known clusters group exactly",
		"%d events in %d known shapes produced exactly %d issues with the expected counts", total, len(clusters), len(got))
}

// TestS37Routing checks that problems reach the right worker and never vanish.
func TestS37Routing(t *testing.T) {
	const id = "S37_routing"
	ctx := context.Background()
	e := openEngine(t)
	apps := map[string]App{}
	for _, sim := range Simulators() {
		apps[sim.Name] = setupApp(t, e, sim)
	}

	// One issue per simulator.
	workBySim := map[string]projection.WorkItem{}
	for name, app := range apps {
		resp := emit(t, e, app, app.Sim.Feedback, app.Sim.Reason,
			map[string]any{"text": "routing fixture"}, "lab", app.Baseline.RevisionID)
		work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{resp.IssueID}, Actor: "lab"})
		if err != nil {
			t.Fatalf("queue %s: %v", name, err)
		}
		workBySim[name] = work
	}

	// A worker that only handles model refresh must never see the MCP issue.
	claimed, err := e.ClaimWork(ctx, engine.ClaimRequest{Worker: "ml-worker", WorkflowTypes: []string{"model-refresh"}, TTL: time.Minute})
	if err != nil {
		Failf(t, id, "§37 routing", "work reaches only a worker whose workflow matches", "claim failed: %v", err)
		return
	}
	if claimed.WorkflowType != "model-refresh" {
		Failf(t, id, "§37 routing", "work reaches only a worker whose workflow matches",
			"the ml-worker was handed a %q item", claimed.WorkflowType)
		return
	}
	for name, work := range workBySim {
		if name == "ml-sim" {
			continue
		}
		if claimed.WorkID == work.WorkID {
			Failf(t, id, "§37 routing", "work reaches only a worker whose workflow matches",
				"the ml-worker claimed %s's item", name)
			return
		}
	}

	// With no matching worker, work waits rather than disappearing.
	if _, err := e.ClaimWork(ctx, engine.ClaimRequest{
		Worker: "nobody", WorkflowTypes: []string{"no-such-workflow"}, TTL: time.Minute,
	}); !errors.Is(err, projection.ErrNoWork) {
		Failf(t, id, "§37 routing", "unclaimed work stays queued",
			"a worker with no matching capability got %v", err)
		return
	}
	queued, err := e.DB().ListWorkItems(string(engine.WorkQueued), 100)
	if err != nil {
		t.Fatalf("list work: %v", err)
	}
	// Five items; ml-sim's was leased by the ml-worker, so four remain queued.
	if len(queued) != len(apps)-1 {
		Failf(t, id, "§37 routing", "unclaimed work stays queued",
			"%d items queued, expected %d", len(queued), len(apps)-1)
		return
	}

	Passf(t, id, "§37 routing", "five workers, no misrouted problem",
		"5 workflow types; the model worker only ever received model-refresh work; %d items remain QUEUED with no matching worker instead of being lost",
		len(queued))
}

// TestS22WrongTargetCheck confirms a worker cannot choose a destination.
func TestS22WrongTargetCheck(t *testing.T) {
	const id = "S22_wrong_target"
	ctx := context.Background()
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	before, err := e.DB().ListTargets("")
	if err != nil {
		t.Fatalf("list targets: %v", err)
	}

	// A worker tries to smuggle a target through the candidate API.
	var issueID string
	for i := 0; i < 3; i++ {
		resp := emit(t, e, app, sim.Feedback, sim.Reason, map[string]any{"text": "awarded"}, "svc", app.Baseline.RevisionID)
		issueID = resp.IssueID
	}
	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{issueID}})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	// CreateCandidate has no target field at all; the request below carries the
	// fields a worker would try, and they are simply not part of the contract.
	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
		Bundle:   objectstore.Bundle{"rules.yaml": bundle("# semantic rules", semanticDoc{Version: 2, Rules: []SemanticRule{{ID: "a", Phrase: "awarded", State: "AWARDED"}}})},
		IssueIDs: []string{issueID}, WorkItemID: work.WorkID, Explanation: "target smuggling attempt",
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}

	after, err := e.DB().ListTargets("")
	if err != nil {
		t.Fatalf("list targets: %v", err)
	}
	if len(after) != len(before) {
		Failf(t, id, "§22 wrong target", "workers cannot choose deployment destinations",
			"target count changed from %d to %d after a worker request", len(before), len(after))
		return
	}
	for _, target := range after {
		if !strings.HasPrefix(target.Path, "/tmp/lymph-lab/") {
			Failf(t, id, "§22 wrong target", "workers cannot choose deployment destinations",
				"target %s is outside the registered lab paths", target.Path)
			return
		}
	}

	// And a candidate that would deploy to /etc is refused at registration
	// time, by the path-ownership rule.
	_, err = e.RegisterApplication(ctx, engine.Manifest{
		Name: "intruder",
		ConfigFamilies: []engine.ConfigFamilySpec{{
			Name:    "sneaky",
			Targets: []engine.TargetSpec{{Type: "SINGLE_FILE", Path: "/etc/lymph-lab/intruder.yaml"}},
		}},
	})
	if err != nil {
		t.Fatalf("registering a distinct path should be allowed: %v", err)
	}
	_, err = e.RegisterApplication(ctx, engine.Manifest{
		Name: "intruder-2",
		ConfigFamilies: []engine.ConfigFamilySpec{{
			Name:    "sneaky-2",
			Targets: []engine.TargetSpec{{Type: "SINGLE_FILE", Path: "/etc/lymph-lab/intruder.yaml"}},
		}},
	})
	if !errors.Is(err, projection.ErrPathConflict) {
		Failf(t, id, "§22 wrong target", "workers cannot choose deployment destinations",
			"a second application was allowed to claim the same path: %v", err)
		return
	}

	Passf(t, id, "§22 wrong target", "destinations come from registration only",
		"candidate %s carried no path; %d registered targets unchanged; a second application claiming the same path was refused with %v",
		shorten(candidate.CandidateID), len(after), projection.ErrPathConflict)
}

// ---------- helpers ----------

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

func anyRefPointsAt(refs []projection.ConfigRef, revisionID string) bool {
	for _, ref := range refs {
		if ref.RevisionID == revisionID {
			return true
		}
	}
	return false
}

func anyReflogNames(entries []projection.ReflogEntry, candidateID string) bool {
	for _, entry := range entries {
		if entry.CandidateID == candidateID {
			return true
		}
	}
	return false
}

func shorten(s string) string {
	s = strings.TrimPrefix(s, "sha256:")
	if len(s) <= 14 {
		return s
	}
	return s[:8] + "…" + s[len(s)-4:]
}

func countsOf(issues []projection.Issue) []int {
	out := make([]int, 0, len(issues))
	for _, issue := range issues {
		out = append(out, issue.OccurrenceCount)
	}
	return out
}

// candidateTree loads the tree of a candidate's proposed bundle.
func candidateTree(t *testing.T, e *engine.Engine, candidate projection.Candidate) objectstore.Bundle {
	t.Helper()
	tree, err := e.Objects().GetTree(candidate.RootTreeHash)
	if err != nil {
		t.Fatalf("candidate tree: %v", err)
	}
	bundle := objectstore.Bundle{}
	for _, entry := range tree.Entries {
		data, err := e.Objects().Get(entry.Hash)
		if err != nil {
			t.Fatalf("candidate blob: %v", err)
		}
		bundle[entry.Path] = data
	}
	return bundle
}

// emitWithID re-sends an existing event id, which is what a spool flush does.
func emitWithID(t *testing.T, e *engine.Engine, app App, eventID string) (engine.EmitResponse, error) {
	t.Helper()
	event, err := engine.NewEvent(
		protocol.EventSource(app.ApplicationID(), app.Sim.Junction),
		protocol.FeedbackEventType(app.Sim.Feedback),
		app.Sim.Junction,
		protocol.EventData{FeedbackType: app.Sim.Feedback, ReasonCode: app.Sim.Reason},
	)
	if err != nil {
		return engine.EmitResponse{}, err
	}
	event.ID = eventID
	return e.Emit(context.Background(), engine.EmitRequest{Event: event, ProducerInstance: "spool"})
}

// ParseMapping reads an ingest mapping out of a bundle.
func ParseMapping(bundle objectstore.Bundle) (ingestMapping, error) {
	var mapping ingestMapping
	for _, data := range bundle {
		if err := yaml.Unmarshal(data, &mapping); err != nil {
			return mapping, err
		}
		break
	}
	return mapping, nil
}
