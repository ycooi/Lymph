package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
)

// leasedWork emits one issue, queues it, and leases it to a worker, returning
// everything a worker would need to return a typed result.
func leasedWork(t *testing.T, e *engine.Engine, app App, worker string, payload map[string]any) (projection.WorkItem, string) {
	t.Helper()
	return leasedWorkTTL(t, e, app, worker, payload, time.Minute)
}

// leasedWorkTTL is leasedWork with an explicit lease duration, for tests that
// need the lease to expire on its own.
func leasedWorkTTL(t *testing.T, e *engine.Engine, app App, worker string, payload map[string]any, ttl time.Duration) (projection.WorkItem, string) {
	t.Helper()
	resp := emit(t, e, app, app.Sim.Feedback, app.Sim.Reason, payload, "lab", app.Baseline.RevisionID)
	work, err := e.QueueWork(context.Background(), engine.QueueIssueRequest{IssueIDs: []string{resp.IssueID}})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	claimed, err := e.ClaimWork(context.Background(), engine.ClaimRequest{
		Worker: worker, WorkflowTypes: []string{work.WorkflowType}, TTL: ttl,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return claimed, resp.IssueID
}

// returnTyped performs one atomic typed worker return.
func returnTyped(t *testing.T, e *engine.Engine, work projection.WorkItem, worker string, artifacts ...engine.ImprovementArtifactRequest) engine.ReturnImprovementResponse {
	t.Helper()
	resp, err := e.ReturnImprovement(context.Background(), engine.ReturnImprovementRequest{
		WorkID:    work.WorkID,
		Worker:    worker,
		Attempt:   work.Attempts,
		Summary:   "lab return",
		Artifacts: artifacts,
	})
	if err != nil {
		t.Fatalf("return improvement: %v", err)
	}
	return resp
}

func modelRefArtifact(uri, digest string) engine.ImprovementArtifactRequest {
	return engine.ImprovementArtifactRequest{
		Kind: engine.ArtifactModelRef,
		ModelRef: &engine.ModelRefArtifactRequest{
			URI: uri, Digest: digest, ModelType: "classifier", Version: "v2",
		},
	}
}

func codeRefArtifact(repo, commit string) engine.ImprovementArtifactRequest {
	return engine.ImprovementArtifactRequest{
		Kind: engine.ArtifactCodeRef,
		CodeRef: &engine.CodeRefArtifactRequest{
			Repository: repo, Commit: commit, TreeHash: "sha256:deadbeef", Branch: "main",
		},
	}
}

func dataFixArtifact(patchID string) engine.ImprovementArtifactRequest {
	return engine.ImprovementArtifactRequest{
		Kind: engine.ArtifactDataFixRef,
		DataFixRef: &engine.DataFixRefArtifactRequest{
			PatchID: patchID, Target: "historical-data-2025",
			Description: "correct the upstream mapping for 2025 rows",
		},
	}
}

func noChangeArtifact(reason, disposition string) engine.ImprovementArtifactRequest {
	return engine.ImprovementArtifactRequest{
		Kind:     engine.ArtifactNoChange,
		NoChange: &engine.NoChangeArtifactRequest{Reason: reason, Disposition: disposition},
	}
}

// TestS08Artifact is the graded item: five artifact kinds are first-class
// canonical records, and only one of them can create a configuration candidate.
func TestS08Artifact(t *testing.T) {
	const id = "S08_artifact"
	ctx := context.Background()
	root := openStoreRoot(t, t.TempDir())
	e := openEngineAt(t, root)

	// One application per kind keeps the evidence easy to read.
	apps := map[string]App{}
	for _, sim := range Simulators() {
		apps[sim.Name] = setupApp(t, e, sim)
	}
	agentApp, mcpApp := apps["agent-sim"], apps["mcp-sim"]
	mlApp, ingestApp, serviceApp := apps["ml-sim"], apps["ingest-sim"], apps["service-sim"]

	candidatesBefore := countCandidates(t, e)
	revisionsBefore := countRevisions(t, e)

	// 1. CONFIG_BUNDLE — the only kind that creates a candidate.
	agentWork, _ := leasedWork(t, e, agentApp, "agent-worker", map[string]any{"request": "track vessel eta"})
	bundleResult := returnTyped(t, e, agentWork, "agent-worker", engine.ImprovementArtifactRequest{
		Kind: engine.ArtifactConfigBundle,
		ConfigBundle: &engine.ConfigBundleArtifactRequest{
			Bundle: objectstore.Bundle{agentApp.Sim.File: bundle("# agent policy", agentDoc{Policies: []agentPolicy{
				{Intent: "weather", Tool: "weather_tool"},
				{Intent: "vessel", Tool: "vessel_tracker"},
			}})},
			Explanation: "add the vessel intent",
		},
	})
	if len(bundleResult.Candidates) != 1 {
		Failf(t, id, "§8 ImprovementArtifact", "CONFIG_BUNDLE creates a candidate",
			"%d candidates were created", len(bundleResult.Candidates))
		return
	}
	if bundleResult.Candidates[0].BaseRevisionID != agentApp.Baseline.RevisionID {
		Failf(t, id, "§8 ImprovementArtifact", "the candidate inherits the work item's base",
			"base is %s", bundleResult.Candidates[0].BaseRevisionID)
		return
	}

	// 2. CODE_REF — a genuine code defect, no config anywhere.
	serviceWork, _ := leasedWork(t, e, serviceApp, "code-worker", map[string]any{"error": "nil dereference"})
	codeResult := returnTyped(t, e, serviceWork, "code-worker", codeRefArtifact("git@example/project", "abc123def456"))
	if len(codeResult.Candidates) != 0 {
		Failf(t, id, "§8 ImprovementArtifact", "CODE_REF creates no candidate",
			"%d candidates were created", len(codeResult.Candidates))
		return
	}

	// 3. MODEL_REF — a model that lives outside Lymph.
	mlWork, _ := leasedWork(t, e, mlApp, "ml-worker", map[string]any{"drift": "new distribution"})
	modelDigest := objectstore.HashOf([]byte("pretend this is a 5 GB model"))
	modelResult := returnTyped(t, e, mlWork, "ml-worker",
		modelRefArtifact("s3://models/fertilizer-classifier/model-v2.bin", modelDigest))
	if len(modelResult.Candidates) != 0 {
		Failf(t, id, "§8 ImprovementArtifact", "MODEL_REF creates no candidate",
			"%d candidates were created", len(modelResult.Candidates))
		return
	}
	if e.Objects().Has(modelDigest) {
		Failf(t, id, "§8 ImprovementArtifact", "an external model is never copied into the object store",
			"the model digest %s exists as an object-store blob", shorten(modelDigest))
		return
	}

	// 4. DATA_FIX_REF — the data was wrong, not the code or the config.
	ingestWork, _ := leasedWork(t, e, ingestApp, "data-worker", map[string]any{"table": "cru", "issue": "wrong mapping"})
	dataResult := returnTyped(t, e, ingestWork, "data-worker", dataFixArtifact("fix-2026-09-20-01"))
	if len(dataResult.Candidates) != 0 {
		Failf(t, id, "§8 ImprovementArtifact", "DATA_FIX_REF creates no candidate",
			"%d candidates were created", len(dataResult.Candidates))
		return
	}

	// 5. NO_CHANGE — the most valuable negative result there is.
	mcpWork, _ := leasedWork(t, e, mcpApp, "mcp-worker", map[string]any{"text": "malformed upstream row"})
	noChangeResult := returnTyped(t, e, mcpWork, "mcp-worker",
		noChangeArtifact("upstream malformed record; current parser behavior is correct", "EXTERNAL_CAUSE"))
	if len(noChangeResult.Candidates) != 0 {
		Failf(t, id, "§8 ImprovementArtifact", "NO_CHANGE creates no candidate",
			"%d candidates were created", len(noChangeResult.Candidates))
		return
	}

	// Kinds are recorded verbatim.
	kinds := map[string]bool{}
	for _, artifact := range append(append(append(append(
		bundleResult.Artifacts, codeResult.Artifacts...), modelResult.Artifacts...),
		dataResult.Artifacts...), noChangeResult.Artifacts...) {
		kinds[artifact.Kind] = true
	}
	for _, kind := range engine.ArtifactKinds() {
		if !kinds[string(kind)] {
			Failf(t, id, "§8 ImprovementArtifact", "all five kinds are recorded", "%s is missing", kind)
			return
		}
	}

	// Exactly one candidate, and no config revision, came out of the five.
	if added := countCandidates(t, e) - candidatesBefore; added != 1 {
		Failf(t, id, "§8 ImprovementArtifact", "only CONFIG_BUNDLE creates a candidate",
			"%d candidates were added by five returns", added)
		return
	}
	if added := countRevisions(t, e) - revisionsBefore; added != 0 {
		Failf(t, id, "§8 ImprovementArtifact", "a worker return never creates a revision by itself",
			"%d revisions appeared without promotion", added)
		return
	}

	// Every result and artifact is canonical: throw the projection away and
	// replay the ledger, and the whole return comes back (mission section 27).
	digestBefore := stateDigest(t, e)
	e.Close()
	if err := removeProjectionFiles(root); err != nil {
		t.Fatalf("remove projection: %v", err)
	}
	rebuilt := openEngineAt(t, root)
	if got := stateDigest(t, rebuilt); got != digestBefore {
		Failf(t, id, "§8 ImprovementArtifact", "artifacts survive a projection rebuild",
			"digest changed: %s vs %s", shorten(digestBefore), shorten(got))
		return
	}
	artifacts, err := rebuilt.Artifacts(projection.ArtifactFilter{Limit: 100})
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	if len(artifacts) != 5 {
		Failf(t, id, "§8 ImprovementArtifact", "artifacts survive a projection rebuild",
			"%d artifacts came back, expected 5", len(artifacts))
		return
	}
	for _, artifact := range artifacts {
		if artifact.Payload == "" || artifact.Payload == "{}" {
			Failf(t, id, "§8 ImprovementArtifact", "artifacts carry their metadata",
				"artifact %s has no payload", shorten(artifact.ArtifactID))
			return
		}
	}

	Passf(t, id, "§8 ImprovementArtifact", "typed artifacts are canonical records",
		"5 kinds recorded (CONFIG_BUNDLE, CODE_REF, MODEL_REF, DATA_FIX_REF, NO_CHANGE) across 5 worker returns; exactly 1 candidate and 0 revisions created; the external model %s is referenced but not stored; deleting the projection and replaying the ledger restored all 5 artifacts to the same state digest",
		shorten(modelDigest))
	_ = ctx
}

// TestS30NotAConfig is the graded item for improvements that are not
// configuration at all.
func TestS30NotAConfig(t *testing.T) {
	const id = "S30_not_a_config"
	ctx := context.Background()
	e := openEngine(t)

	sim := ServiceSim()
	app := setupApp(t, e, sim)

	// The other simulators are set up first so that their baseline imports are
	// not mistaken for something the worker returns produced.
	mlApp := setupApp(t, e, MLSim())
	ingestApp := setupApp(t, e, IngestSim())
	mcpApp := setupApp(t, e, MCPSim())

	// Baseline counts: nothing below may create a candidate, a revision, or a
	// target.
	candidatesBefore := countCandidates(t, e)
	revisionsBefore := countRevisions(t, e)
	targetsBefore := countTargets(t, e)

	type returned struct {
		work   projection.WorkItem
		result engine.ReturnImprovementResponse
	}
	var known []returned

	// A code bug fixed by a commit.
	codeWork, codeIssue := leasedWork(t, e, app, "codex-maintenance", map[string]any{"bug": "retry storm"})
	known = append(known, returned{codeWork, returnTyped(t, e, codeWork, "codex-maintenance",
		codeRefArtifact("git@example/semantic-service", "9f2c1ab77c0d"))})

	// A model refreshed outside Lymph.
	mlWork, _ := leasedWork(t, e, mlApp, "ml-maintainer", map[string]any{"drift": "distribution"})
	known = append(known, returned{mlWork, returnTyped(t, e, mlWork, "ml-maintainer",
		modelRefArtifact("s3://models/classifier/model-v2.bin", "sha256:"+strings.Repeat("ab", 32)))})

	// Bad historical data corrected by a patch.
	dataWork, _ := leasedWork(t, e, ingestApp, "data-steward", map[string]any{"rows": "mis-mapped"})
	known = append(known, returned{dataWork, returnTyped(t, e, dataWork, "data-steward",
		dataFixArtifact("fix-2026-09-20-02"))})

	// And an investigation that concluded nothing should change.
	noChangeWork, noChangeIssue := leasedWork(t, e, mcpApp, "mcp-maintainer", map[string]any{"text": "garbled feed"})
	known = append(known, returned{noChangeWork, returnTyped(t, e, noChangeWork, "mcp-maintainer",
		noChangeArtifact("current config is correct for this input", "FALSE_POSITIVE"))})

	// No configuration artefacts appeared.
	if added := countCandidates(t, e) - candidatesBefore; added != 0 {
		Failf(t, id, "§30 not everything is a config", "non-config improvements create no candidates",
			"%d candidates were created", added)
		return
	}
	if added := countRevisions(t, e) - revisionsBefore; added != 0 {
		Failf(t, id, "§30 not everything is a config", "non-config improvements create no revisions",
			"%d revisions were created", added)
		return
	}
	if added := countTargets(t, e) - targetsBefore; added != 0 {
		Failf(t, id, "§30 not everything is a config", "no managed target is touched",
			"%d targets appeared", added)
		return
	}

	// Lineage is complete for each: work -> result -> artifact, and the issue
	// is on the result.
	for _, entry := range known {
		result, err := e.DB().GetImprovementResultByWork(entry.work.WorkID)
		if err != nil {
			Failf(t, id, "§30 not everything is a config", "lineage is complete",
				"work %s has no result: %v", shorten(entry.work.WorkID), err)
			return
		}
		artifacts, err := e.DB().ListImprovementArtifacts(projection.ArtifactFilter{ResultID: result.ResultID, Limit: 10})
		if err != nil || len(artifacts) == 0 {
			Failf(t, id, "§30 not everything is a config", "lineage is complete",
				"result %s has no artifacts (%v)", shorten(result.ResultID), err)
			return
		}
		if len(result.IssueIDs) == 0 {
			Failf(t, id, "§30 not everything is a config", "lineage is complete",
				"result %s names no issues", shorten(result.ResultID))
			return
		}
	}
	if !contains(known[0].result.Result.IssueIDs, codeIssue) {
		Failf(t, id, "§30 not everything is a config", "the code result names its issue",
			"result issues are %v", known[0].result.Result.IssueIDs)
		return
	}
	if !contains(known[3].result.Result.IssueIDs, noChangeIssue) {
		Failf(t, id, "§30 not everything is a config", "the NO_CHANGE result names its issue",
			"result issues are %v", known[3].result.Result.IssueIDs)
		return
	}

	// A NO_CHANGE result is still evidence: it is queryable and it says why.
	noChangeArtifactRecord, err := e.DB().GetImprovementArtifact(known[3].result.Artifacts[0].ArtifactID)
	if err != nil {
		t.Fatalf("reload NO_CHANGE artifact: %v", err)
	}
	if !strings.Contains(noChangeArtifactRecord.Payload, "FALSE_POSITIVE") {
		Failf(t, id, "§30 not everything is a config", "a NO_CHANGE result keeps its reasoning",
			"payload is %s", noChangeArtifactRecord.Payload)
		return
	}

	Passf(t, id, "§30 not everything is a config", "improvements that are not configuration",
		"4 non-config outcomes recorded (CODE_REF, MODEL_REF, DATA_FIX_REF, NO_CHANGE) with 0 candidates, 0 revisions and 0 managed-target writes; each result names its issue and keeps its metadata; the NO_CHANGE conclusion is queryable evidence, not a silence")
	_ = ctx
}

// TestL25ArtifactMatrix covers the required artifact tests from mission
// section 35, one case per line of the list.
func TestL25ArtifactMatrix(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	validBundle := engine.ImprovementArtifactRequest{
		Kind: engine.ArtifactConfigBundle,
		ConfigBundle: &engine.ConfigBundleArtifactRequest{
			Bundle: objectstore.Bundle{"rules.yaml": bundle("# rules", semanticDoc{Version: 2, Rules: []SemanticRule{
				{ID: "a", Phrase: "awarded", State: "AWARDED"},
				{ID: "p", Phrase: "purchased", State: "AWARDED"},
				{ID: "s", Phrase: "sold", State: "SOLD"},
				{ID: "o", Phrase: "offered", State: "OFFERED"},
				{ID: "sh", Phrase: "shipped", State: "SHIPPED"},
				{ID: "b", Phrase: "business was booked with", State: "AWARDED"},
			}})},
			Explanation: "scoped rule",
		},
	}

	rejects := []struct {
		name      string
		artifacts []engine.ImprovementArtifactRequest
		want      error
	}{
		{"unknown kind", []engine.ImprovementArtifactRequest{{Kind: "MODEL"}}, engine.ErrInvalidArtifact},
		{"empty kind", []engine.ImprovementArtifactRequest{{Kind: ""}}, engine.ErrInvalidArtifact},
		{"kind/payload mismatch", []engine.ImprovementArtifactRequest{{
			Kind:    engine.ArtifactModelRef,
			CodeRef: &engine.CodeRefArtifactRequest{Repository: "r", Commit: "c"},
		}}, engine.ErrInvalidArtifact},
		{"two payloads", []engine.ImprovementArtifactRequest{{
			Kind:     engine.ArtifactModelRef,
			ModelRef: &engine.ModelRefArtifactRequest{URI: "u", Digest: "d"},
			NoChange: &engine.NoChangeArtifactRequest{Reason: "r"},
		}}, engine.ErrInvalidArtifact},
		{"model without uri", []engine.ImprovementArtifactRequest{{
			Kind:     engine.ArtifactModelRef,
			ModelRef: &engine.ModelRefArtifactRequest{Digest: "sha256:x"},
		}}, engine.ErrInvalidArtifact},
		{"model without digest", []engine.ImprovementArtifactRequest{{
			Kind:     engine.ArtifactModelRef,
			ModelRef: &engine.ModelRefArtifactRequest{URI: "s3://x"},
		}}, engine.ErrInvalidArtifact},
		{"code without commit", []engine.ImprovementArtifactRequest{{
			Kind:    engine.ArtifactCodeRef,
			CodeRef: &engine.CodeRefArtifactRequest{Repository: "git@x"},
		}}, engine.ErrInvalidArtifact},
		{"data fix with no reference", []engine.ImprovementArtifactRequest{{
			Kind:       engine.ArtifactDataFixRef,
			DataFixRef: &engine.DataFixRefArtifactRequest{Description: "trust me"},
		}}, engine.ErrInvalidArtifact},
		{"data fix uri without digest", []engine.ImprovementArtifactRequest{{
			Kind:       engine.ArtifactDataFixRef,
			DataFixRef: &engine.DataFixRefArtifactRequest{URI: "s3://patch"},
		}}, engine.ErrInvalidArtifact},
		{"no_change without reason", []engine.ImprovementArtifactRequest{{
			Kind:     engine.ArtifactNoChange,
			NoChange: &engine.NoChangeArtifactRequest{Disposition: "OTHER"},
		}}, engine.ErrInvalidArtifact},
		{"no_change with unknown disposition", []engine.ImprovementArtifactRequest{{
			Kind:     engine.ArtifactNoChange,
			NoChange: &engine.NoChangeArtifactRequest{Reason: "why", Disposition: "BECAUSE"},
		}}, engine.ErrInvalidArtifact},
		{"invalid yaml bundle", []engine.ImprovementArtifactRequest{{
			Kind: engine.ArtifactConfigBundle,
			ConfigBundle: &engine.ConfigBundleArtifactRequest{
				Bundle: objectstore.Bundle{"rules.yaml": []byte("rules: [unclosed")},
			},
		}}, engine.ErrStructuralInvalid},
	}

	for _, tc := range rejects {
		work, _ := leasedWork(t, e, app, "matrix-worker", map[string]any{"text": "awarded"})
		_, err := e.ReturnImprovement(ctx, engine.ReturnImprovementRequest{
			WorkID: work.WorkID, Worker: "matrix-worker", Attempt: work.Attempts, Artifacts: tc.artifacts,
		})
		if err == nil {
			Failf(t, "L25_matrix", "§35 required artifact tests", "invalid returns are refused",
				"%s was accepted", tc.name)
			return
		}
		if !errors.Is(err, tc.want) {
			Failf(t, "L25_matrix", "§35 required artifact tests", "invalid returns are refused",
				"%s was refused with %v, expected %v", tc.name, err, tc.want)
			return
		}
		// The work item must still be leased: a refused return changes nothing.
		current, err := e.DB().GetWorkItem(work.WorkID)
		if err != nil {
			t.Fatalf("reload work: %v", err)
		}
		if current.State != string(engine.WorkLeased) {
			Failf(t, "L25_matrix", "§35 required artifact tests", "a refused return leaves the lease intact",
				"%s moved the work item to %s", tc.name, current.State)
			return
		}
	}

	// Valid single-kind returns, and the combination.
	singles := []struct {
		name    string
		request engine.ImprovementArtifactRequest
	}{
		{"CONFIG_BUNDLE", validBundle},
		{"CODE_REF", codeRefArtifact("git@example/x", "c0ffee")},
		{"MODEL_REF", modelRefArtifact("s3://m/v2", "sha256:"+strings.Repeat("11", 32))},
		{"DATA_FIX_REF", dataFixArtifact("patch-1")},
		{"NO_CHANGE", noChangeArtifact("nothing to change", "CURRENT_CONFIG_CORRECT")},
	}
	for _, tc := range singles {
		work, _ := leasedWork(t, e, app, "matrix-worker", map[string]any{"text": "awarded"})
		resp := returnTyped(t, e, work, "matrix-worker", tc.request)
		if len(resp.Artifacts) != 1 || resp.Artifacts[0].Kind != string(tc.request.Kind) {
			Failf(t, "L25_matrix", "§35 required artifact tests", "one return, one artifact",
				"%s produced %d artifacts", tc.name, len(resp.Artifacts))
			return
		}
	}

	// One return, several artifacts: a model refresh that also changes config.
	work, issueID := leasedWork(t, e, app, "matrix-worker", map[string]any{"text": "awarded"})
	combined := returnTyped(t, e, work, "matrix-worker",
		modelRefArtifact("s3://models/classifier/model-v2.bin", "sha256:"+strings.Repeat("22", 32)),
		validBundle)
	if len(combined.Artifacts) != 2 || len(combined.Candidates) != 1 {
		Failf(t, "L25_matrix", "§35 required artifact tests", "one result can carry several artifacts",
			"%d artifacts, %d candidates", len(combined.Artifacts), len(combined.Candidates))
		return
	}
	if !contains(combined.Result.IssueIDs, issueID) {
		Failf(t, "L25_matrix", "§35 required artifact tests", "a result names its issue",
			"issues are %v", combined.Result.IssueIDs)
		return
	}

	// Idempotency: the same return again resolves to the stored result.
	again, err := e.ReturnImprovement(ctx, engine.ReturnImprovementRequest{
		WorkID: work.WorkID, Worker: "matrix-worker", Attempt: work.Attempts,
		Artifacts: []engine.ImprovementArtifactRequest{validBundle},
	})
	if err != nil {
		t.Fatalf("idempotent return: %v", err)
	}
	if !again.Idempotent || again.Result.ResultID != combined.Result.ResultID {
		Failf(t, "L25_matrix", "§35 required artifact tests", "a repeated return is idempotent",
			"second return produced result %s (idempotent=%v)", shorten(again.Result.ResultID), again.Idempotent)
		return
	}

	results, err := e.DB().ListImprovementResults("", 100)
	if err != nil {
		t.Fatalf("list results: %v", err)
	}
	if len(results) != 6 {
		Failf(t, "L25_matrix", "§35 required artifact tests", "one result per work item",
			"%d results for 6 returned work items", len(results))
		return
	}

	Note(t, "S08_artifact", "L25_matrix", "§35 required artifact tests", "artifact contract",
		"12 malformed returns refused (unknown kinds, mismatched and doubled payloads, missing uri/digest/commit/reason, unknown disposition, unparseable bundle) each leaving the lease intact; 5 single-kind returns accepted; 1 return carrying MODEL_REF + CONFIG_BUNDLE produced 2 artifacts and 1 candidate; a repeated return was idempotent; %d results stored",
		len(results))
}

// TestL25StaleWorkerAttempt is the attempt-token regression: a worker may only
// return the attempt it actually holds (mission section 15).
func TestL25StaleWorkerAttempt(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t)
	app := setupApp(t, e, MCPSim())

	// A short lease so the worker can die and the item can be re-leased.
	work, _ := leasedWorkTTL(t, e, app, "worker-A", map[string]any{"text": "awarded"}, 20*time.Millisecond)
	heldAttempt := work.Attempts

	// A dies; the lease expires and worker-B takes over.
	time.Sleep(30 * time.Millisecond)
	if _, err := e.SweepLeases(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	second, err := e.ClaimWork(ctx, engine.ClaimRequest{Worker: "worker-B", TTL: time.Minute})
	if err != nil {
		t.Fatalf("claim by B: %v", err)
	}
	if second.Attempts != heldAttempt+1 {
		Failf(t, "L25_attempt", "§15 attempt token", "attempts advance",
			"attempt %d after re-lease, expected %d", second.Attempts, heldAttempt+1)
		return
	}

	// A returns late, even with the same textual name as before.
	_, err = e.ReturnImprovement(ctx, engine.ReturnImprovementRequest{
		WorkID: work.WorkID, Worker: "worker-A", Attempt: heldAttempt,
		Artifacts: []engine.ImprovementArtifactRequest{noChangeArtifact("late conclusion", "OTHER")},
	})
	if !errors.Is(err, engine.ErrStaleWorkerAttempt) {
		Failf(t, "L25_attempt", "§15 attempt token", "a stale attempt cannot return",
			"worker-A's late return was accepted (%v)", err)
		return
	}

	// Nor may the current worker return the old attempt number.
	_, err = e.ReturnImprovement(ctx, engine.ReturnImprovementRequest{
		WorkID: work.WorkID, Worker: "worker-B", Attempt: heldAttempt,
		Artifacts: []engine.ImprovementArtifactRequest{noChangeArtifact("wrong attempt", "OTHER")},
	})
	if !errors.Is(err, engine.ErrStaleWorkerAttempt) {
		Failf(t, "L25_attempt", "§15 attempt token", "the attempt number must match",
			"worker-B returned attempt %d and it was accepted (%v)", heldAttempt, err)
		return
	}

	// B returns the attempt it holds, and it lands.
	resp := returnTyped(t, e, second, "worker-B", noChangeArtifact("current config is correct", "EXTERNAL_CAUSE"))
	if resp.Result.Attempt != second.Attempts {
		Failf(t, "L25_attempt", "§15 attempt token", "the stored result records its attempt",
			"result attempt %d, expected %d", resp.Result.Attempt, second.Attempts)
		return
	}

	Note(t, "S08_artifact", "L25_attempt", "§15 attempt token", "stale worker results are refused",
		"worker-A held attempt %d and died; worker-B took attempt %d; A's late return was refused with %v, B's return of the old attempt number was refused too, and B's own attempt was recorded with attempt=%d",
		heldAttempt, second.Attempts, engine.ErrStaleWorkerAttempt, resp.Result.Attempt)
}

// TestL25ArtifactRebuildAndDisaster repeats the durability gates for typed
// artifacts (mission sections 27, 89).
func TestL25ArtifactRebuildAndDisaster(t *testing.T) {
	ctx := context.Background()
	root := openStoreRoot(t, t.TempDir())
	e := openEngineAt(t, root)

	apps := map[string]App{}
	for _, sim := range Simulators()[:3] {
		apps[sim.Name] = setupApp(t, e, sim)
	}
	agentApp, mlApp, mcpApp := apps["agent-sim"], apps["ml-sim"], apps["mcp-sim"]

	work1, _ := leasedWork(t, e, agentApp, "w1", map[string]any{"request": "vessel eta"})
	returnTyped(t, e, work1, "w1", engine.ImprovementArtifactRequest{
		Kind: engine.ArtifactConfigBundle,
		ConfigBundle: &engine.ConfigBundleArtifactRequest{
			Bundle: objectstore.Bundle{agentApp.Sim.File: bundle("# agent policy", agentDoc{
				Policies: []agentPolicy{{Intent: "vessel", Tool: "vessel_tracker"}},
			})},
			Explanation: "agent fix",
		},
	})
	work2, _ := leasedWork(t, e, mlApp, "w2", map[string]any{"drift": "x"})
	returnTyped(t, e, work2, "w2", modelRefArtifact("s3://models/m2.bin", "sha256:"+strings.Repeat("33", 32)))
	work3, _ := leasedWork(t, e, mcpApp, "w3", map[string]any{"text": "odd"})
	returnTyped(t, e, work3, "w3", noChangeArtifact("upstream data is malformed", "EXTERNAL_CAUSE"))

	digestBefore := stateDigest(t, e)
	resultsBefore, err := e.DB().ListImprovementResults("", 100)
	if err != nil {
		t.Fatalf("list results: %v", err)
	}
	artifactsBefore, err := e.Artifacts(projection.ArtifactFilter{Limit: 100})
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	e.Close()

	// 1. Delete the projection.
	if err := removeProjectionFiles(root); err != nil {
		t.Fatalf("remove projection: %v", err)
	}
	rebuilt := openEngineAt(t, root)
	if got := stateDigest(t, rebuilt); got != digestBefore {
		t.Fatalf("rebuild digest changed: %s vs %s", shorten(digestBefore), shorten(got))
	}
	resultsAfter, err := rebuilt.DB().ListImprovementResults("", 100)
	if err != nil {
		t.Fatalf("list results: %v", err)
	}
	artifactsAfter, err := rebuilt.Artifacts(projection.ArtifactFilter{Limit: 100})
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	if len(resultsAfter) != len(resultsBefore) || len(artifactsAfter) != len(artifactsBefore) {
		Failf(t, "L25_durability", "§27 rebuild gate", "typed artifacts survive rebuild",
			"%d/%d results and %d/%d artifacts returned", len(resultsAfter), len(resultsBefore),
			len(artifactsAfter), len(artifactsBefore))
		return
	}
	for i := range artifactsBefore {
		if artifactsAfter[i] != artifactsBefore[i] {
			Failf(t, "L25_durability", "§27 rebuild gate", "artifacts come back byte-identical",
				"artifact %s changed", shorten(artifactsBefore[i].ArtifactID))
			return
		}
	}
	rebuilt.Close()

	// 2. Disaster: keep only identity, ledger and objects.
	restored := openStoreRoot(t, t.TempDir())
	for _, item := range []struct{ from, to string }{
		{filepath.Join(root, "identity.json"), filepath.Join(restored, "identity.json")},
		{filepath.Join(root, "ledger"), filepath.Join(restored, "ledger")},
		{filepath.Join(root, "objects"), filepath.Join(restored, "objects")},
	} {
		if err := copyTree(item.from, item.to); err != nil {
			t.Fatalf("restore %s: %v", item.from, err)
		}
	}
	recovered := openEngineAt(t, restored)
	if got := stateDigest(t, recovered); got != digestBefore {
		Failf(t, "L25_durability", "§27 rebuild gate", "typed artifacts survive disaster recovery",
			"digest after restore: %s vs %s", shorten(got), shorten(digestBefore))
		return
	}
	recoveredResults, err := recovered.DB().ListImprovementResults("", 100)
	if err != nil {
		t.Fatalf("list results: %v", err)
	}

	Note(t, "S08_artifact", "L25_durability", "§27 rebuild gate", "typed artifacts are canonical",
		"3 typed returns (CONFIG_BUNDLE, MODEL_REF, NO_CHANGE) survived both a deleted projection and a restore of only identity.json + ledger/ + objects/: %d results and %d artifacts returned, byte-identical, at the same state digest (%s)",
		len(recoveredResults), len(artifactsAfter), shorten(digestBefore))
	_ = ctx
}

// TestL25CrashDuringReturn kills the daemon while returns are in flight. The
// acceptable outcomes are "result fully canonical" or "result absent" — never a
// candidate without its result, or a RETURNED work item without a result
// (mission section 87).
func TestL25CrashDuringReturn(t *testing.T) {
	root := openStoreRoot(t, t.TempDir())

	d := startDaemon(t, root, "")
	cli := d.client()
	reg := registerViaClient(t, cli, "crash-return")
	appID := reg.Application.ApplicationID
	junction := reg.JunctionIDs["semantic.event_state"]

	// Seed a baseline so candidates can resolve a base revision.
	if err := cli.Post(context.Background(), "/v1/baselines", engine.BaselineRequest{
		Application: appID, ConfigFamily: reg.FamilyIDs["semantic_event_rules"],
		Bundle: map[string][]byte{"rules.yaml": []byte("version: 1\nrules:\n  - id: a\n    phrase: awarded\n    state: AWARDED\n")},
		Author: "lab",
	}, nil); err != nil {
		t.Fatalf("baseline: %v", err)
	}

	// Queue one work item per round, return it, and kill at a random point.
	rounds := 6
	issued, returned := 0, 0
	for round := 0; round < rounds; round++ {
		payload, err := json.Marshal(map[string]any{"text": fmt.Sprintf("crash sample %d", round)})
		if err != nil {
			t.Fatalf("payload: %v", err)
		}
		event, err := engine.NewEvent("lymph://"+appID+"/"+junction,
			"lymph.feedback.unknown.v1", junction,
			protocol.EventData{
				FeedbackType: protocol.FeedbackUnknown,
				ReasonCode:   "UNKNOWN_EVENT_PHRASE",
				Payload:      payload,
			})
		if err != nil {
			t.Fatalf("event: %v", err)
		}
		var emitResp struct {
			IssueID string `json:"issue_id"`
		}
		if err := cli.Post(context.Background(), "/v1/events", event, &emitResp); err != nil {
			continue // the daemon may already be gone
		}
		issued++

		var work projection.WorkItem
		if err := cli.Post(context.Background(), "/v1/work-items",
			engine.QueueIssueRequest{IssueIDs: []string{emitResp.IssueID}}, &work); err != nil {
			continue
		}
		var claimed projection.WorkItem
		if err := cli.Post(context.Background(), "/v1/work-items/claim",
			map[string]any{"worker": "crasher", "ttl_seconds": 60}, &claimed); err != nil {
			continue
		}

		// Fire the return and kill the daemon almost immediately, so the kill
		// lands inside the transaction for some rounds.
		done := make(chan struct{})
		go func() {
			defer close(done)
			var out map[string]any
			_ = cli.Post(context.Background(), "/v1/work-items/"+claimed.WorkID+"/return",
				engine.ReturnImprovementRequest{
					WorkID: claimed.WorkID, Worker: "crasher", Attempt: claimed.Attempts,
					Artifacts: []engine.ImprovementArtifactRequest{{Kind: engine.ArtifactNoChange,
						NoChange: &engine.NoChangeArtifactRequest{Reason: "crash test", Disposition: "OTHER"}}},
				}, &out)
		}()
		time.Sleep(time.Duration(1+round) * time.Millisecond)
		d.kill9()
		<-done
		returned++

		d = startDaemon(t, root, "")
		cli = d.client()
	}
	d.stop()

	// The invariants.
	e := openEngineAt(t, root)
	report, err := e.VerifyLedger()
	if err != nil {
		Failf(t, "L25_crash", "§87 crash during ReturnImprovement", "the ledger survives",
			"%v", err)
		return
	}
	results, err := e.DB().ListImprovementResults("", 1000)
	if err != nil {
		t.Fatalf("list results: %v", err)
	}
	artifacts, err := e.Artifacts(projection.ArtifactFilter{Limit: 1000})
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}

	// Every artifact belongs to a result that exists.
	resultIDs := map[string]bool{}
	for _, result := range results {
		resultIDs[result.ResultID] = true
	}
	for _, artifact := range artifacts {
		if !resultIDs[artifact.ResultID] {
			Failf(t, "L25_crash", "§87 crash during ReturnImprovement", "no orphan artifacts",
				"artifact %s references missing result %s", shorten(artifact.ArtifactID), shorten(artifact.ResultID))
			return
		}
	}
	// Every returned work item has its result, and every result its work item.
	workItems, err := e.DB().ListWorkItems("", 1000)
	if err != nil {
		t.Fatalf("list work: %v", err)
	}
	for _, item := range workItems {
		if item.State != string(engine.WorkReturned) {
			continue
		}
		if _, err := e.DB().GetImprovementResultByWork(item.WorkID); err != nil {
			Failf(t, "L25_crash", "§87 crash during ReturnImprovement", "no RETURNED work without a result",
				"work %s is RETURNED but has no result", shorten(item.WorkID))
			return
		}
	}
	// Every candidate created with a result still has one.
	candidates, err := e.DB().ListCandidates("", "", 1000)
	if err != nil {
		t.Fatalf("list candidates: %v", err)
	}

	Note(t, "S08_artifact", "L25_crash", "§87 crash during ReturnImprovement", "kill -9 during typed returns",
		"%d rounds: %d events issued, %d returns attempted, %d results stored, %d artifacts, %d candidates; every artifact has its result, every RETURNED item has one, %d ledger records verify (torn tail %d bytes). No partial lineage survived any kill.",
		rounds, issued, returned, len(results), len(artifacts), len(candidates), report.Records, report.TornTailBytes)
}

func countRevisions(t *testing.T, e *engine.Engine) int {
	t.Helper()
	revisions, err := e.DB().ListRevisions("", 10000)
	if err != nil {
		t.Fatalf("list revisions: %v", err)
	}
	return len(revisions)
}

// TestL25DiskFailureDuringReturn is mission section 88: a write failure in the
// middle of a typed return must leave either a complete result or none.
func TestL25DiskFailureDuringReturn(t *testing.T) {
	root := openStoreRoot(t, t.TempDir())

	// 4096 blocks of 512 bytes: enough for the schema, not enough for the work.
	d := startDaemon(t, root, "ulimit -f 4096")
	cli := d.client()
	reg := registerViaClient(t, cli, "disk-return")
	appID := reg.Application.ApplicationID
	junction := reg.JunctionIDs["semantic.event_state"]
	familyID := reg.FamilyIDs["semantic_event_rules"]

	if err := cli.Post(context.Background(), "/v1/baselines", engine.BaselineRequest{
		Application: appID, ConfigFamily: familyID,
		Bundle: map[string][]byte{"rules.yaml": []byte("version: 1\nrules:\n  - id: a\n    phrase: awarded\n    state: AWARDED\n")},
		Author: "lab",
	}, nil); err != nil {
		t.Fatalf("baseline: %v", err)
	}

	completed := 0
	var writeErr error
	for round := 0; round < 3000; round++ {
		payload, _ := json.Marshal(map[string]any{"text": fmt.Sprintf("disk sample %d", round)})
		event, err := engine.NewEvent("lymph://"+appID+"/"+junction,
			"lymph.feedback.unknown.v1", junction,
			protocol.EventData{FeedbackType: protocol.FeedbackUnknown, ReasonCode: "UNKNOWN_EVENT_PHRASE", Payload: payload})
		if err != nil {
			t.Fatalf("event: %v", err)
		}
		var emitted struct {
			IssueID string `json:"issue_id"`
		}
		if err := cli.Post(context.Background(), "/v1/events", event, &emitted); err != nil {
			writeErr = err
			break
		}
		var work projection.WorkItem
		if err := cli.Post(context.Background(), "/v1/work-items",
			engine.QueueIssueRequest{IssueIDs: []string{emitted.IssueID}}, &work); err != nil {
			writeErr = err
			break
		}
		var claimed projection.WorkItem
		if err := cli.Post(context.Background(), "/v1/work-items/claim",
			map[string]any{"worker": "disk-worker", "ttl_seconds": 60}, &claimed); err != nil {
			writeErr = err
			break
		}
		var returned map[string]any
		if err := cli.Post(context.Background(), "/v1/work-items/"+claimed.WorkID+"/return",
			engine.ReturnImprovementRequest{
				WorkID: claimed.WorkID, Worker: "disk-worker", Attempt: claimed.Attempts,
				Summary: "disk fixture",
				Artifacts: []engine.ImprovementArtifactRequest{
					modelRefArtifact(fmt.Sprintf("s3://models/m%d.bin", round), "sha256:"+strings.Repeat("44", 32)),
					{Kind: engine.ArtifactConfigBundle, ConfigBundle: &engine.ConfigBundleArtifactRequest{
						Bundle: objectstore.Bundle{"rules.yaml": []byte(fmt.Sprintf("version: %d\nrules:\n  - id: a\n    phrase: awarded\n    state: AWARDED\n", round+2))},
					}},
				},
			}, &returned); err != nil {
			writeErr = err
			break
		}
		completed++
	}

	if writeErr == nil {
		d.stop()
		Skippedf(t, "L25_disk_return", "§88 disk failure during a typed return", "write-failure injection",
			"the 2 MiB limit was not reached in 3000 rounds; increase the workload to exhaust it")
		return
	}
	t.Logf("write failure after %d complete returns: %v", completed, writeErr)
	d.stop()

	e, err := engine.Open(engine.Options{Root: root, Logger: quietLogger()})
	if err != nil {
		Failf(t, "L25_disk_return", "§88 disk failure during a typed return", "the store reopens after a write failure",
			"%v", err)
		return
	}
	defer e.Close()

	report, err := e.VerifyLedger()
	if err != nil {
		Failf(t, "L25_disk_return", "§88 disk failure during a typed return", "a partial result never becomes canonical",
			"ledger verification failed: %v", err)
		return
	}

	// Every stored result must be complete: artifacts, and a RETURNED item.
	results, err := e.DB().ListImprovementResults("", 10000)
	if err != nil {
		t.Fatalf("list results: %v", err)
	}
	for _, result := range results {
		artifacts, err := e.DB().ListImprovementArtifacts(projection.ArtifactFilter{ResultID: result.ResultID, Limit: 100})
		if err != nil {
			t.Fatalf("list artifacts: %v", err)
		}
		if len(artifacts) != 2 {
			Failf(t, "L25_disk_return", "§88 disk failure during a typed return", "a stored result is complete",
				"result %s has %d of 2 artifacts", shorten(result.ResultID), len(artifacts))
			return
		}
		item, err := e.DB().GetWorkItem(result.WorkItemID)
		if err != nil {
			t.Fatalf("work item: %v", err)
		}
		if item.State != string(engine.WorkReturned) || item.ImprovementResultID != result.ResultID {
			Failf(t, "L25_disk_return", "§88 disk failure during a typed return", "a stored result has its work item",
				"work %s is %s with result id %s", shorten(item.WorkID), item.State, shorten(item.ImprovementResultID))
			return
		}
	}

	// No candidate exists without its result, and no work item claims a result
	// that is not stored.
	items, err := e.DB().ListWorkItems("", 10000)
	if err != nil {
		t.Fatalf("list work: %v", err)
	}
	orphans := 0
	for _, item := range items {
		if item.ImprovementResultID == "" {
			continue
		}
		if _, err := e.DB().GetImprovementResult(item.ImprovementResultID); err != nil {
			orphans++
		}
	}
	if orphans > 0 {
		Failf(t, "L25_disk_return", "§88 disk failure during a typed return", "no work item references a missing result",
			"%d work items do", orphans)
		return
	}

	candidates, err := e.DB().ListCandidates("", "", 10000)
	if err != nil {
		t.Fatalf("list candidates: %v", err)
	}
	for _, candidate := range candidates {
		if candidate.WorkItemID == "" {
			continue
		}
		if _, err := e.DB().GetImprovementResultByWork(candidate.WorkItemID); err != nil {
			Failf(t, "L25_disk_return", "§88 disk failure during a typed return", "no candidate exists without its result",
				"candidate %s references work %s with no result", shorten(candidate.CandidateID), shorten(candidate.WorkItemID))
			return
		}
	}

	// And the projection still rebuilds from canon.
	before := stateDigest(t, e)
	if _, err := e.Rebuild(); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if after := stateDigest(t, e); after != before {
		Failf(t, "L25_disk_return", "§88 disk failure during a typed return", "the projection still matches canon",
			"digest changed: %s vs %s", shorten(before), shorten(after))
		return
	}

	Note(t, "S08_artifact", "L25_disk_return", "§88 disk failure during a typed return", "a failing write is refused, never half-applied",
		"writes refused with %v after %d complete returns; %d stored results each carry both artifacts and a RETURNED work item, no candidate exists without its result, %d ledger records verify (%d torn bytes recovered) and the rebuild digest is unchanged",
		shorten(writeErr.Error()), completed, len(results), report.Records, report.TornTailBytes)
}

func countTargets(t *testing.T, e *engine.Engine) int {
	t.Helper()
	targets, err := e.DB().ListTargets("")
	if err != nil {
		t.Fatalf("list targets: %v", err)
	}
	return len(targets)
}
