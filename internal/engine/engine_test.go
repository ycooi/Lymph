package engine_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func newEngine(t *testing.T) *engine.Engine {
	t.Helper()
	e, err := engine.Open(engine.Options{Root: t.TempDir(), Logger: quietLogger()})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func registerSemanticService(t *testing.T, e *engine.Engine) engine.Registration {
	t.Helper()
	reg, err := e.RegisterApplication(context.Background(), engine.Manifest{
		Name:  "semantic-service",
		Type:  "mcp-server",
		Owner: "operator",
		Junctions: []engine.JunctionSpec{
			{
				Name:          "semantic.event_state",
				Workflow:      "semantic-rule-repair",
				ConfigFamily:  "semantic_event_rules",
				FeedbackTypes: []string{"UNKNOWN", "AMBIGUOUS", "CONFLICT"},
			},
			{
				Name:         "retrieval.candidate_generation",
				Workflow:     "retrieval-investigation",
				ConfigFamily: "semantic_event_rules",
			},
		},
		ConfigFamilies: []engine.ConfigFamilySpec{
			{
				Name:           "semantic_event_rules",
				SchemaRevision: "semantic-rules-v3",
				Workflow:       "semantic-rule-repair",
				Targets: []engine.TargetSpec{
					{
						Type:         "SINGLE_FILE",
						Path:         "/etc/semantic-service/semantic/events.yaml",
						ReloadPolicy: "SIGHUP",
						Atomicity:    "FULL",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	return reg
}

func emit(t *testing.T, e *engine.Engine, appRef, junction string, feedback protocol.FeedbackType, reason, payload, producer string) engine.EmitResponse {
	t.Helper()
	raw := []byte("null")
	if payload != "" {
		raw = []byte(payload)
	}
	event, err := engine.NewEvent(
		protocol.EventSource(appRef, junction),
		protocol.FeedbackEventType(feedback),
		junction,
		protocol.EventData{FeedbackType: feedback, ReasonCode: reason, Payload: raw},
	)
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	resp, err := e.Emit(context.Background(), engine.EmitRequest{Event: event, ProducerInstance: producer})
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	return resp
}

func TestRegistrationIsImmutableAndIdempotent(t *testing.T) {
	e := newEngine(t)
	reg := registerSemanticService(t, e)
	if !reg.Changed {
		t.Fatal("first registration should be a new revision")
	}
	if reg.JunctionIDs["semantic.event_state"] == "" {
		t.Fatal("junction identity missing from registration result")
	}

	// The same manifest must not create another revision (section 7).
	again, err := e.RegisterApplication(context.Background(), engine.Manifest{
		ApplicationID: reg.Application.ApplicationID,
		Name:          "semantic-service",
		Type:          "mcp-server",
		Owner:         "operator",
		Junctions: []engine.JunctionSpec{
			{Name: "semantic.event_state", Workflow: "semantic-rule-repair", ConfigFamily: "semantic_event_rules", FeedbackTypes: []string{"UNKNOWN", "AMBIGUOUS", "CONFLICT"}},
			{Name: "retrieval.candidate_generation", Workflow: "retrieval-investigation", ConfigFamily: "semantic_event_rules"},
		},
		ConfigFamilies: []engine.ConfigFamilySpec{
			{Name: "semantic_event_rules", SchemaRevision: "semantic-rules-v3", Workflow: "semantic-rule-repair",
				Targets: []engine.TargetSpec{{Type: "SINGLE_FILE", Path: "/etc/semantic-service/semantic/events.yaml", ReloadPolicy: "SIGHUP", Atomicity: "FULL"}}},
		},
	})
	if err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if again.Changed {
		t.Fatal("an identical manifest must not create a new revision")
	}
	regs, err := e.DB().ListRegistrations(reg.Application.ApplicationID)
	if err != nil {
		t.Fatalf("list registrations: %v", err)
	}
	if len(regs) != 1 {
		t.Fatalf("expected exactly one registration revision, got %d", len(regs))
	}
}

func TestEventsGroupIntoIssuesAndAreIdempotent(t *testing.T) {
	e := newEngine(t)
	reg := registerSemanticService(t, e)
	app := reg.Application.ApplicationID

	first := emit(t, e, app, "semantic.event_state", protocol.FeedbackUnknown, "UNKNOWN_EVENT_PHRASE",
		`{"text":"Acme business was booked at 512.00"}`, "producer-1")
	if !first.Accepted || first.IssueID == "" || !first.IssueCreated {
		t.Fatalf("unexpected first acknowledgement: %+v", first)
	}

	second := emit(t, e, app, "semantic.event_state", protocol.FeedbackUnknown, "UNKNOWN_EVENT_PHRASE",
		`{"text":"Acme business was booked at 918.45"}`, "producer-2")
	if second.IssueID != first.IssueID {
		t.Fatalf("structurally identical payloads must group: %s vs %s", second.IssueID, first.IssueID)
	}
	if second.IssueCreated {
		t.Fatal("second occurrence must not create another issue")
	}
	if second.OccurrenceNo != 2 {
		t.Fatalf("expected occurrence 2, got %d", second.OccurrenceNo)
	}

	// A different reason code is a different problem and must not merge.
	third := emit(t, e, app, "semantic.event_state", protocol.FeedbackConflict, "CONFLICTING_PRICE",
		`{"text":"Acme business was booked at 512.00"}`, "producer-1")
	if third.IssueID == first.IssueID {
		t.Fatal("different reason codes must not merge into one issue")
	}

	issue, err := e.DB().GetIssue(first.IssueID)
	if err != nil {
		t.Fatalf("get issue: %v", err)
	}
	if issue.OccurrenceCount != 2 {
		t.Fatalf("expected 2 occurrences, got %d", issue.OccurrenceCount)
	}
	if issue.UniqueSources != 2 {
		t.Fatalf("expected 2 unique producers, got %d", issue.UniqueSources)
	}
	if issue.Status != string(engine.IssueOpen) {
		t.Fatalf("expected OPEN, got %s", issue.Status)
	}

	// Re-sending the same event id must be a no-op (section 68).
	event, err := engine.NewEvent(protocol.EventSource(app, "semantic.event_state"),
		protocol.FeedbackEventType(protocol.FeedbackUnknown), "semantic.event_state",
		protocol.EventData{FeedbackType: protocol.FeedbackUnknown, ReasonCode: "UNKNOWN_EVENT_PHRASE"})
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	event.ID = first.EventID
	dup, err := e.Emit(context.Background(), engine.EmitRequest{Event: event, ProducerInstance: "producer-1"})
	if err != nil {
		t.Fatalf("duplicate emit: %v", err)
	}
	if !dup.Duplicate {
		t.Fatal("re-sending the same event id must be reported as a duplicate")
	}
	reloaded, err := e.DB().GetIssue(first.IssueID)
	if err != nil {
		t.Fatalf("get issue: %v", err)
	}
	if reloaded.OccurrenceCount != 2 {
		t.Fatalf("duplicate must not change counters, got %d", reloaded.OccurrenceCount)
	}
}

func TestUnknownJunctionIsRefused(t *testing.T) {
	e := newEngine(t)
	reg := registerSemanticService(t, e)

	event, err := engine.NewEvent(protocol.EventSource(reg.Application.ApplicationID, "not.declared"),
		protocol.FeedbackEventType(protocol.FeedbackUnknown), "not.declared",
		protocol.EventData{FeedbackType: protocol.FeedbackUnknown})
	if err != nil {
		t.Fatalf("build event: %v", err)
	}
	if _, err := e.Emit(context.Background(), engine.EmitRequest{Event: event}); !errors.Is(err, engine.ErrUnknownJunction) {
		t.Fatalf("expected ErrUnknownJunction, got %v", err)
	}
}

func TestPathOwnershipPreventsTrampling(t *testing.T) {
	e := newEngine(t)
	registerSemanticService(t, e)

	_, err := e.RegisterApplication(context.Background(), engine.Manifest{
		Name: "dataingest",
		ConfigFamilies: []engine.ConfigFamilySpec{
			{Name: "cru_schema", Targets: []engine.TargetSpec{
				{Type: "SINGLE_FILE", Path: "/etc/semantic-service/semantic/events.yaml"},
			}},
		},
	})
	if !errors.Is(err, projection.ErrPathConflict) {
		t.Fatalf("expected a path ownership conflict, got %v", err)
	}
}

func TestConfigPromotionStaleCandidateAndRebuild(t *testing.T) {
	e := newEngine(t)
	reg := registerSemanticService(t, e)
	app := reg.Application.ApplicationID
	ctx := context.Background()

	// Bootstrap from reality: r1 becomes active, approved, desired, deployed
	// and last_good (section 27).
	baseline := objectstore.Bundle{"events.yaml": []byte("rules:\n  - phrase: unknown\n")}
	r1, err := e.ImportBaseline(ctx, engine.BaselineRequest{
		Application:  app,
		ConfigFamily: "semantic_event_rules",
		Bundle:       baseline,
		Author:       "operator",
		Label:        "2.3.1",
	})
	if err != nil {
		t.Fatalf("baseline: %v", err)
	}
	if r1.Sequence != 1 {
		t.Fatalf("baseline should be r1, got r%d", r1.Sequence)
	}
	for _, name := range []string{engine.RefActive, engine.RefApproved, engine.RefDesired, engine.RefDeployed, engine.RefLastGood} {
		ref, err := e.DB().GetRef(app, r1.ConfigFamilyID, app, name)
		if err != nil {
			t.Fatalf("ref %s: %v", name, err)
		}
		if ref.RevisionID != r1.RevisionID {
			t.Fatalf("ref %s points at %s, expected %s", name, ref.RevisionID, r1.RevisionID)
		}
	}

	// Candidate A: fix the unknown phrase.
	ca, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application:  app,
		ConfigFamily: "semantic_event_rules",
		Bundle:       objectstore.Bundle{"events.yaml": []byte("rules:\n  - phrase: booked\n")},
		Explanation:  "add the newly observed booked-phrase rule",
	})
	if err != nil {
		t.Fatalf("candidate A: %v", err)
	}
	if ca.State != string(engine.CandidateDraft) {
		t.Fatalf("candidate should start DRAFT, got %s", ca.State)
	}

	// A candidate cannot be approved before it passed its tests.
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{CandidateID: ca.CandidateID, Approver: "operator"}); err == nil {
		t.Fatal("approving a DRAFT candidate must fail")
	}

	ca, err = e.RecordValidation(ctx, engine.ValidationRequest{CandidateID: ca.CandidateID, Kind: "structural", Result: "PASS", SuiteID: "semantic-service.semantic-rules.validate.v2"})
	if err != nil {
		t.Fatalf("structural validation: %v", err)
	}
	if ca.State != string(engine.CandidateStructurallyOK) {
		t.Fatalf("expected STRUCTURALLY_VALID, got %s", ca.State)
	}
	ca, err = e.RecordValidation(ctx, engine.ValidationRequest{CandidateID: ca.CandidateID, Kind: "replay", Result: "PASS", SuiteID: "semantic-service.semantic-rules.replay"})
	if err != nil {
		t.Fatalf("replay validation: %v", err)
	}
	if ca.State != string(engine.CandidateTesting) {
		t.Fatalf("expected TESTING after the first test run, got %s", ca.State)
	}
	ca, err = e.RecordValidation(ctx, engine.ValidationRequest{CandidateID: ca.CandidateID, Kind: "regression", Result: "PASS", SuiteID: "semantic-service.semantic-rules.regression"})
	if err != nil {
		t.Fatalf("regression validation: %v", err)
	}
	if ca.State != string(engine.CandidatePassed) {
		t.Fatalf("expected PASSED, got %s", ca.State)
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{CandidateID: ca.CandidateID, Approver: "operator", Comment: "looks right"}); err != nil {
		t.Fatalf("approve: %v", err)
	}

	// Candidate B is built against the same active revision r1.
	cb, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application:  app,
		ConfigFamily: "semantic_event_rules",
		Bundle:       objectstore.Bundle{"events.yaml": []byte("rules:\n  - phrase: other\n")},
		Explanation:  "competing fix",
	})
	if err != nil {
		t.Fatalf("candidate B: %v", err)
	}

	promotion, err := e.PromoteCandidate(ctx, ca.CandidateID, "operator")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if promotion.Revision.Sequence != 2 {
		t.Fatalf("expected r2 after promotion, got r%d", promotion.Revision.Sequence)
	}
	if promotion.Candidate.State != string(engine.CandidateDeployable) {
		t.Fatalf("expected DEPLOYABLE, got %s", promotion.Candidate.State)
	}
	active, err := e.DB().GetRef(app, r1.ConfigFamilyID, app, engine.RefActive)
	if err != nil {
		t.Fatalf("active ref: %v", err)
	}
	if active.RevisionID != promotion.Revision.RevisionID {
		t.Fatalf("active should now be r2, got %s", active.RevisionID)
	}
	// r43 is approved and desired, but deployed has not moved: no deployment
	// has run (section 52).
	deployed, err := e.DB().GetRef(app, r1.ConfigFamilyID, app, engine.RefDeployed)
	if err != nil {
		t.Fatalf("deployed ref: %v", err)
	}
	if deployed.RevisionID != r1.RevisionID {
		t.Fatalf("deployed must not move without a deployment, got %s", deployed.RevisionID)
	}

	// Revisions are immutable: r1 is still there, still pointing at its bytes.
	r1again, err := e.DB().GetRevision(r1.RevisionID)
	if err != nil {
		t.Fatalf("reload r1: %v", err)
	}
	if r1again.ContentHash != r1.ContentHash {
		t.Fatal("revision content changed; revisions must be immutable")
	}

	// The stale case: candidate B was built on r1, but active is now r2.
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{CandidateID: cb.CandidateID, Approver: "operator"}); err == nil {
		// B is still DRAFT, so approval fails for a different reason; drive it
		// through the pipeline so the stale check is what stops it.
	}
	for _, step := range []engine.ValidationRequest{
		{CandidateID: cb.CandidateID, Kind: "structural", Result: "PASS"},
		{CandidateID: cb.CandidateID, Kind: "replay", Result: "PASS"},
		{CandidateID: cb.CandidateID, Kind: "regression", Result: "PASS"},
	} {
		if _, err := e.RecordValidation(ctx, step); err != nil {
			t.Fatalf("validation %s: %v", step.Kind, err)
		}
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{CandidateID: cb.CandidateID, Approver: "operator"}); err != nil {
		t.Fatalf("approve B: %v", err)
	}
	if _, err := e.PromoteCandidate(ctx, cb.CandidateID, "operator"); !errors.Is(err, projection.ErrStaleCandidate) {
		t.Fatalf("expected STALE_CANDIDATE, got %v", err)
	}
	stale, err := e.DB().GetCandidate(cb.CandidateID)
	if err != nil {
		t.Fatalf("reload B: %v", err)
	}
	if stale.State != string(engine.CandidateStale) {
		t.Fatalf("candidate B should be preserved as STALE, got %s", stale.State)
	}
	stillActive, err := e.DB().GetRef(app, r1.ConfigFamilyID, app, engine.RefActive)
	if err != nil {
		t.Fatalf("active ref: %v", err)
	}
	if stillActive.RevisionID != promotion.Revision.RevisionID {
		t.Fatal("a stale candidate must never overwrite the newer revision")
	}

	// The reflog must explain the move, years later (section 21).
	entries, err := e.DB().ListReflog(app, r1.ConfigFamilyID, app, 20)
	if err != nil {
		t.Fatalf("reflog: %v", err)
	}
	found := false
	for _, entry := range entries {
		if entry.RefName == engine.RefActive && entry.NewRevisionID == promotion.Revision.RevisionID {
			found = true
			if entry.OldRevisionID != r1.RevisionID {
				t.Fatalf("reflog old revision should be r1, got %s", entry.OldRevisionID)
			}
			if entry.CandidateID != ca.CandidateID {
				t.Fatalf("reflog should name the candidate, got %s", entry.CandidateID)
			}
		}
	}
	if !found {
		t.Fatal("no reflog entry recorded the promotion")
	}

	// Now the important part: SQLite is only a projection. Rebuild it from the
	// ledger and object store and check that nothing changes (section 88).
	beforeRefs, err := e.DB().ListRefs(app, r1.ConfigFamilyID, app)
	if err != nil {
		t.Fatalf("refs before rebuild: %v", err)
	}
	beforeRevisions, err := e.DB().ListRevisions(r1.ConfigFamilyID, 50)
	if err != nil {
		t.Fatalf("revisions before rebuild: %v", err)
	}
	beforeIssues, err := e.DB().ListIssues("", 50, "count")
	if err != nil {
		t.Fatalf("issues before rebuild: %v", err)
	}

	applied, err := e.Rebuild()
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if applied == 0 {
		t.Fatal("rebuild replayed nothing; the ledger should not be empty")
	}

	afterRefs, err := e.DB().ListRefs(app, r1.ConfigFamilyID, app)
	if err != nil {
		t.Fatalf("refs after rebuild: %v", err)
	}
	if len(afterRefs) != len(beforeRefs) {
		t.Fatalf("refs changed across rebuild: %d before, %d after", len(beforeRefs), len(afterRefs))
	}
	for i := range beforeRefs {
		if beforeRefs[i] != afterRefs[i] {
			t.Fatalf("ref %s changed across rebuild:\n  %+v\n  %+v", beforeRefs[i].Name, beforeRefs[i], afterRefs[i])
		}
	}
	afterRevisions, err := e.DB().ListRevisions(r1.ConfigFamilyID, 50)
	if err != nil {
		t.Fatalf("revisions after rebuild: %v", err)
	}
	if len(afterRevisions) != len(beforeRevisions) {
		t.Fatalf("revisions changed across rebuild: %d before, %d after", len(beforeRevisions), len(afterRevisions))
	}
	// The stale candidate survived the rebuild, and the ledger is still intact.
	reloadedB, err := e.DB().GetCandidate(cb.CandidateID)
	if err != nil {
		t.Fatalf("candidate B after rebuild: %v", err)
	}
	if reloadedB.State != string(engine.CandidateStale) {
		t.Fatalf("candidate state did not survive the rebuild: %s", reloadedB.State)
	}
	report, err := e.VerifyLedger()
	if err != nil {
		t.Fatalf("ledger verify after rebuild: %v", err)
	}
	if report.Records == 0 {
		t.Fatal("ledger is empty after rebuild")
	}

	// Objects referenced by r2 must still be present and intact (section 89).
	if err := e.Objects().VerifyRevision(promotion.Revision.ContentHash); err != nil {
		t.Fatalf("object integrity: %v", err)
	}
	_ = beforeIssues
}

func TestWorkLeaseExpiryReturnsToQueue(t *testing.T) {
	e := newEngine(t)
	reg := registerSemanticService(t, e)
	app := reg.Application.ApplicationID
	ctx := context.Background()

	resp := emit(t, e, app, "semantic.event_state", protocol.FeedbackUnknown, "UNKNOWN_EVENT_PHRASE",
		`{"text":"Acme business was booked at 512.00"}`, "producer-1")

	work, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{resp.IssueID}, Actor: "operator"})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	if work.WorkflowType != "semantic-rule-repair" {
		t.Fatalf("work should inherit the junction's workflow, got %q", work.WorkflowType)
	}
	issue, err := e.DB().GetIssue(resp.IssueID)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if issue.Status != string(engine.IssueQueuedForImprovement) {
		t.Fatalf("issue should be QUEUED_FOR_IMPROVEMENT, got %s", issue.Status)
	}

	claimed, err := e.ClaimWork(ctx, engine.ClaimRequest{Worker: "codex-maintenance", TTL: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.State != string(engine.WorkLeased) || claimed.LeaseOwner != "codex-maintenance" {
		t.Fatalf("unexpected lease: %+v", claimed)
	}
	if claimed.Attempts != 1 {
		t.Fatalf("expected attempt 1, got %d", claimed.Attempts)
	}

	// The worker disappears. Nobody renews the lease, so the item returns to
	// the queue (section 42, section 87).
	time.Sleep(20 * time.Millisecond)
	released, err := e.SweepLeases(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if released != 1 {
		t.Fatalf("expected 1 released lease, got %d", released)
	}
	requeued, err := e.DB().GetWorkItem(work.WorkID)
	if err != nil {
		t.Fatalf("work item: %v", err)
	}
	if requeued.State != string(engine.WorkQueued) {
		t.Fatalf("expected QUEUED after expiry, got %s", requeued.State)
	}

	// A second worker can now claim the same work: at-least-once delivery.
	claimedAgain, err := e.ClaimWork(ctx, engine.ClaimRequest{Worker: "second-worker", TTL: time.Minute})
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if claimedAgain.WorkID != work.WorkID || claimedAgain.Attempts != 2 {
		t.Fatalf("unexpected second lease: %+v", claimedAgain)
	}
}

func TestLedgerIsCanonicalAcrossRestart(t *testing.T) {
	root := t.TempDir()
	e, err := engine.Open(engine.Options{Root: root, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	instance := e.Identity().InstanceUUID
	reg := registerSemanticService(t, e)
	emit(t, e, reg.Application.ApplicationID, "semantic.event_state", protocol.FeedbackUnknown,
		"UNKNOWN_EVENT_PHRASE", `{"text":"Acme business was booked at 512.00"}`, "producer-1")
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A restart must reuse the identity, resume the ledger and keep the data.
	reopened, err := engine.Open(engine.Options{Root: root, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if reopened.Identity().InstanceUUID != instance {
		t.Fatal("instance identity changed across restart")
	}
	status, err := reopened.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Applications != 1 || status.Events != 1 {
		t.Fatalf("state did not survive restart: %+v", status)
	}
	if status.LedgerHead == "" || status.LedgerRecords == 0 {
		t.Fatalf("ledger head missing after restart: %+v", status)
	}
	if status.DriftRecords != 0 {
		t.Fatalf("projection should be caught up, %d records pending", status.DriftRecords)
	}
}
