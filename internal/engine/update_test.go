package engine_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
)

// The human-gated approved-update channel (mission sections 19 to 42, 61).
//
// These tests are the hard negatives. Each asks "can this happen?" and expects
// the answer to be no, because the invariant is:
//
//	no new improvement may enter the delivery channel without an explicit human
//	approval for that exact immutable content.

// updateFixture is one application with a managed family, an EXTERNAL family,
// a baseline, and a promoted revision a human approved for one installation.
type updateFixture struct {
	engine         *engine.Engine
	app            string
	installation   string
	otherInstall   string
	family         string
	base           projection.ConfigRevision
	revision       projection.ConfigRevision
	candidate      projection.Candidate
	approval       projection.Approval
	externalFamily projection.ConfigRevision
}

// newUpdateFixture registers a two-family application: one family Lymph
// manages, and one it must never touch. humanActor decides whether the approval
// in the fixture is a human one or an unlabelled (machine) one.
func newUpdateFixture(t *testing.T, humanActor bool) updateFixture {
	t.Helper()
	ctx := context.Background()
	e := newEngine(t)

	defaultInstallation := identity.NewID()
	reg, err := e.RegisterApplication(ctx, engine.Manifest{
		Name:           "fixture",
		Type:           "service",
		Owner:          "operator",
		InstallationID: defaultInstallation,
		Installations: []engine.InstallationSpec{
			{InstallationID: defaultInstallation, Name: "laptop", Environment: "local"},
			{Name: "server", Environment: "production"},
		},
		Junctions: []engine.JunctionSpec{
			{Name: "rules.match", Workflow: "rule-repair", ConfigFamily: "managed_rules"},
		},
		ConfigFamilies: []engine.ConfigFamilySpec{
			{
				Name:           "managed_rules",
				SchemaRevision: "rules-v1",
				Workflow:       "rule-repair",
				ManagementMode: projection.ManagementLymphManaged,
				Targets: []engine.TargetSpec{{
					Type: "SINGLE_FILE", Path: "/etc/fixture/rules.yaml",
					ReloadPolicy: "SIGHUP", Atomicity: "FULL",
				}},
			},
			{
				// Deliberately left at the default: EXTERNAL.
				Name:           "credentials",
				SchemaRevision: "creds-v1",
				Targets: []engine.TargetSpec{{
					Type: "SINGLE_FILE", Path: "/etc/fixture/credentials.yaml",
					ReloadPolicy: "RESTART", Atomicity: "FULL",
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	app := reg.Application.ApplicationID
	installation := reg.DefaultInstallationID
	if installation == "" {
		t.Fatalf("fixture has no default installation: %+v", reg)
	}
	otherInstallation := ""
	for _, id := range reg.InstallationIDs {
		if id != installation {
			otherInstallation = id
		}
	}
	if otherInstallation == "" {
		t.Fatal("fixture problem: the second installation was not registered")
	}

	base, err := e.ImportBaseline(ctx, engine.BaselineRequest{
		Application:  app,
		ConfigFamily: "managed_rules",
		Bundle:       objectstore.Bundle{"rules.yaml": []byte("rules:\n  - phrase: baseline\n")},
		Author:       "operator",
		Label:        "v1",
	})
	if err != nil {
		t.Fatalf("baseline: %v", err)
	}

	// The candidate must be tied to the problem it claims to fix, otherwise a
	// later rejection has no junction to return to. This mirrors the real loop:
	// feedback creates an issue, the worker works the issue, the candidate cites
	// it.
	occurrence := emit(t, e, app, "rules.match", protocol.FeedbackUnknown,
		"UNKNOWN_EVENT_PHRASE", `{"text":"Acme booked at 512.00"}`, "fixture-process")
	if occurrence.IssueID == "" {
		t.Fatal("fixture problem: emitting feedback created no issue")
	}

	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application:  app,
		ConfigFamily: "managed_rules",
		Bundle: objectstore.Bundle{
			"rules.yaml": []byte("rules:\n  - phrase: baseline\n  - phrase: booked\n"),
		},
		Explanation: "add the booked-phrase rule",
		IssueIDs:    []string{occurrence.IssueID},
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		candidate, err = e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS",
			SuiteID: "fixture." + kind,
		})
		if err != nil {
			t.Fatalf("%s validation: %v", kind, err)
		}
	}
	if candidate.State != string(engine.CandidatePassed) {
		t.Fatalf("candidate should have passed: %s", candidate.State)
	}

	approvalRequest := engine.ApprovalRequest{
		CandidateID: candidate.CandidateID,
		Approver:    "operator",
		Comment:     "reviewed",
	}
	if humanActor {
		approvalRequest.ActorType = projection.ActorHuman
		approvalRequest.PeerUID = 1000
	}
	if _, err := e.ApproveCandidate(ctx, approvalRequest); err != nil {
		t.Fatalf("approve: %v", err)
	}
	approval, err := e.DB().LatestApproval(candidate.CandidateID)
	if err != nil {
		t.Fatalf("latest approval: %v", err)
	}

	promotion, err := e.PromoteCandidate(ctx, candidate.CandidateID, "operator")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}

	external, err := e.ImportBaseline(ctx, engine.BaselineRequest{
		Application:  app,
		ConfigFamily: "credentials",
		Bundle:       objectstore.Bundle{"credentials.yaml": []byte("token: not-a-real-secret\n")},
		Author:       "operator",
		Label:        "c1",
	})
	if err != nil {
		t.Fatalf("external baseline: %v", err)
	}

	return updateFixture{
		engine:         e,
		app:            app,
		installation:   installation,
		otherInstall:   otherInstallation,
		family:         promotion.Revision.ConfigFamilyID,
		base:           base,
		revision:       promotion.Revision,
		candidate:      candidate,
		approval:       approval,
		externalFamily: external,
	}
}

func refSnapshot(t *testing.T, f updateFixture, names ...string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, name := range names {
		ref, err := f.engine.DB().GetRef(f.app, f.family, f.installation, name)
		if err != nil {
			out[name] = ""
			continue
		}
		out[name] = ref.RevisionID
	}
	return out
}

// TestUnapprovedRevisionIsNotDeliverable is the core invariant: passing tests
// is not approval (mission sections 19, 42).
func TestUnapprovedRevisionIsNotDeliverable(t *testing.T) {
	ctx := context.Background()
	e := newEngine(t)
	reg, err := e.RegisterApplication(ctx, engine.Manifest{
		Name: "fixture",
		ConfigFamilies: []engine.ConfigFamilySpec{{
			Name: "managed_rules", ManagementMode: projection.ManagementLymphManaged,
			Targets: []engine.TargetSpec{{Type: "SINGLE_FILE", Path: "/etc/fixture/rules.yaml"}},
		}},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	app := reg.Application.ApplicationID
	installation := reg.DefaultInstallationID

	if _, err := e.ImportBaseline(ctx, engine.BaselineRequest{
		Application: app, ConfigFamily: "managed_rules",
		Bundle: objectstore.Bundle{"rules.yaml": []byte("rules: []\n")},
	}); err != nil {
		t.Fatalf("baseline: %v", err)
	}
	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app, ConfigFamily: "managed_rules",
		Bundle:      objectstore.Bundle{"rules.yaml": []byte("rules:\n  - phrase: booked\n")},
		Explanation: "proposal nobody has approved",
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS", SuiteID: "fixture." + kind,
		}); err != nil {
			t.Fatalf("validation: %v", err)
		}
	}
	// A revision that names a candidate but has no approval for it. Promotion
	// normally requires approval, so the revision is created directly: this is
	// the shape a bypass would take, and the gate must still refuse it.
	revision, err := e.CreateRevision(ctx, engine.RevisionRequest{
		Application:  app,
		ConfigFamily: "managed_rules",
		Bundle:       objectstore.Bundle{"rules.yaml": []byte("rules:\n  - phrase: booked\n")},
		Message:      "unapproved proposal",
		CandidateID:  candidate.CandidateID,
		Author:       "worker",
	})
	if err != nil {
		t.Fatalf("create revision: %v", err)
	}

	updates, err := e.Updates(ctx, app, installation, "")
	if err != nil {
		t.Fatalf("updates: %v", err)
	}
	for _, update := range updates {
		if update.RevisionID == revision.RevisionID {
			t.Fatalf("unapproved revision %s appeared in the delivery list", update.RevisionID)
		}
	}
	_, err = e.FetchUpdate(ctx, app, installation, revision.ConfigFamilyID, revision.RevisionID, "")
	var refused *engine.ErrNotDeliverable
	if !errors.As(err, &refused) {
		t.Fatalf("fetching an unapproved revision must fail, got %v", err)
	}
	if refused.ReasonCode != engine.NotNoApproval {
		t.Fatalf("reason code %s, expected %s", refused.ReasonCode, engine.NotNoApproval)
	}
}

// TestHumanApprovedRevisionIsDeliverable is the positive control, and it checks
// the part that is easy to get wrong: the approval binds to the tree the node
// fetches, not to the revision-object address.
func TestHumanApprovedRevisionIsDeliverable(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	updates, err := f.engine.Updates(ctx, f.app, f.installation, "")
	if err != nil {
		t.Fatalf("updates: %v", err)
	}
	if len(updates) != 1 {
		t.Fatalf("expected exactly one deliverable update, got %d: %+v", len(updates), updates)
	}
	update := updates[0]
	if update.RevisionID != f.revision.RevisionID {
		t.Fatalf("wrong revision offered: %s", update.RevisionID)
	}
	if update.ApprovalID != f.approval.ApprovalID {
		t.Fatalf("update cites approval %s, expected %s", update.ApprovalID, f.approval.ApprovalID)
	}
	if update.ContentHash != f.revision.RootTreeHash {
		t.Fatalf("update content hash %s, expected tree hash %s", update.ContentHash, f.revision.RootTreeHash)
	}
	if update.ContentHash == f.revision.ContentHash {
		t.Fatal("delivery bound to the revision-object address instead of the tree the node fetches")
	}

	fetched, err := f.engine.FetchUpdate(ctx, f.app, f.installation, f.family,
		f.revision.RevisionID, f.base.RevisionID)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(fetched.Bundle) == 0 {
		t.Fatal("fetched bundle is empty")
	}
	if fetched.TreeHash != f.revision.RootTreeHash {
		t.Fatalf("fetched tree hash %s, expected %s", fetched.TreeHash, f.revision.RootTreeHash)
	}
}

// TestMachineApprovalIsNotDeliverable: an unlabelled approval is SYSTEM, and a
// SYSTEM approval closes the channel rather than opening it.
func TestMachineApprovalIsNotDeliverable(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, false)

	if f.approval.ActorType != projection.ActorSystem {
		t.Fatalf("unlabelled approval recorded as %s, expected SYSTEM", f.approval.ActorType)
	}
	updates, err := f.engine.Updates(ctx, f.app, f.installation, "")
	if err != nil {
		t.Fatalf("updates: %v", err)
	}
	if len(updates) != 0 {
		t.Fatalf("a SYSTEM approval made %d updates deliverable", len(updates))
	}
	_, err = f.engine.FetchUpdate(ctx, f.app, f.installation, f.family, f.revision.RevisionID, "")
	var refused *engine.ErrNotDeliverable
	if !errors.As(err, &refused) {
		t.Fatalf("expected a delivery refusal, got %v", err)
	}
	if refused.ReasonCode != engine.NotApprovalNotHuman {
		t.Fatalf("reason code %s, expected %s", refused.ReasonCode, engine.NotApprovalNotHuman)
	}
}

// TestExternalFamilyIsNeverDelivered: the management boundary
// (mission sections 5, 6).
func TestExternalFamilyIsNeverDelivered(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	_, err := f.engine.FetchUpdate(ctx, f.app, f.installation,
		f.externalFamily.ConfigFamilyID, f.externalFamily.RevisionID, "")
	var refused *engine.ErrNotDeliverable
	if !errors.As(err, &refused) {
		t.Fatalf("expected a delivery refusal for an EXTERNAL family, got %v", err)
	}
	if refused.ReasonCode != engine.NotFamilyExternal {
		t.Fatalf("reason code %s, expected %s", refused.ReasonCode, engine.NotFamilyExternal)
	}

	updates, err := f.engine.Updates(ctx, f.app, f.installation, "")
	if err != nil {
		t.Fatalf("updates: %v", err)
	}
	for _, update := range updates {
		if update.ConfigFamilyID == f.externalFamily.ConfigFamilyID {
			t.Fatal("an EXTERNAL family appeared in the delivery list")
		}
	}
}

// TestApprovalIsBoundToInstallation (mission sections 3, 47, 48, 66).
func TestApprovalIsBoundToInstallation(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	// The approval names one installation, so the application's other
	// installation must not be able to consume it (mission sections 47, 48).
	_, err := f.engine.FetchUpdate(ctx, f.app, f.otherInstall, f.family, f.revision.RevisionID, "")
	var refused *engine.ErrNotDeliverable
	if !errors.As(err, &refused) {
		t.Fatalf("another installation fetched this installation's approval: %v", err)
	}
	if refused.ReasonCode != engine.NotApprovalTarget {
		t.Fatalf("reason code %s, expected %s", refused.ReasonCode, engine.NotApprovalTarget)
	}

	updates, err := f.engine.Updates(ctx, f.app, f.otherInstall, "")
	if err != nil {
		t.Fatalf("updates for the other installation: %v", err)
	}
	if len(updates) != 0 {
		t.Fatalf("the other installation was offered %d updates", len(updates))
	}
}

// TestApprovalIsBoundToExactContent: an approval for one hash cannot authorise
// different content, even if the projection is tampered with afterwards
// (mission sections 3, 4).
func TestApprovalIsBoundToExactContent(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	if _, err := f.engine.DB().SQL().Exec(
		`UPDATE approvals SET content_hash = ? WHERE approval_id = ?`,
		"sha256:0000000000000000000000000000000000000000000000000000000000000000",
		f.approval.ApprovalID); err != nil {
		t.Fatalf("tamper with the approval: %v", err)
	}
	_, err := f.engine.FetchUpdate(ctx, f.app, f.installation, f.family, f.revision.RevisionID, "")
	var refused *engine.ErrNotDeliverable
	if !errors.As(err, &refused) {
		t.Fatalf("an approval for a different hash still delivered content: %v", err)
	}
	if refused.ReasonCode != engine.NotApprovalContent {
		t.Fatalf("reason code %s, expected %s", refused.ReasonCode, engine.NotApprovalContent)
	}
}

// TestWrongApplicationCannotFetch: family/application mismatch is refused.
func TestWrongApplicationCannotFetch(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	other, err := f.engine.RegisterApplication(ctx, engine.Manifest{
		Name:           "other",
		ConfigFamilies: []engine.ConfigFamilySpec{{Name: "other_rules"}},
	})
	if err != nil {
		t.Fatalf("register other: %v", err)
	}
	if _, err := f.engine.FetchUpdate(ctx, other.Application.ApplicationID,
		other.DefaultInstallationID, f.family, f.revision.RevisionID, ""); err == nil {
		t.Fatal("a different application fetched another application's approved revision")
	}
}

// TestReportAppliedMovesObservedAndLastGoodOnly (mission sections 31, 36).
func TestReportAppliedMovesObservedAndLastGoodOnly(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	before := refSnapshot(t, f, projection.RefObserved, engine.RefLastGood,
		engine.RefDeployed, engine.RefDesired)
	// observed starts empty on purpose: it means "the revision this node has
	// confirmed it is running", and before any report there is no such
	// confirmation, whatever governance assumes (mission sections 34, 35).
	if before[projection.RefObserved] != "" {
		t.Fatalf("observed starts at %q; only a node report may set it",
			before[projection.RefObserved])
	}

	result, err := f.engine.ReportApplied(ctx, engine.ReportAppliedRequest{
		ApplicationID:      f.app,
		InstallationID:     f.installation,
		ConfigFamilyID:     f.family,
		RevisionID:         f.revision.RevisionID,
		ObservedHash:       f.revision.RootTreeHash,
		PreviousRevisionID: f.base.RevisionID,
		ProcessID:          "test-process",
	})
	if err != nil {
		t.Fatalf("report applied: %v", err)
	}
	if result.Result != projection.ResultApplied {
		t.Fatalf("result %s", result.Result)
	}

	after := refSnapshot(t, f, projection.RefObserved, engine.RefLastGood,
		engine.RefDeployed, engine.RefDesired)
	if after[projection.RefObserved] != f.revision.RevisionID {
		t.Fatalf("observed is %q, expected %s", after[projection.RefObserved], f.revision.RevisionID)
	}
	if after[engine.RefLastGood] != f.revision.RevisionID {
		t.Fatalf("last_good is %q, expected %s", after[engine.RefLastGood], f.revision.RevisionID)
	}
	if after[engine.RefDeployed] != before[engine.RefDeployed] {
		t.Fatalf("deployed moved to %q on a pull-mode apply; it stays reserved for a push adapter",
			after[engine.RefDeployed])
	}
	if after[engine.RefDesired] != f.revision.RevisionID {
		t.Fatalf("desired should remain the approved revision, got %q", after[engine.RefDesired])
	}

	// A hash that is not the revision's hash is corruption, not success.
	if _, err := f.engine.ReportApplied(ctx, engine.ReportAppliedRequest{
		ApplicationID: f.app, InstallationID: f.installation, ConfigFamilyID: f.family,
		RevisionID: f.revision.RevisionID, ObservedHash: "sha256:deadbeef",
	}); err == nil {
		t.Fatal("ReportApplied must refuse a hash that does not match the revision")
	}
}

// TestRejectionIsFeedbackAndDoesNotMoveRefs (mission sections 37, 38, 39).
func TestRejectionIsFeedbackAndDoesNotMoveRefs(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	before := refSnapshot(t, f, projection.RefObserved, engine.RefLastGood, engine.RefDesired)
	result, err := f.engine.ReportRejected(ctx, engine.ReportRejectedRequest{
		ApplicationID:  f.app,
		InstallationID: f.installation,
		ConfigFamilyID: f.family,
		RevisionID:     f.revision.RevisionID,
		ReasonCode:     "LOCAL_VALIDATION_FAILED",
		Message:        "the node's own schema check rejected the bundle",
	})
	if err != nil {
		t.Fatalf("report rejected: %v", err)
	}
	if result.Result != projection.ResultRejected {
		t.Fatalf("result %s", result.Result)
	}

	after := refSnapshot(t, f, projection.RefObserved, engine.RefLastGood, engine.RefDesired)
	for _, name := range []string{projection.RefObserved, engine.RefLastGood, engine.RefDesired} {
		if after[name] != before[name] {
			t.Fatalf("%s moved on a rejection: %q then %q", name, before[name], after[name])
		}
	}

	events, err := f.engine.Events(f.app, 50)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	found := false
	for _, event := range events {
		if event.ReasonCode == "APPROVED_UPDATE_REJECTED" {
			found = true
		}
	}
	if !found {
		t.Fatal("the rejection did not return to the learning loop as feedback")
	}

	updates, err := f.engine.Updates(ctx, f.app, f.installation, "")
	if err != nil {
		t.Fatalf("updates: %v", err)
	}
	for _, update := range updates {
		if update.RevisionID == f.revision.RevisionID {
			t.Fatal("a revision this installation rejected was offered again")
		}
	}
}

// TestWithdrawnUpdateIsNotDelivered (mission section 41).
func TestWithdrawnUpdateIsNotDelivered(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	if _, err := f.engine.WithdrawUpdate(ctx, f.app, f.installation, f.family,
		f.revision.RevisionID, "uid:1000", "operator changed their mind"); err != nil {
		t.Fatalf("withdraw: %v", err)
	}

	updates, err := f.engine.Updates(ctx, f.app, f.installation, "")
	if err != nil {
		t.Fatalf("updates: %v", err)
	}
	if len(updates) != 0 {
		t.Fatalf("a withdrawn update is still offered: %+v", updates)
	}
	_, err = f.engine.FetchUpdate(ctx, f.app, f.installation, f.family, f.revision.RevisionID, "")
	var refused *engine.ErrNotDeliverable
	if !errors.As(err, &refused) || refused.ReasonCode != engine.NotWithdrawn {
		t.Fatalf("expected a withdrawal refusal, got %v", err)
	}

	// The approval is not erased: the history is "approved, then withdrawn".
	approvals, err := f.engine.DB().ListApprovals(f.candidate.CandidateID)
	if err != nil {
		t.Fatalf("approvals: %v", err)
	}
	if len(approvals) != 1 || approvals[0].Decision != "APPROVED" {
		t.Fatalf("withdrawal erased the approval: %+v", approvals)
	}
}

// TestApplicationResultIsCanonicalAndIdempotent (mission sections 33, 54).
func TestApplicationResultIsCanonicalAndIdempotent(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	request := engine.ReportAppliedRequest{
		ApplicationID: f.app, InstallationID: f.installation, ConfigFamilyID: f.family,
		RevisionID: f.revision.RevisionID, ObservedHash: f.revision.RootTreeHash,
	}
	first, err := f.engine.ReportApplied(ctx, request)
	if err != nil {
		t.Fatalf("first report: %v", err)
	}
	second, err := f.engine.ReportApplied(ctx, request)
	if err != nil {
		t.Fatalf("duplicate report: %v", err)
	}
	if second.ResultID != first.ResultID {
		t.Fatalf("a duplicate report created a second logical result (%s then %s)",
			first.ResultID, second.ResultID)
	}
	results, err := f.engine.ApplicationResults(f.app, f.installation, 50)
	if err != nil {
		t.Fatalf("results: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one canonical result, got %d", len(results))
	}
}

// TestRepeatedRejectionIsIdempotent.
func TestRepeatedRejectionIsIdempotent(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)
	request := engine.ReportRejectedRequest{
		ApplicationID: f.app, InstallationID: f.installation, ConfigFamilyID: f.family,
		RevisionID: f.revision.RevisionID, ReasonCode: "SCHEMA_UNSUPPORTED", Message: "no",
	}
	first, err := f.engine.ReportRejected(ctx, request)
	if err != nil {
		t.Fatalf("first rejection: %v", err)
	}
	second, err := f.engine.ReportRejected(ctx, request)
	if err != nil {
		t.Fatalf("duplicate rejection: %v", err)
	}
	if second.ResultID != first.ResultID {
		t.Fatal("a duplicate rejection created a second canonical result")
	}
	results, err := f.engine.ApplicationResults(f.app, f.installation, 50)
	if err != nil {
		t.Fatalf("results: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one canonical result, got %d", len(results))
	}
}

// TestRejectionWithoutAValidReasonCodeIsRefused: the vocabulary is closed.
func TestRejectionWithoutAValidReasonCodeIsRefused(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)
	if _, err := f.engine.ReportRejected(ctx, engine.ReportRejectedRequest{
		ApplicationID: f.app, InstallationID: f.installation, ConfigFamilyID: f.family,
		RevisionID: f.revision.RevisionID, ReasonCode: "it did not work",
	}); err == nil {
		t.Fatal("a free-form rejection reason was accepted")
	}
}

// TestNewerApprovedRevisionIsOfferedFirst (mission sections 56, 57).
func TestNewerApprovedRevisionIsOfferedFirst(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	second, err := f.engine.CreateCandidate(ctx, engine.CandidateRequest{
		Application:  f.app,
		ConfigFamily: "managed_rules",
		Bundle: objectstore.Bundle{
			"rules.yaml": []byte("rules:\n  - phrase: baseline\n  - phrase: booked\n  - phrase: settled\n"),
		},
		Explanation:    "add the settled-phrase rule",
		BaseRevisionID: f.revision.RevisionID,
	})
	if err != nil {
		t.Fatalf("second candidate: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if second, err = f.engine.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: second.CandidateID, Kind: kind, Result: "PASS", SuiteID: "fixture." + kind,
		}); err != nil {
			t.Fatalf("validation: %v", err)
		}
	}
	if _, err := f.engine.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: second.CandidateID, Approver: "operator",
		ActorType: projection.ActorHuman, PeerUID: 1000,
	}); err != nil {
		t.Fatalf("approve second: %v", err)
	}
	promotion, err := f.engine.PromoteCandidate(ctx, second.CandidateID, "operator")
	if err != nil {
		t.Fatalf("promote second: %v", err)
	}

	updates, err := f.engine.Updates(ctx, f.app, f.installation, "")
	if err != nil {
		t.Fatalf("updates: %v", err)
	}
	if len(updates) != 2 {
		t.Fatalf("expected both approved revisions to be visible, got %d", len(updates))
	}
	if updates[0].RevisionID != promotion.Revision.RevisionID {
		t.Fatalf("newest update is %s, expected %s", updates[0].RevisionID, promotion.Revision.RevisionID)
	}
	if updates[0].RevisionSequence <= updates[1].RevisionSequence {
		t.Fatal("updates are not ordered newest first")
	}
}

// TestARejectedRevisionCanBeSteppedOver closes a deadlock the real loop found.
//
// Promotion is compare-and-swap, so a revision that follows a rejected one is
// necessarily built on top of it. The node will never apply the rejected
// revision — that is what rejecting means — so a rule of "exact base only"
// would leave the node permanently stuck one revision behind, with approved
// content it could never receive. Revisions the node has refused, or that an
// operator withdrew, may therefore be skipped; anything else may not.
func TestARejectedRevisionCanBeSteppedOver(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	// The node reports it is running the first approved revision.
	if _, err := f.engine.ReportApplied(ctx, engine.ReportAppliedRequest{
		ApplicationID: f.app, InstallationID: f.installation, ConfigFamilyID: f.family,
		RevisionID: f.revision.RevisionID, ObservedHash: f.revision.RootTreeHash,
	}); err != nil {
		t.Fatalf("report applied: %v", err)
	}

	// Two more revisions, each built on the one before.
	middle := f.approveAndPromote(t, "middle", f.revision.RevisionID)
	last := f.approveAndPromote(t, "last", middle.RevisionID)

	// The node is on the first revision; the newer one's base is the middle
	// revision, which the node has neither applied nor refused. That is a
	// genuine base mismatch.
	eligibility, err := f.engine.IsRevisionDeliverable(ctx, f.app, f.installation,
		f.family, last.RevisionID, f.revision.RevisionID)
	if err != nil {
		t.Fatalf("eligibility before the refusal: %v", err)
	}
	if eligibility.BaseCompatible {
		t.Fatal("a revision built on a revision the node never applied was reported compatible")
	}

	// Now the node refuses the middle revision.
	if _, err := f.engine.ReportRejected(ctx, engine.ReportRejectedRequest{
		ApplicationID: f.app, InstallationID: f.installation, ConfigFamilyID: f.family,
		RevisionID: middle.RevisionID, ReasonCode: "SCHEMA_UNSUPPORTED", Message: "no",
	}); err != nil {
		t.Fatalf("report rejected: %v", err)
	}

	eligibility, err = f.engine.IsRevisionDeliverable(ctx, f.app, f.installation,
		f.family, last.RevisionID, f.revision.RevisionID)
	if err != nil {
		t.Fatalf("eligibility after the refusal: %v", err)
	}
	if !eligibility.BaseCompatible {
		t.Fatalf("the node is still stuck behind a revision it refused: %s", eligibility.BaseReasonCode)
	}
	if len(eligibility.SkippedRevisions) != 1 || eligibility.SkippedRevisions[0] != middle.RevisionID {
		t.Fatalf("expected the refused revision to be reported as skipped, got %v", eligibility.SkippedRevisions)
	}

	// And the update is actually fetchable, carrying that fact.
	fetched, err := f.engine.FetchUpdate(ctx, f.app, f.installation, f.family, last.RevisionID, f.revision.RevisionID)
	if err != nil {
		t.Fatalf("fetch after the refusal: %v", err)
	}
	if len(fetched.Update.SkippedRevisions) != 1 {
		t.Fatalf("the delivered update does not report what it steps over: %v", fetched.Update.SkippedRevisions)
	}
}

// approveAndPromote is a convenience for building a chain of revisions.
func (f updateFixture) approveAndPromote(t *testing.T, phrase, base string) projection.ConfigRevision {
	t.Helper()
	ctx := context.Background()
	candidate, err := f.engine.CreateCandidate(ctx, engine.CandidateRequest{
		Application:  f.app,
		ConfigFamily: "managed_rules",
		Bundle: objectstore.Bundle{
			"rules.yaml": []byte("rules:\n  - phrase: baseline\n  - phrase: " + phrase + "\n"),
		},
		Explanation:    "add the " + phrase + " rule",
		BaseRevisionID: base,
	})
	if err != nil {
		t.Fatalf("candidate %s: %v", phrase, err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := f.engine.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS", SuiteID: "fixture." + kind,
		}); err != nil {
			t.Fatalf("validation %s: %v", phrase, err)
		}
	}
	if _, err := f.engine.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: candidate.CandidateID, Approver: "operator",
		ActorType: projection.ActorHuman, PeerUID: 1000,
	}); err != nil {
		t.Fatalf("approve %s: %v", phrase, err)
	}
	promotion, err := f.engine.PromoteCandidate(ctx, candidate.CandidateID, "operator")
	if err != nil {
		t.Fatalf("promote %s: %v", phrase, err)
	}
	return promotion.Revision
}

// TestBaseRevisionMismatchIsReported (mission section 55).
func TestBaseRevisionMismatchIsReported(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	eligibility, err := f.engine.IsRevisionDeliverable(ctx, f.app, f.installation,
		f.family, f.revision.RevisionID, "a-revision-this-node-is-not-on")
	if err != nil {
		t.Fatalf("eligibility: %v", err)
	}
	if eligibility.BaseCompatible {
		t.Fatal("a revision built on another base was reported compatible")
	}
	if eligibility.BaseReasonCode != engine.NotBaseMismatch {
		t.Fatalf("base reason code %s, expected %s", eligibility.BaseReasonCode, engine.NotBaseMismatch)
	}
}

// TestRebuildPreservesUpdateHistory (mission section 64).
func TestRebuildPreservesUpdateHistory(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	if _, err := f.engine.ReportApplied(ctx, engine.ReportAppliedRequest{
		ApplicationID: f.app, InstallationID: f.installation, ConfigFamilyID: f.family,
		RevisionID: f.revision.RevisionID, ObservedHash: f.revision.RootTreeHash,
	}); err != nil {
		t.Fatalf("report applied: %v", err)
	}
	if _, err := f.engine.WithdrawUpdate(ctx, f.app, f.installation, f.family,
		f.revision.RevisionID, "uid:1000", "second thoughts"); err != nil {
		t.Fatalf("withdraw: %v", err)
	}

	before := refSnapshot(t, f, projection.RefObserved, engine.RefLastGood)
	resultsBefore, err := f.engine.ApplicationResults(f.app, f.installation, 50)
	if err != nil {
		t.Fatalf("results: %v", err)
	}
	if len(resultsBefore) == 0 {
		t.Fatal("fixture problem: nothing to rebuild")
	}

	if _, err := f.engine.Rebuild(); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	resultsAfter, err := f.engine.ApplicationResults(f.app, f.installation, 50)
	if err != nil {
		t.Fatalf("results after rebuild: %v", err)
	}
	if len(resultsAfter) != len(resultsBefore) {
		t.Fatalf("rebuild lost application results: %d before, %d after",
			len(resultsBefore), len(resultsAfter))
	}
	for i := range resultsBefore {
		if resultsAfter[i].ResultID != resultsBefore[i].ResultID {
			t.Fatalf("result %d changed across rebuild: %s then %s",
				i, resultsBefore[i].ResultID, resultsAfter[i].ResultID)
		}
	}
	dispositionsAfter, err := f.engine.UpdateDispositions(f.app, f.installation, "", 50)
	if err != nil {
		t.Fatalf("dispositions after rebuild: %v", err)
	}
	if len(dispositionsAfter) == 0 {
		t.Fatal("rebuild lost the update disposition")
	}
	if dispositionsAfter[0].State != projection.UpdateWithdrawn {
		t.Fatalf("withdrawal did not survive the rebuild: %s", dispositionsAfter[0].State)
	}

	after := refSnapshot(t, f, projection.RefObserved, engine.RefLastGood)
	for _, name := range []string{projection.RefObserved, engine.RefLastGood} {
		if after[name] != before[name] {
			t.Fatalf("rebuild moved %s from %q to %q", name, before[name], after[name])
		}
	}
	if after[projection.RefObserved] != f.revision.RevisionID {
		t.Fatalf("observed after rebuild is %q, expected %s",
			after[projection.RefObserved], f.revision.RevisionID)
	}
}

// TestConcurrentUpdateChecksAreSafe (mission section 62).
func TestConcurrentUpdateChecksAreSafe(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			updates, err := f.engine.Updates(ctx, f.app, f.installation, "")
			if err != nil {
				errs <- err
				return
			}
			if len(updates) != 1 {
				errs <- errors.New("concurrent check saw a different answer")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Update: %v", err)
	}
}

// TestConcurrentFetchAndReportAreSafe (mission section 62).
func TestConcurrentFetchAndReportAreSafe(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := f.engine.FetchUpdate(ctx, f.app, f.installation, f.family,
				f.revision.RevisionID, f.base.RevisionID); err != nil {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := f.engine.ReportApplied(ctx, engine.ReportAppliedRequest{
				ApplicationID: f.app, InstallationID: f.installation, ConfigFamilyID: f.family,
				RevisionID: f.revision.RevisionID, ObservedHash: f.revision.RootTreeHash,
			}); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent fetch/report: %v", err)
	}
}

// TestNodeOutageIsNotTreatedAsSuccess (mission section 65).
func TestNodeOutageIsNotTreatedAsSuccess(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	before := refSnapshot(t, f, projection.RefObserved, engine.RefLastGood)
	if _, err := f.engine.FetchUpdate(ctx, f.app, f.installation, f.family,
		f.revision.RevisionID, f.base.RevisionID); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	// The node fetched and then vanished. Nothing may be assumed.
	dispositions, err := f.engine.UpdateDispositions(f.app, f.installation, "", 50)
	if err != nil {
		t.Fatalf("dispositions: %v", err)
	}
	if len(dispositions) != 1 {
		t.Fatalf("expected one disposition after fetch, got %d", len(dispositions))
	}
	if dispositions[0].State != projection.UpdateFetched {
		t.Fatalf("state after fetch is %s, expected FETCHED", dispositions[0].State)
	}
	after := refSnapshot(t, f, projection.RefObserved, engine.RefLastGood)
	for _, name := range []string{projection.RefObserved, engine.RefLastGood} {
		if after[name] != before[name] {
			t.Fatalf("a fetch alone moved %s from %q to %q: fetching is not applying",
				name, before[name], after[name])
		}
	}
}

// TestDeferralBlocksDelivery: a DEFERRED decision is a decision, and the gate
// reads the latest one (mission section 46).
func TestDeferralBlocksDelivery(t *testing.T) {
	ctx := context.Background()
	f := newUpdateFixture(t, true)

	if _, err := f.engine.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: f.candidate.CandidateID, Approver: "operator",
		ActorType: projection.ActorHuman, PeerUID: 1000,
		Decision: "DEFERRED", Comment: "not this week",
	}); err != nil {
		t.Fatalf("defer: %v", err)
	}

	updates, err := f.engine.Updates(ctx, f.app, f.installation, "")
	if err != nil {
		t.Fatalf("updates: %v", err)
	}
	if len(updates) != 0 {
		t.Fatalf("a deferred candidate is still deliverable: %+v", updates)
	}

	// A later explicit approval re-opens it, because the decision history says
	// so rather than a mutable flag somewhere.
	if _, err := f.engine.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: f.candidate.CandidateID, Approver: "operator",
		ActorType: projection.ActorHuman, PeerUID: 1000, Comment: "go ahead",
	}); err != nil {
		t.Fatalf("re-approve: %v", err)
	}
	updates, err = f.engine.Updates(ctx, f.app, f.installation, "")
	if err != nil {
		t.Fatalf("updates: %v", err)
	}
	if len(updates) != 1 {
		t.Fatalf("re-approval did not restore delivery: %d updates", len(updates))
	}
}
