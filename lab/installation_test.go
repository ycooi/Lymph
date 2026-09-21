package lab

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
)

// twoInstallations builds one application with two deployments.
func twoInstallations(t *testing.T, e *engine.Engine) (App, string, string) {
	t.Helper()
	sim := MCPSim()
	regionA := installationSpec("region-a", "office")
	regionB := installationSpec("region-b", "production")
	// With more than one installation the manifest must say which is default.
	app := setupAppWith(t, e, sim, []engine.InstallationSpec{regionA, regionB}, regionA.InstallationID)
	if len(app.Installations()) != 2 {
		t.Fatalf("expected 2 installations, got %d", len(app.Installations()))
	}
	return app, app.Installations()[0], app.Installations()[1]
}

// TestS16Installation is the graded item: configuration state is per
// installation, while learning still aggregates per application.
func TestS16Installation(t *testing.T) {
	const id = "S16_installation"
	ctx := context.Background()
	root := openStoreRoot(t, t.TempDir())
	e := openEngineAt(t, root)

	app, x, y := twoInstallations(t, e)

	// Each installation gets its own baseline ref.
	xBase := activeRevisionFor(t, e, app, x)
	yBase := activeRevisionFor(t, e, app, y)

	// Give X a newer active revision than Y, without touching Y.
	xNewer, err := e.CreateRevision(ctx, engine.RevisionRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID, InstallationID: x,
		Bundle:  objectstore.Bundle{"rules.yaml": []byte("# rules for X\nversion: 2\nrules:\n  - id: a\n    phrase: awarded\n    state: AWARDED\n")},
		Message: "X moves ahead", Author: "lab",
	})
	if err != nil {
		t.Fatalf("create X revision: %v", err)
	}
	expected := xBase.RevisionID
	if _, err := e.SetRef(ctx, engine.SetRefRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID, InstallationID: x,
		RefName: engine.RefActive, RevisionID: xNewer.RevisionID,
		ExpectedOld: &expected, Reason: "installation X moved ahead", Actor: "lab",
	}); err != nil {
		t.Fatalf("move X active: %v", err)
	}

	xActive := activeRevisionFor(t, e, app, x)
	yActive := activeRevisionFor(t, e, app, y)
	if xActive.RevisionID == yActive.RevisionID {
		Failf(t, id, "§16 installation identity", "two installations hold different revisions",
			"X and Y both point at %s", shorten(xActive.RevisionID))
		return
	}
	if yActive.RevisionID != yBase.RevisionID {
		Failf(t, id, "§16 installation identity", "moving X does not move Y",
			"Y moved to %s", shorten(yActive.RevisionID))
		return
	}

	// A candidate built on X's active revision promotes only X.
	xWork, _ := leasedWork(t, e, app, "x-worker", map[string]any{"text": "booked with acme"})
	candidateX, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID, InstallationID: x,
		Bundle: objectstore.Bundle{"rules.yaml": bundle("# rules", semanticDoc{Version: 3, Rules: []SemanticRule{
			{ID: "a", Phrase: "awarded", State: "AWARDED"},
			{ID: "b", Phrase: "business was booked with", State: "AWARDED"},
		}})},
		IssueIDs: []string{xWork.IssueIDs[0]}, WorkItemID: xWork.WorkID, Explanation: "X-only fix",
	})
	if err != nil {
		t.Fatalf("candidate on X: %v", err)
	}
	if candidateX.InstallationID != x {
		Failf(t, id, "§16 installation identity", "a candidate records its installation",
			"candidate installation is %q, expected %q", candidateX.InstallationID, x)
		return
	}
	if candidateX.BaseRevisionID != xActive.RevisionID {
		Failf(t, id, "§16 installation identity", "the candidate bases on its installation's active revision",
			"base %s, X active %s", shorten(candidateX.BaseRevisionID), shorten(xActive.RevisionID))
		return
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidateX.CandidateID, Kind: kind, Result: "PASS", SuiteID: "mcp." + kind,
		}); err != nil {
			t.Fatalf("validation: %v", err)
		}
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{CandidateID: candidateX.CandidateID, Approver: "operator"}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	promotion, err := e.PromoteCandidate(ctx, candidateX.CandidateID, "lab")
	if err != nil {
		t.Fatalf("promote X: %v", err)
	}

	xAfter := activeRevisionFor(t, e, app, x)
	yAfter := activeRevisionFor(t, e, app, y)
	if xAfter.RevisionID != promotion.Revision.RevisionID {
		Failf(t, id, "§16 installation identity", "promotion moves only its own installation",
			"X active is %s, expected %s", shorten(xAfter.RevisionID), shorten(promotion.Revision.RevisionID))
		return
	}
	if yAfter.RevisionID != yBase.RevisionID {
		Failf(t, id, "§16 installation identity", "promotion moves only its own installation",
			"Y moved to %s", shorten(yAfter.RevisionID))
		return
	}

	// The reflog says which installation moved.
	reflogX, err := e.DB().ListReflog(app.ApplicationID(), app.FamilyID, x, 50)
	if err != nil {
		t.Fatalf("reflog X: %v", err)
	}
	reflogY, err := e.DB().ListReflog(app.ApplicationID(), app.FamilyID, y, 50)
	if err != nil {
		t.Fatalf("reflog Y: %v", err)
	}
	for _, entry := range reflogX {
		if entry.InstallationID != x {
			Failf(t, id, "§16 installation identity", "reflog entries name their installation",
				"an X entry names %q", entry.InstallationID)
			return
		}
	}
	movedActive := 0
	for _, entry := range reflogX {
		if entry.RefName == engine.RefActive {
			movedActive++
		}
	}
	// X's active ref moved three times: the baseline import, the explicit move
	// that put it ahead of Y, and the promotion.
	if movedActive != 3 {
		Failf(t, id, "§16 installation identity", "reflog records this installation's moves",
			"X has %d active moves, Y has %d entries", movedActive, len(reflogY))
		return
	}
	yActiveMoves := 0
	for _, entry := range reflogY {
		if entry.RefName == engine.RefActive {
			yActiveMoves++
		}
	}
	if yActiveMoves != 1 {
		Failf(t, id, "§16 installation identity", "the untouched installation keeps its single baseline move",
			"Y has %d active moves", yActiveMoves)
		return
	}

	// Managed targets are installation-specific.
	targets, err := e.DB().ListTargets(app.FamilyID)
	if err != nil {
		t.Fatalf("targets: %v", err)
	}
	if len(targets) != 2 {
		Failf(t, id, "§16 installation identity", "each installation has its own managed target",
			"%d targets for two installations", len(targets))
		return
	}
	seenTargets := map[string]bool{}
	for _, target := range targets {
		if target.InstallationID == "" {
			Failf(t, id, "§16 installation identity", "managed targets carry an installation",
				"target %s has none", target.Path)
			return
		}
		seenTargets[target.InstallationID] = true
	}
	if len(seenTargets) != 2 {
		Failf(t, id, "§16 installation identity", "managed targets cover both installations",
			"targets belong to %d installations", len(seenTargets))
		return
	}

	// Learning still aggregates: one unknown seen in both deployments is one
	// issue (mission section 72).
	emitFull(t, e, app.ApplicationID(), app.Sim.Junction, app.Sim.Feedback, app.Sim.Reason,
		map[string]any{"text": "awarded by the ministry"}, "prod-x",
		xAfter.RevisionID, time.Now().UTC(), "", x)
	shared := emitFull(t, e, app.ApplicationID(), app.Sim.Junction, app.Sim.Feedback, app.Sim.Reason,
		map[string]any{"text": "awarded by the ministry"}, "prod-y",
		yBase.RevisionID, time.Now().UTC(), "", y)
	sharedIssue, err := e.DB().GetIssue(shared.IssueID)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if sharedIssue.OccurrenceCount != 2 || sharedIssue.UniqueSources != 2 {
		Failf(t, id, "§16 installation identity", "events from both installations make one issue",
			"occurrence %d, sources %d", sharedIssue.OccurrenceCount, sharedIssue.UniqueSources)
		return
	}

	// Everything survives a projection rebuild.
	digestBefore := stateDigest(t, e)
	e.Close()
	if err := removeProjectionFiles(root); err != nil {
		t.Fatalf("remove projection: %v", err)
	}
	rebuilt := openEngineAt(t, root)
	if got := stateDigest(t, rebuilt); got != digestBefore {
		Failf(t, id, "§16 installation identity", "installation state survives a projection rebuild",
			"digest changed: %s vs %s", shorten(digestBefore), shorten(got))
		return
	}
	if activeRevisionFor(t, rebuilt, app, x).RevisionID != xAfter.RevisionID {
		Failf(t, id, "§16 installation identity", "X's ref comes back",
			"X is at %s after rebuild", shorten(activeRevisionFor(t, rebuilt, app, x).RevisionID))
		return
	}
	if activeRevisionFor(t, rebuilt, app, y).RevisionID != yBase.RevisionID {
		Failf(t, id, "§16 installation identity", "Y's ref comes back",
			"Y is at %s after rebuild", shorten(activeRevisionFor(t, rebuilt, app, y).RevisionID))
		return
	}

	Passf(t, id, "§16 installation identity", "per-installation configuration state",
		"one application, two installations (%s, %s) sharing a config family: X moved to r%d while Y stayed at r%d; a candidate on X promoted only X; %d active reflog moves for X; 2 installation-scoped managed targets; one unknown seen in both deployments stayed one issue with %d occurrences across %d sources; a projection rebuild reproduced all of it",
		shorten(x), shorten(y), xAfter.Sequence, yBase.Sequence, movedActive,
		sharedIssue.OccurrenceCount, sharedIssue.UniqueSources)
}

// TestL25CrossInstallationStale is Hard Gate D: one installation moving must not
// stale another installation's candidate (mission sections 67, 94).
func TestL25CrossInstallationStale(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t)
	app, x, y := twoInstallations(t, e)

	makeCandidate := func(installation, name string) projection.Candidate {
		t.Helper()
		work, _ := leasedWork(t, e, app, name+"-worker", map[string]any{"text": "awarded " + name})
		candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
			Application: app.ApplicationID(), ConfigFamily: app.FamilyID, InstallationID: installation,
			Bundle: objectstore.Bundle{"rules.yaml": bundle("# rules", semanticDoc{Version: 2, Rules: []SemanticRule{
				{ID: name, Phrase: "awarded " + name, State: "AWARDED"},
			}})},
			IssueIDs: []string{work.IssueIDs[0]}, WorkItemID: work.WorkID, Explanation: name,
		})
		if err != nil {
			t.Fatalf("candidate %s: %v", name, err)
		}
		for _, kind := range []string{"structural", "replay", "regression"} {
			if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
				CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS", SuiteID: "mcp." + kind,
			}); err != nil {
				t.Fatalf("validation %s: %v", name, err)
			}
		}
		if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{CandidateID: candidate.CandidateID, Approver: "operator"}); err != nil {
			t.Fatalf("approve %s: %v", name, err)
		}
		return candidate
	}

	xa := makeCandidate(x, "xa")
	xb := makeCandidate(x, "xb")
	ya := makeCandidate(y, "ya")

	if _, err := e.PromoteCandidate(ctx, xb.CandidateID, "lab"); err != nil {
		t.Fatalf("promote X-B: %v", err)
	}

	if _, err := e.PromoteCandidate(ctx, xa.CandidateID, "lab"); !errors.Is(err, projection.ErrStaleCandidate) {
		Failf(t, "L25_cross_install", "§67 cross-installation staleness", "a stale candidate in the same installation is refused",
			"promoting X-A returned %v", err)
		return
	}
	if _, err := e.PromoteCandidate(ctx, ya.CandidateID, "lab"); err != nil {
		Failf(t, "L25_cross_install", "§67 cross-installation staleness", "another installation's promotion does not stale this candidate",
			"promoting Y-A failed: %v", err)
		return
	}

	xActive := activeRevisionFor(t, e, app, x)
	yActive := activeRevisionFor(t, e, app, y)
	if xActive.RevisionID == yActive.RevisionID {
		Failf(t, "L25_cross_install", "§67 cross-installation staleness", "each installation ends on its own revision",
			"both are at %s", shorten(xActive.RevisionID))
		return
	}
	stale, err := e.DB().GetCandidate(xa.CandidateID)
	if err != nil {
		t.Fatalf("reload X-A: %v", err)
	}
	if stale.State != string(engine.CandidateStale) {
		Failf(t, "L25_cross_install", "§67 cross-installation staleness", "the stale candidate is preserved as STALE",
			"X-A is %s", stale.State)
		return
	}

	Note(t, "S16_installation", "L25_cross_install", "§67 cross-installation staleness", "staleness is per installation",
		"X-B promoted; X-A refused as STALE and kept; Y-A promoted successfully because Y never moved; X ends at r%d and Y at r%d",
		xActive.Sequence, yActive.Sequence)
}

// TestL25CandidateBranchRefs is mission section 69.
func TestL25CandidateBranchRefs(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t)
	app, x, y := twoInstallations(t, e)

	work, _ := leasedWork(t, e, app, "x-worker", map[string]any{"text": "awarded"})
	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID, InstallationID: x,
		Bundle:   objectstore.Bundle{"rules.yaml": bundle("# rules", semanticDoc{Version: 2, Rules: []SemanticRule{{ID: "a", Phrase: "awarded", State: "AWARDED"}}})},
		IssueIDs: []string{work.IssueIDs[0]}, WorkItemID: work.WorkID, Explanation: "X branch",
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}

	branchName := engine.CandidateRefName(candidate.CandidateID)
	if _, err := e.DB().GetRef(app.ApplicationID(), app.FamilyID, x, branchName); err != nil {
		Failf(t, "L25_branch_ref", "§69 candidate branch refs", "the branch exists under its own installation",
			"X has no ref %s: %v", shorten(branchName), err)
		return
	}
	if _, err := e.DB().GetRef(app.ApplicationID(), app.FamilyID, y, branchName); err == nil {
		Failf(t, "L25_branch_ref", "§69 candidate branch refs", "the branch does not leak to another installation",
			"Y also has ref %s", shorten(branchName))
		return
	}

	xRefs, err := e.DB().ListRefs(app.ApplicationID(), app.FamilyID, x)
	if err != nil {
		t.Fatalf("refs X: %v", err)
	}
	yRefs, err := e.DB().ListRefs(app.ApplicationID(), app.FamilyID, y)
	if err != nil {
		t.Fatalf("refs Y: %v", err)
	}
	if len(xRefs) != 6 || len(yRefs) != 5 {
		Failf(t, "L25_branch_ref", "§69 candidate branch refs", "each installation holds its own ref set",
			"X has %d refs (5 pointers + 1 branch expected), Y has %d", len(xRefs), len(yRefs))
		return
	}

	Note(t, "S16_installation", "L25_branch_ref", "§69 candidate branch refs", "candidate branches are installation-scoped",
		"the branch ref for candidate %s exists under X (%d refs) and not under Y (%d refs)",
		shorten(candidate.CandidateID), len(xRefs), len(yRefs))
}

// TestL25SameNamesEverywhere is mission section 68: identical names on both
// axes still resolve to different identities.
func TestL25SameNamesEverywhere(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t)

	makeApp := func(name string) App {
		t.Helper()
		sim := &Simulator{
			Name:     name,
			Family:   "default",
			File:     "config.yaml",
			Junction: "parser.default",
			Workflow: "parser-default-improvement",
			Owner:    "lab",
			Baseline: objectstore.Bundle{"config.yaml": []byte("version: 1\nmode: strict\n")},
		}
		siteX := installationSpec("x", "site-x")
		siteY := installationSpec("y", "site-y")
		return setupAppWith(t, e, sim, []engine.InstallationSpec{siteX, siteY}, siteX.InstallationID)
	}

	alpha := makeApp("alpha")
	beta := makeApp("beta")

	identities := map[string]bool{}
	for _, app := range []App{alpha, beta} {
		if identities[app.ApplicationID()] {
			t.Fatalf("two applications share an identity")
		}
		identities[app.ApplicationID()] = true
		for _, installation := range app.Installations() {
			if identities[installation] {
				t.Fatalf("two installations share an identity")
			}
			identities[installation] = true
		}
	}

	// Drive one application's installation forward; the other three must not
	// move.
	target := alpha.Installation(0)
	base := activeRevisionFor(t, e, alpha, target)
	revision, err := e.CreateRevision(ctx, engine.RevisionRequest{
		Application: alpha.ApplicationID(), ConfigFamily: alpha.FamilyID, InstallationID: target,
		Bundle:  objectstore.Bundle{"config.yaml": []byte("version: 2\nmode: strict\nextra: yes\n")},
		Message: "only alpha/site-x moves",
	})
	if err != nil {
		t.Fatalf("revision: %v", err)
	}
	expected := base.RevisionID
	if _, err := e.SetRef(ctx, engine.SetRefRequest{
		Application: alpha.ApplicationID(), ConfigFamily: alpha.FamilyID, InstallationID: target,
		RefName: engine.RefActive, RevisionID: revision.RevisionID,
		ExpectedOld: &expected, Reason: "same-names test",
	}); err != nil {
		t.Fatalf("ref move: %v", err)
	}

	moved := 0
	for _, app := range []App{alpha, beta} {
		for _, installation := range app.Installations() {
			active := activeRevisionFor(t, e, app, installation)
			if active.RevisionID != revision.RevisionID {
				continue
			}
			moved++
			if app.ApplicationID() != alpha.ApplicationID() || installation != target {
				Failf(t, "L25_same_names", "§68 same names everywhere", "identities do not collide",
					"%s/%s unexpectedly points at the new revision", app.Sim.Name, shorten(installation))
				return
			}
		}
	}
	if moved != 1 {
		Failf(t, "L25_same_names", "§68 same names everywhere", "exactly one installation moved",
			"%d installations point at the new revision", moved)
		return
	}

	Note(t, "S16_installation", "L25_same_names", "§68 same names everywhere", "name collisions are harmless",
		"2 applications x 2 installations sharing family %q, junction %q and file %q resolved to 2 application identities and 4 installation identities; promoting one left the other three untouched",
		"default", "parser.default", "config.yaml")
}

// TestL25AmbiguousInstallation is mission section 60.
func TestL25AmbiguousInstallation(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t)
	app, x, y := twoInstallations(t, e)

	xEvent := emitFull(t, e, app.ApplicationID(), app.Sim.Junction, app.Sim.Feedback, app.Sim.Reason,
		map[string]any{"text": "distinct phrasing one"}, "prod-x", app.Baseline.RevisionID, time.Now().UTC(), "", x)
	yEvent := emitFull(t, e, app.ApplicationID(), app.Sim.Junction, app.Sim.Feedback, app.Sim.Reason,
		map[string]any{"text": "distinct phrasing two"}, "prod-y", app.Baseline.RevisionID, time.Now().UTC(), "", y)

	single, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{xEvent.IssueID}})
	if err != nil {
		Failf(t, "L25_ambiguous", "§60 resolving installation from issues", "a single-installation issue is unambiguous",
			"queue failed: %v", err)
		return
	}
	if single.InstallationID != x {
		Failf(t, "L25_ambiguous", "§60 resolving installation from issues", "the work item takes the issue's installation",
			"work item installation is %q, expected %q", single.InstallationID, x)
		return
	}

	if _, err := e.QueueWork(ctx, engine.QueueIssueRequest{
		IssueIDs: []string{xEvent.IssueID, yEvent.IssueID},
	}); !errors.Is(err, engine.ErrAmbiguousInstallation) {
		Failf(t, "L25_ambiguous", "§60 resolving installation from issues", "multi-installation work is refused, not guessed",
			"queueing issues from two installations returned %v", err)
		return
	}

	explicit, err := e.QueueWork(ctx, engine.QueueIssueRequest{
		IssueIDs: []string{xEvent.IssueID, yEvent.IssueID}, InstallationID: y,
	})
	if err != nil {
		Failf(t, "L25_ambiguous", "§60 resolving installation from issues", "an explicit installation resolves the ambiguity",
			"queue failed: %v", err)
		return
	}
	if explicit.InstallationID != y {
		Failf(t, "L25_ambiguous", "§60 resolving installation from issues", "the explicit installation is used",
			"work item installation is %q", explicit.InstallationID)
		return
	}

	other := setupApp(t, e, MLSim())
	if _, err := e.QueueWork(ctx, engine.QueueIssueRequest{
		IssueIDs: []string{xEvent.IssueID}, InstallationID: other.ApplicationID(),
	}); err == nil {
		Failf(t, "L25_ambiguous", "§60 resolving installation from issues", "a foreign installation is refused",
			"an installation belonging to another application was accepted")
		return
	}

	Note(t, "S16_installation", "L25_ambiguous", "§60 resolving installation from issues", "installation is inferred or refused, never guessed",
		"one issue produced work for installation %s; two installations in one work item was refused with %v; an explicit installation resolved it; an installation belonging to another application was refused",
		shorten(x), engine.ErrAmbiguousInstallation)
}

// TestL25ProjectionUpgrade is mission sections 48 and 49.
func TestL25ProjectionUpgrade(t *testing.T) {
	ctx := context.Background()
	root := openStoreRoot(t, t.TempDir())
	e := openEngineAt(t, root)

	app, x, y := twoInstallations(t, e)
	emitFull(t, e, app.ApplicationID(), app.Sim.Junction, app.Sim.Feedback, app.Sim.Reason,
		map[string]any{"text": "awarded"}, "prod-x", app.Baseline.RevisionID, time.Now().UTC(), "", x)
	emitFull(t, e, app.ApplicationID(), app.Sim.Junction, app.Sim.Feedback, app.Sim.Reason,
		map[string]any{"text": "awarded"}, "prod-y", app.Baseline.RevisionID, time.Now().UTC(), "", y)

	logicalBefore := stateDigest(t, e)
	ledgerBefore := hashTree(t, filepath.Join(root, "ledger"))
	objectsBefore := hashTree(t, filepath.Join(root, "objects"))
	e.Close()

	// Pretend the projection was written by the previous schema version.
	setProjectionSchemaVersion(t, root, "1")

	upgraded := openEngineAt(t, root)
	upgrade := upgraded.DB().Upgrade()
	if !upgrade.Rebuilt || upgrade.FromVersion != "1" {
		Failf(t, "L25_upgrade", "§48 projection schema upgrade", "a schema mismatch rebuilds the projection",
			"upgrade reported %+v", upgrade)
		return
	}
	if got := hashTree(t, filepath.Join(root, "ledger")); got != ledgerBefore {
		Failf(t, "L25_upgrade", "§48 projection schema upgrade", "the canonical ledger is untouched",
			"ledger digest changed: %s vs %s", shorten(ledgerBefore), shorten(got))
		return
	}
	if got := hashTree(t, filepath.Join(root, "objects")); got != objectsBefore {
		Failf(t, "L25_upgrade", "§48 projection schema upgrade", "the object store is untouched",
			"object digest changed: %s vs %s", shorten(objectsBefore), shorten(got))
		return
	}
	if got := stateDigest(t, upgraded); got != logicalBefore {
		Failf(t, "L25_upgrade", "§48 projection schema upgrade", "no logical history disappears",
			"logical digest changed: %s vs %s", shorten(logicalBefore), shorten(got))
		return
	}
	if activeRevisionFor(t, upgraded, app, x).RevisionID == "" || activeRevisionFor(t, upgraded, app, y).RevisionID == "" {
		Failf(t, "L25_upgrade", "§48 projection schema upgrade", "installation refs return",
			"an installation has no active ref after the upgrade")
		return
	}

	Note(t, "S24_sqlite_rebuild", "L25_upgrade", "§48 projection schema upgrade", "disposable projection, rebuilt not migrated",
		"projection schema 1 -> %s: the daemon detected the mismatch, dropped the projection and replayed the ledger; ledger and object digests are byte-identical and the logical state digest is unchanged",
		upgrade.ToVersion)
	_ = ctx
}

// TestL25ReflogSurvives is mission section 70.
func TestL25ReflogSurvives(t *testing.T) {
	ctx := context.Background()
	root := openStoreRoot(t, t.TempDir())
	e := openEngineAt(t, root)
	app, _, y := twoInstallations(t, e)

	work, _ := leasedWork(t, e, app, "w", map[string]any{"text": "awarded"})
	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app.ApplicationID(), ConfigFamily: app.FamilyID, InstallationID: y,
		Bundle:   objectstore.Bundle{"rules.yaml": bundle("# rules", semanticDoc{Version: 2, Rules: []SemanticRule{{ID: "a", Phrase: "awarded", State: "AWARDED"}}})},
		IssueIDs: []string{work.IssueIDs[0]}, WorkItemID: work.WorkID, Explanation: "reflog fixture",
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS", SuiteID: "mcp." + kind,
		}); err != nil {
			t.Fatalf("validation: %v", err)
		}
	}
	if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{CandidateID: candidate.CandidateID, Approver: "operator"}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if _, err := e.PromoteCandidate(ctx, candidate.CandidateID, "lab"); err != nil {
		t.Fatalf("promote: %v", err)
	}

	reflogBefore, err := e.DB().ListReflog(app.ApplicationID(), app.FamilyID, y, 100)
	if err != nil {
		t.Fatalf("reflog: %v", err)
	}
	if len(reflogBefore) == 0 {
		t.Fatalf("no reflog entries were written")
	}
	digestBefore := stateDigest(t, e)
	e.Close()

	restarted := openEngineAt(t, root)
	reflogRestart, err := restarted.DB().ListReflog(app.ApplicationID(), app.FamilyID, y, 100)
	if err != nil {
		t.Fatalf("reflog after restart: %v", err)
	}
	if len(reflogRestart) != len(reflogBefore) {
		Failf(t, "L25_reflog", "§70 reflog preserves installation", "reflog survives restart",
			"%d entries before, %d after", len(reflogBefore), len(reflogRestart))
		return
	}
	if _, err := restarted.Rebuild(); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if got := stateDigest(t, restarted); got != digestBefore {
		Failf(t, "L25_reflog", "§70 reflog preserves installation", "reflog survives rebuild",
			"digest changed: %s vs %s", shorten(digestBefore), shorten(got))
		return
	}
	restarted.Close()

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
	reflogRestored, err := recovered.DB().ListReflog(app.ApplicationID(), app.FamilyID, y, 100)
	if err != nil {
		t.Fatalf("reflog after restore: %v", err)
	}
	for i := range reflogBefore {
		if i >= len(reflogRestored) || !sameReflogEntry(reflogBefore[i], reflogRestored[i]) {
			Failf(t, "L25_reflog", "§70 reflog preserves installation", "reflog survives disaster restore",
				"entry %d changed after restore", i)
			return
		}
	}

	Note(t, "S16_installation", "L25_reflog", "§70 reflog preserves installation", "ref movements keep their installation",
		"%d reflog entries for installation %s, each naming its installation, survived a restart, a projection rebuild and a restore of only identity.json + ledger/ + objects/, byte-identical",
		len(reflogRestored), shorten(y))
}

// helpers

// TestL25ConcurrentInstallations is mission section 85 and Hard Gate D under
// the race detector: promotions in different installations are independent,
// promotions in the same installation still have exactly one winner.
func TestL25ConcurrentInstallations(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t)
	app, x, y := twoInstallations(t, e)

	// One candidate per installation, plus a crowd of racers on X.
	makeCandidate := func(installation, name string) projection.Candidate {
		t.Helper()
		work, _ := leasedWork(t, e, app, "w-"+name, map[string]any{"text": "awarded " + name})
		candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
			Application: app.ApplicationID(), ConfigFamily: app.FamilyID, InstallationID: installation,
			Bundle: objectstore.Bundle{"rules.yaml": bundle("# rules", semanticDoc{Version: 2, Rules: []SemanticRule{
				{ID: name, Phrase: "awarded " + name, State: "AWARDED"},
			}})},
			IssueIDs: []string{work.IssueIDs[0]}, WorkItemID: work.WorkID, Explanation: name,
		})
		if err != nil {
			t.Fatalf("candidate %s: %v", name, err)
		}
		for _, kind := range []string{"structural", "replay", "regression"} {
			if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
				CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS", SuiteID: "mcp." + kind,
			}); err != nil {
				t.Fatalf("validation %s: %v", name, err)
			}
		}
		if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
			CandidateID: candidate.CandidateID, Approver: "operator",
		}); err != nil {
			t.Fatalf("approve %s: %v", name, err)
		}
		return candidate
	}

	xCandidate := makeCandidate(x, "x-cross")
	yCandidate := makeCandidate(y, "y-cross")

	// Cross-installation: both must succeed.
	type outcome struct {
		label      string
		revisionID string
		err        error
	}
	results := make(chan outcome, 2)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, entry := range []struct {
		label     string
		candidate projection.Candidate
	}{
		{"X", xCandidate},
		{"Y", yCandidate},
	} {
		wg.Add(1)
		go func(label, candidateID string) {
			defer wg.Done()
			<-start
			promotion, err := e.PromoteCandidate(ctx, candidateID, "lab")
			results <- outcome{label: label, revisionID: promotion.Revision.RevisionID, err: err}
		}(entry.label, entry.candidate.CandidateID)
	}
	close(start)
	wg.Wait()
	close(results)

	byLabel := map[string]outcome{}
	for result := range results {
		byLabel[result.label] = result
		if result.err != nil {
			Failf(t, "L25_concurrent_install", "§85 concurrent installations", "both installations promote independently",
				"%s failed: %v", result.label, result.err)
			return
		}
	}
	xActive := activeRevisionFor(t, e, app, x)
	yActive := activeRevisionFor(t, e, app, y)
	if xActive.RevisionID != byLabel["X"].revisionID || yActive.RevisionID != byLabel["Y"].revisionID {
		Failf(t, "L25_concurrent_install", "§85 concurrent installations", "each installation keeps its own winner",
			"X at %s (expected %s), Y at %s (expected %s)",
			shorten(xActive.RevisionID), shorten(byLabel["X"].revisionID),
			shorten(yActive.RevisionID), shorten(byLabel["Y"].revisionID))
		return
	}

	// Same installation: twenty racers, one winner (unchanged from L2).
	const racers = 20
	racersList := make([]projection.Candidate, 0, racers)
	for i := 0; i < racers; i++ {
		racersList = append(racersList, makeCandidate(x, fmt.Sprintf("racer%d", i)))
	}
	sameResults := make(chan outcome, racers)
	var sameWG sync.WaitGroup
	sameStart := make(chan struct{})
	for i, candidate := range racersList {
		sameWG.Add(1)
		go func(index int, candidateID string) {
			defer sameWG.Done()
			<-sameStart
			promotion, err := e.PromoteCandidate(ctx, candidateID, "lab")
			sameResults <- outcome{label: fmt.Sprintf("racer%d", index), revisionID: promotion.Revision.RevisionID, err: err}
		}(i, candidate.CandidateID)
	}
	close(sameStart)
	sameWG.Wait()
	close(sameResults)

	winners, stale := 0, 0
	var winnerRevision string
	for result := range sameResults {
		switch {
		case result.err == nil:
			winners++
			winnerRevision = result.revisionID
		case errors.Is(result.err, projection.ErrStaleCandidate):
			stale++
		default:
			t.Logf("%s failed with %v", result.label, result.err)
		}
	}
	if winners != 1 {
		Failf(t, "L25_concurrent_install", "§85 concurrent installations", "one winner per installation",
			"%d of %d racers on X succeeded", winners, racers)
		return
	}
	if finalActive := activeRevisionFor(t, e, app, x); finalActive.RevisionID != winnerRevision {
		Failf(t, "L25_concurrent_install", "§85 concurrent installations", "the winner holds the ref",
			"X is at %s, winner was %s", shorten(finalActive.RevisionID), shorten(winnerRevision))
		return
	}
	if yAfter := activeRevisionFor(t, e, app, y); yAfter.RevisionID != yActive.RevisionID {
		Failf(t, "L25_concurrent_install", "§85 concurrent installations", "X's race does not move Y",
			"Y moved from %s to %s", shorten(yActive.RevisionID), shorten(yAfter.RevisionID))
		return
	}

	Note(t, "S16_installation", "L25_concurrent_install", "§85 concurrent installations", "concurrent promotion stays per-installation",
		"X and Y promoted concurrently and both succeeded, each keeping its own revision; %d racers on X produced %d winner and %d STALE refusals; Y never moved",
		racers, winners, stale)
}

// sameReflogEntry compares two reflog rows, including their issue lists.
func sameReflogEntry(a, b projection.ReflogEntry) bool {
	if a.LogID != b.LogID || a.InstallationID != b.InstallationID || a.RefName != b.RefName ||
		a.OldRevisionID != b.OldRevisionID || a.NewRevisionID != b.NewRevisionID ||
		a.Reason != b.Reason || a.CandidateID != b.CandidateID ||
		a.LedgerSequence != b.LedgerSequence || !a.CreatedAt.Equal(b.CreatedAt) {
		return false
	}
	if len(a.IssueIDs) != len(b.IssueIDs) {
		return false
	}
	for i := range a.IssueIDs {
		if a.IssueIDs[i] != b.IssueIDs[i] {
			return false
		}
	}
	return true
}

// hashTree digests a directory tree deterministically.
func hashTree(t *testing.T, root string) string {
	t.Helper()
	var lines []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		lines = append(lines, fmt.Sprintf("%s:%s", relative, objectstore.HashOf(raw)))
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	sort.Strings(lines)
	return objectstore.HashOf([]byte(strings.Join(lines, "\n")))
}

// setProjectionSchemaVersion rewrites the stored schema version, simulating a
// projection written by an older binary.
func setProjectionSchemaVersion(t *testing.T, root, version string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(root, "db", "lymph.sqlite"))
	if err != nil {
		t.Fatalf("open projection: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE meta SET value = ? WHERE key = 'projection.schema_version'`, version); err != nil {
		t.Fatalf("set schema version: %v", err)
	}
}
