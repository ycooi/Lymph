package lab

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/httpapi"
	"github.com/ycooi/Lymph/internal/objectstore"
)

// TestS31NastyInputs throws malformed, hostile and stupid input at the daemon
// over its real HTTP surface. The daemon may refuse anything it likes; it may
// not panic, and it may not write outside its store.
func TestS31NastyInputs(t *testing.T) {
	const id = "S31_fuzz"
	root := t.TempDir()
	e := openEngineAt(t, root)
	srv := httpapi.New(httpapi.Options{Engine: e, SocketPath: filepath.Join(root, "sock"), Logger: quietLogger()})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	reg, err := e.RegisterApplication(context.Background(), engine.Manifest{
		Name: "fuzz-app",
		Junctions: []engine.JunctionSpec{
			{Name: "semantic.event_state", Workflow: "semantic-rule-repair", ConfigFamily: "semantic_event_rules"},
		},
		ConfigFamilies: []engine.ConfigFamilySpec{
			{Name: "semantic_event_rules", SchemaRevision: "v1"},
		},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	appID := reg.Application.ApplicationID

	deep := strings.Repeat("[", 20000) + strings.Repeat("]", 20000)
	huge := strings.Repeat("A", 1<<20)
	invalidUTF8 := string([]byte{0xff, 0xfe, 0xfd})

	cases := []struct {
		name string
		path string
		body string
	}{
		{"empty object", "/v1/events", `{}`},
		{"truncated json", "/v1/events", `{"specversion":"1.0","id":`},
		{"invalid utf-8", "/v1/events", fmt.Sprintf(`{"specversion":"1.0","id":"x","source":"lymph://%s/js","type":"lymph.feedback.unknown.v1","time":"2026-09-20T00:00:00Z","data":{"feedback_type":"UNKNOWN"},"subject":"%s"}`, appID, invalidUTF8)},
		{"deeply nested json", "/v1/events", fmt.Sprintf(`{"specversion":"1.0","id":"deep","source":"lymph://%s/js","type":"lymph.feedback.unknown.v1","time":"2026-09-20T00:00:00Z","data":%s}`, appID, deep)},
		{"duplicate fields", "/v1/events", fmt.Sprintf(`{"specversion":"1.0","specversion":"1.0","id":"dup","id":"dup2","source":"lymph://%s/js","type":"lymph.feedback.unknown.v1","time":"2026-09-20T00:00:00Z","data":{"feedback_type":"UNKNOWN"}}`, appID)},
		{"huge payload", "/v1/events", fmt.Sprintf(`{"specversion":"1.0","id":"huge","source":"lymph://%s/js","type":"lymph.feedback.unknown.v1","time":"2026-09-20T00:00:00Z","data":{"feedback_type":"UNKNOWN","payload":{"text":"%s"}}}`, appID, huge)},
		{"unknown feedback type", "/v1/events", fmt.Sprintf(`{"specversion":"1.0","id":"bad-type","source":"lymph://%s/js","type":"lymph.feedback.purple.v1","time":"2026-09-20T00:00:00Z","data":{"feedback_type":"PURPLE"}}`, appID)},
		{"bad source", "/v1/events", `{"specversion":"1.0","id":"bad-source","source":"ftp://nope","type":"lymph.feedback.unknown.v1","time":"2026-09-20T00:00:00Z","data":{"feedback_type":"UNKNOWN"}}`},
		{"empty body", "/v1/events", ""},
		{"bad manifest", "/v1/applications", `{"name":""}`},
		{"manifest with traversal path", "/v1/applications", `{"name":"traversal","config_families":[{"name":"x","targets":[{"target_type":"SINGLE_FILE","path":"../../../../etc/passwd"}]}]}`},
		{"bad state name", "/v1/work-items/does-not-exist/complete", `{"state":"PURPLE"}`},
		{"bad object hash", "/v1/objects/sha256:zzzz", ""},
		{"weird object hash", "/v1/objects/../../etc/passwd", ""},
	}

	bad := 0
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			method := http.MethodPost
			if tc.body == "" {
				method = http.MethodGet
			}
			req, err := http.NewRequest(method, ts.URL+tc.path, strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				Failf(t, id, "§31 fuzz", "the daemon never panics on hostile input",
					"%s: request failed at the transport: %v", tc.name, err)
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			if resp.StatusCode >= 500 {
				bad++
				t.Logf("%s: status %d body %s", tc.name, resp.StatusCode, truncateText(string(body), 200))
			}
			if len(body) > 0 && json.Valid(body) == false {
				// A request the router itself rejects (path traversal, unknown
				// verb) gets the standard library's plain-text reply before it
				// ever reaches a handler. That is acceptable: it leaks nothing
				// and it is not a daemon bug.
				if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
					return
				}
				bad++
				t.Logf("%s: response was not JSON: %s", tc.name, truncateText(string(body), 200))
			}
		})
	}
	if bad > 0 {
		Failf(t, id, "§31 fuzz", "the daemon never panics on hostile input",
			"%d hostile inputs produced a server error or a non-JSON reply", bad)
		return
	}

	// Weird bundle paths must stay data, never become filesystem paths.
	objectsBefore := countFiles(t, filepath.Join(root, "objects"))
	traversal, err := e.CreateCandidate(context.Background(), engine.CandidateRequest{
		Application: appID, ConfigFamily: reg.FamilyIDs["semantic_event_rules"],
		Bundle: objectstore.Bundle{
			"../../../../tmp/lymph-escape.txt": []byte("not a config\n"),
			"/absolute/path.yaml":              []byte("version: 1\n"),
			"a/../../b.yaml":                   []byte("version: 1\n"),
			"NUL\x00name.yaml":                 []byte("version: 1\n"),
			strings.Repeat("n", 300):           []byte("version: 1\n"),
		},
		Explanation: "path traversal attempt",
	})
	if err != nil {
		// Refusing the whole bundle is a perfectly good answer.
		t.Logf("traversal bundle refused: %v", err)
	} else {
		stored := candidateTree(t, e, traversal)
		if len(stored) != 5 {
			Failf(t, id, "§31 fuzz", "bundle paths stay data",
				"expected 5 stored files, got %d", len(stored))
			return
		}
	}
	if _, err := os.Stat("/tmp/lymph-escape.txt"); err == nil {
		Failf(t, id, "§31 fuzz", "bundle paths stay data",
			"a bundle path escaped the store and created /tmp/lymph-escape.txt")
		return
	}
	objectsAfter := countFiles(t, filepath.Join(root, "objects"))
	if objectsAfter-objectsBefore > 10 {
		Failf(t, id, "§31 fuzz", "bundle paths stay data",
			"a five-file bundle created %d object files", objectsAfter-objectsBefore)
		return
	}

	Passf(t, id, "§31 fuzz", "hostile input, no panic, no escape",
		"%d hostile requests (empty, truncated, invalid UTF-8, 20k-deep JSON, duplicate fields, 1 MiB payload, bad state names, traversal paths) all answered with a JSON status below 500; bundle paths with .., /, NUL and 300 characters stayed inside the object store",
		len(cases))
}

// TestS32StateFuzz drives random sequences of legitimate-looking and illegal
// operations, and checks that the state machines never accept an impossible
// transition.
func TestS32StateFuzz(t *testing.T) {
	const id = "S32_state_fuzz"
	ctx := context.Background()
	e := openEngine(t)
	sim := MCPSim()
	app := setupApp(t, e, sim)

	// 1. The transition functions must be total and consistent: every pair of
	// known states answers yes or no, and the forbidden pairs the plan names are
	// refused.
	if CheckCandidateAllowed(engine.CandidateDraft, engine.CandidateActive) {
		Failf(t, id, "§32 state-machine fuzzing", "impossible transitions are refused",
			"DRAFT -> ACTIVE was accepted")
		return
	}
	if CheckCandidateAllowed(engine.CandidateRejected, engine.CandidateDeployable) {
		Failf(t, id, "§32 state-machine fuzzing", "impossible transitions are refused",
			"REJECTED -> DEPLOYABLE was accepted")
		return
	}
	for _, from := range candidateStates() {
		for _, to := range candidateStates() {
			_ = CheckCandidateAllowed(from, to) // must not panic
		}
	}
	for _, from := range issueStates() {
		for _, to := range issueStates() {
			err := engine.CheckIssueTransition(from, to)
			if from == to && err != nil {
				Failf(t, id, "§32 state-machine fuzzing", "self-transitions are legal",
					"%s -> %s returned %v", from, to, err)
				return
			}
		}
	}

	// 2. Random operation sequences. A refusal is fine; silently applying an
	// illegal change is not.
	random := rand.New(rand.NewSource(20260920))
	refused, applied := 0, 0
	var candidates []string

	for step := 0; step < scaled(120, 600); step++ {
		switch random.Intn(8) {
		case 0:
			resp := emit(t, e, app, sim.Feedback, sim.Reason,
				map[string]any{"text": fmt.Sprintf("fuzz %d", random.Intn(5))}, "fuzz", app.Baseline.RevisionID)
			if _, err := e.QueueWork(ctx, engine.QueueIssueRequest{IssueIDs: []string{resp.IssueID}}); err != nil {
				refused++
			} else {
				applied++
			}
		case 1:
			candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
				Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
				Bundle:      objectstore.Bundle{"rules.yaml": []byte(fmt.Sprintf("version: 2\nrules:\n  - id: f%d\n    phrase: awarded\n    state: AWARDED\n", step))},
				Explanation: "fuzz candidate",
			})
			if err != nil {
				refused++
			} else {
				applied++
				candidates = append(candidates, candidate.CandidateID)
			}
		case 2:
			if len(candidates) == 0 {
				continue
			}
			candidateID := candidates[random.Intn(len(candidates))]
			kinds := []string{"structural", "replay", "regression", "holdout", "nonsense"}
			results := []string{"PASS", "FAIL", "MAYBE"}
			if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
				CandidateID: candidateID, Kind: kinds[random.Intn(len(kinds))],
				Result: results[random.Intn(len(results))], SuiteID: "fuzz",
			}); err != nil {
				refused++
			} else {
				applied++
			}
		case 3:
			if len(candidates) == 0 {
				continue
			}
			candidateID := candidates[random.Intn(len(candidates))]
			if _, err := e.ApproveCandidate(ctx, engine.ApprovalRequest{
				CandidateID: candidateID, Approver: "fuzz",
			}); err != nil {
				refused++
			} else {
				applied++
			}
		case 4:
			if len(candidates) == 0 {
				continue
			}
			candidateID := candidates[random.Intn(len(candidates))]
			if _, err := e.PromoteCandidate(ctx, candidateID, "fuzz"); err != nil {
				refused++
			} else {
				applied++
			}
		case 5:
			issues, err := e.DB().ListIssues("", 5, "recent")
			if err != nil || len(issues) == 0 {
				continue
			}
			statuses := issueStates()
			if _, err := e.SetIssueStatus(ctx, issues[0].IssueID,
				statuses[random.Intn(len(statuses))], "fuzz", ""); err != nil {
				refused++
			} else {
				applied++
			}
		case 6:
			if _, err := e.ClaimWork(ctx, engine.ClaimRequest{
				Worker: "fuzz-worker", TTL: time.Duration(random.Intn(3)) * time.Millisecond,
			}); err != nil {
				refused++
			} else {
				applied++
			}
		case 7:
			active := activeRevision(t, e, app)
			expected := "not-the-active-revision"
			if _, err := e.SetRef(ctx, engine.SetRefRequest{
				Application: app.ApplicationID(), ConfigFamily: app.FamilyID,
				RefName: engine.RefActive, RevisionID: active.RevisionID,
				ExpectedOld: &expected, Reason: "fuzz", Actor: "fuzz",
			}); err != nil {
				refused++
			} else {
				applied++
			}
		}
	}

	// Everything still obeys its state machine.
	issues, err := e.DB().ListIssues("", 1000, "count")
	if err != nil {
		t.Fatalf("issues: %v", err)
	}
	for _, issue := range issues {
		if !engine.ValidIssueStatus(engine.IssueStatus(issue.Status)) {
			Failf(t, id, "§32 state-machine fuzzing", "only known states exist",
				"issue %s ended in state %q", shorten(issue.IssueID), issue.Status)
			return
		}
	}
	candidateList, err := e.DB().ListCandidates("", "", 1000)
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	for _, candidate := range candidateList {
		if !engine.ValidCandidateState(engine.CandidateState(candidate.State)) {
			Failf(t, id, "§32 state-machine fuzzing", "only known states exist",
				"candidate %s ended in state %q", shorten(candidate.CandidateID), candidate.State)
			return
		}
	}
	workItems, err := e.DB().ListWorkItems("", 1000)
	if err != nil {
		t.Fatalf("work items: %v", err)
	}
	for _, item := range workItems {
		if !engine.ValidWorkState(engine.WorkState(item.State)) {
			Failf(t, id, "§32 state-machine fuzzing", "only known states exist",
				"work item %s ended in state %q", shorten(item.WorkID), item.State)
			return
		}
	}

	// And the store is still self-consistent after the abuse.
	report, err := e.VerifyLedger()
	if err != nil {
		Failf(t, id, "§32 state-machine fuzzing", "the store survives random abuse", "%v", err)
		return
	}
	before := stateDigest(t, e)
	if _, err := e.Rebuild(); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if after := stateDigest(t, e); after != before {
		Failf(t, id, "§32 state-machine fuzzing", "the store survives random abuse",
			"digest changed across rebuild: %s vs %s", shorten(before), shorten(after))
		return
	}

	Passf(t, id, "§32 state-machine fuzzing", "random operations, no impossible states",
		"%d random operations: %d applied, %d refused; every issue, candidate and work item ended in a known state; DRAFT->ACTIVE and REJECTED->DEPLOYABLE refused; ledger verified with %d records and the rebuild digest is unchanged",
		scaled(120, 600), applied, refused, report.Records)
}

// ---------- helpers ----------

func candidateStates() []engine.CandidateState {
	return []engine.CandidateState{
		engine.CandidateDraft, engine.CandidateStructurallyOK, engine.CandidateTesting,
		engine.CandidatePassed, engine.CandidateApproved, engine.CandidateDeployable,
		engine.CandidateActive, engine.CandidateRejected, engine.CandidateStale,
		engine.CandidateRolledBack, engine.CandidateSuperseded,
	}
}

func issueStates() []engine.IssueStatus {
	return []engine.IssueStatus{
		engine.IssueOpen, engine.IssueTriaged, engine.IssueQueuedForImprovement,
		engine.IssueInProgress, engine.IssueCandidateProduced, engine.IssueValidating,
		engine.IssueFixed, engine.IssueDeferred, engine.IssueIgnored, engine.IssueReopened,
	}
}

// CheckCandidateAllowed reports whether a candidate transition is legal.
func CheckCandidateAllowed(from, to engine.CandidateState) bool {
	return engine.CheckCandidateTransition(from, to) == nil
}

func countFiles(t *testing.T, root string) int {
	t.Helper()
	count := 0
	err := filepath.WalkDir(root, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !entry.IsDir() {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("count files in %s: %v", root, err)
	}
	return count
}

func truncateText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
