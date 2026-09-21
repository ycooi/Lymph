package httpapi_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/httpapi"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
	"github.com/ycooi/Lymph/pkg/client"
)

// The transport level of the human gate and the delivery channel
// (mission sections 22, 25 to 32, 61, 66).

// startDaemonWithACL runs a daemon whose local authorization policy is supplied
// by the test, so "who may approve" is decided by uid rather than assumed. The
// tests connect as the invoking user, so a policy that names that uid with a
// single role is how "a worker tries to approve" is expressed without faking
// peer credentials.
func startDaemonWithACL(t *testing.T, policy string) (root, socket string, stop func()) {
	t.Helper()
	root = t.TempDir()
	e, err := engine.Open(engine.Options{Root: root, Logger: quiet()})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	socket = filepath.Join(root, "lymph.sock")

	var acl *httpapi.ACL
	if policy != "" {
		path := filepath.Join(root, "acl.json")
		if err := os.WriteFile(path, []byte(policy), 0o600); err != nil {
			t.Fatalf("write policy: %v", err)
		}
		if acl, err = httpapi.LoadACL(path); err != nil {
			t.Fatalf("load policy: %v", err)
		}
	}
	srv := httpapi.New(httpapi.Options{Engine: e, SocketPath: socket, Logger: quiet(), ACL: acl})
	if err := srv.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve()
	}()
	waitForSocket(t, socket)
	return root, socket, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-done
		e.Close()
	}
}

// specActorHuman is how a human approval appears in the JSON the API returns.
const specActorHuman = `"actor_type": "HUMAN"`

func policyFor(roles ...string) string {
	quoted := make([]string, 0, len(roles))
	for _, role := range roles {
		quoted = append(quoted, fmt.Sprintf("%q", role))
	}
	return fmt.Sprintf(`{"principals":[{"uid":%d,"roles":[%s]}]}`,
		os.Getuid(), strings.Join(quoted, ","))
}

// seedCandidate registers an application with one managed family, imports a
// baseline, and leaves a PASSED candidate awaiting a decision.
func seedCandidate(t *testing.T, e *engine.Engine) (app, installation, candidateID string) {
	t.Helper()
	ctx := context.Background()
	reg, err := e.RegisterApplication(ctx, engine.Manifest{
		Name: "gateway",
		ConfigFamilies: []engine.ConfigFamilySpec{{
			Name:           "rules",
			ManagementMode: projection.ManagementLymphManaged,
			Targets:        []engine.TargetSpec{{Type: "SINGLE_FILE", Path: "/etc/gateway/rules.yaml"}},
		}},
		Junctions: []engine.JunctionSpec{{Name: "rules.match", ConfigFamily: "rules"}},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	app = reg.Application.ApplicationID
	installation = reg.DefaultInstallationID
	if _, err := e.ImportBaseline(ctx, engine.BaselineRequest{
		Application: app, ConfigFamily: "rules",
		Bundle: objectstore.Bundle{"rules.yaml": []byte("rules: []\n")},
	}); err != nil {
		t.Fatalf("baseline: %v", err)
	}
	candidate, err := e.CreateCandidate(ctx, engine.CandidateRequest{
		Application: app, ConfigFamily: "rules",
		Bundle:      objectstore.Bundle{"rules.yaml": []byte("rules:\n  - phrase: booked\n")},
		Explanation: "add the booked rule",
	})
	if err != nil {
		t.Fatalf("candidate: %v", err)
	}
	for _, kind := range []string{"structural", "replay", "regression"} {
		if _, err := e.RecordValidation(ctx, engine.ValidationRequest{
			CandidateID: candidate.CandidateID, Kind: kind, Result: "PASS", SuiteID: "gateway." + kind,
		}); err != nil {
			t.Fatalf("validation: %v", err)
		}
	}
	return app, installation, candidate.CandidateID
}

// TestWorkerCannotApprove is the hard negative the mission puts first: a local
// principal holding only the worker role cannot decide, and asserting
// "role":"OPERATOR" in the body changes nothing (mission sections 20, 22, 66).
func TestWorkerCannotApprove(t *testing.T) {
	root, socket, stop := startDaemonWithACL(t, policyFor("WORKER"))
	defer stop()
	_, _, candidateID := seedCandidate(t, engineAt(t, root))

	status, body := unixRequest(t, socket, http.MethodPost, "/v1/candidates/"+candidateID+"/approve",
		map[string]any{
			"approver":   "worker-1",
			"actor_type": "HUMAN",
			"role":       "OPERATOR",
		}, nil)
	if status != http.StatusForbidden {
		t.Fatalf("a WORKER principal approved a candidate: status %d %s", status, body)
	}
	if !strings.Contains(string(body), "FORBIDDEN") {
		t.Fatalf("refusal did not name the reason: %s", body)
	}

	// The candidate is untouched by the attempt.
	status, body = unixRequest(t, socket, http.MethodGet, "/v1/candidates/"+candidateID, nil, nil)
	if status != http.StatusOK {
		t.Fatalf("read candidate: %d %s", status, body)
	}
	if !strings.Contains(string(body), string(engine.CandidatePassed)) {
		t.Fatalf("the refused approval changed the candidate: %s", body)
	}
	if strings.Contains(string(body), "APPROVED") {
		t.Fatalf("the refused approval left a decision behind: %s", body)
	}
}

// TestApplicationPrincipalCannotApprove: the APPLICATION role may fetch and
// report, not decide.
func TestApplicationPrincipalCannotApprove(t *testing.T) {
	root, socket, stop := startDaemonWithACL(t, policyFor("APPLICATION"))
	defer stop()
	_, _, candidateID := seedCandidate(t, engineAt(t, root))

	status, body := unixRequest(t, socket, http.MethodPost, "/v1/candidates/"+candidateID+"/approve",
		map[string]any{"approver": "gateway"}, nil)
	if status != http.StatusForbidden {
		t.Fatalf("an APPLICATION principal approved a candidate: status %d %s", status, body)
	}
}

// TestOperatorPrincipalCanApprove is the positive control for the same path,
// and it checks that the daemon records what it saw rather than what it was
// told.
func TestOperatorPrincipalCanApprove(t *testing.T) {
	root, socket, stop := startDaemonWithACL(t, policyFor("OPERATOR"))
	defer stop()
	_, _, candidateID := seedCandidate(t, engineAt(t, root))

	status, body := unixRequest(t, socket, http.MethodPost, "/v1/candidates/"+candidateID+"/approve",
		map[string]any{"approver": "operator", "actor_type": "SYSTEM"}, nil)
	if status != http.StatusOK {
		t.Fatalf("operator approval failed: %d %s", status, body)
	}

	// The daemon decided the actor type from the peer's uid and recorded it,
	// ignoring the "actor_type":"SYSTEM" the request tried to supply.
	status, body = unixRequest(t, socket, http.MethodGet, "/v1/candidates/"+candidateID, nil, nil)
	if status != http.StatusOK {
		t.Fatalf("read candidate: %d %s", status, body)
	}
	if !strings.Contains(string(body), specActorHuman) {
		t.Fatalf("the approval was not recorded as human: %s", body)
	}
	if !strings.Contains(string(body), fmt.Sprintf(`"peer_uid": %d`, os.Getuid())) {
		t.Fatalf("the approval did not record the peer uid: %s", body)
	}
}

// TestUnknownPrincipalGetsNoPrivilege: an unrecognised local user holds nothing.
func TestUnknownPrincipalGetsNoPrivilege(t *testing.T) {
	policy := `{"principals":[{"uid":424242,"roles":["OPERATOR"]}],"default_roles":[]}`
	root, socket, stop := startDaemonWithACL(t, policy)
	defer stop()
	_, _, candidateID := seedCandidate(t, engineAt(t, root))

	status, body := unixRequest(t, socket, http.MethodPost, "/v1/candidates/"+candidateID+"/approve",
		map[string]any{"approver": "nobody"}, nil)
	if status != http.StatusForbidden {
		t.Fatalf("an unrecognised local user could approve: %d %s", status, body)
	}
}

// TestWhoamiReportsTheGate: "is the human gate actually on?" is a query.
func TestWhoamiReportsTheGate(t *testing.T) {
	_, socket, stop := startDaemonWithACL(t, policyFor("WORKER"))
	defer stop()
	status, body := unixRequest(t, socket, http.MethodGet, "/v1/whoami", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("whoami: %d %s", status, body)
	}
	text := string(body)
	if !strings.Contains(text, `"may_approve": false`) {
		t.Fatalf("whoami did not report the gate as closed for a worker: %s", text)
	}
	if !strings.Contains(text, "ACL") {
		t.Fatalf("whoami did not report the authorization mode: %s", text)
	}
}

func TestWorkerCannotUseOperatorControlPlane(t *testing.T) {
	_, socket, stop := startDaemonWithACL(t, policyFor("WORKER"))
	defer stop()

	for _, tc := range []struct {
		method string
		path   string
		body   any
	}{
		{http.MethodPost, "/v1/applications", map[string]any{"name": "forbidden"}},
		{http.MethodPost, "/v1/projection/rebuild", map[string]any{}},
		{http.MethodGet, "/v1/audit", nil},
	} {
		status, body := unixRequest(t, socket, tc.method, tc.path, tc.body, nil)
		if status != http.StatusForbidden {
			t.Fatalf("worker reached operator route %s %s: %d %s", tc.method, tc.path, status, body)
		}
	}
}

// TestSessionlessUpdateReadIsRejected (mission section 61).
func TestSessionlessUpdateReadIsRejected(t *testing.T) {
	root, socket, stop := startDaemon(t)
	defer stop()
	seedCandidate(t, engineAt(t, root))

	status, body := unixRequest(t, socket, http.MethodGet, "/v1/updates", nil, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("an update list was served without a session: %d %s", status, body)
	}
	if !strings.Contains(string(body), "SESSION_REQUIRED") {
		t.Fatalf("refusal did not say why: %s", body)
	}

	// An unknown session is refused too, rather than falling back to anything.
	status, body = unixRequest(t, socket, http.MethodGet, "/v1/updates", nil,
		map[string]string{protocol.HeaderSession: "019a0000-0000-7000-8000-000000000000"})
	if status != http.StatusUnauthorized {
		t.Fatalf("an unknown session was accepted: %d %s", status, body)
	}
}

// TestApprovedUpdateFlowsThroughTheProtocol runs the whole pull loop over the
// real socket: operator approves, application discovers, fetches, verifies,
// applies and reports (mission sections 25 to 32, 50, 67).
func TestApprovedUpdateFlowsThroughTheProtocol(t *testing.T) {
	root, socket, stop := startDaemon(t)
	defer stop()
	store := engineAt(t, root)
	app, installation, candidateID := seedCandidate(t, store)
	ctx := context.Background()

	// 1. The operator approves the exact content and promotes it.
	admin := client.New(client.Options{Socket: socket, Timeout: 5e9})
	var candidate projection.Candidate
	if err := admin.Post(ctx, "/v1/candidates/"+candidateID+"/approve",
		map[string]any{"approver": "operator", "installation_id": installation}, &candidate); err != nil {
		t.Fatalf("approve: %v", err)
	}
	var promotion engine.PromotionResult
	if err := admin.Post(ctx, "/v1/candidates/"+candidateID+"/promote",
		map[string]any{"actor": "operator"}, &promotion); err != nil {
		t.Fatalf("promote: %v", err)
	}

	// 2. The application discovers it through its own session.
	appClient, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket: socket, ApplicationID: app, InstallationID: installation,
		ClientName: "gateway", ClientVersion: "test",
	})
	if err != nil {
		t.Fatalf("application client: %v", err)
	}
	if _, err := appClient.Hello(ctx); err != nil {
		t.Fatalf("hello: %v", err)
	}
	updates, err := appClient.CheckUpdates(ctx)
	if err != nil {
		t.Fatalf("check updates: %v", err)
	}
	if len(updates) != 1 {
		t.Fatalf("expected one approved update, got %d", len(updates))
	}
	offered := updates[0]

	// 3. It fetches the bytes and verifies them itself.
	content, err := appClient.FetchUpdate(ctx, offered.ConfigFamilyID, offered.RevisionID)
	if err != nil {
		t.Fatalf("fetch update: %v", err)
	}
	if content.RootTreeHash != offered.ContentHash {
		t.Fatalf("fetched %s, approval covers %s", content.RootTreeHash, offered.ContentHash)
	}
	if _, ok := content.Bundle["rules.yaml"]; !ok {
		t.Fatalf("bundle does not carry the expected file: %v", content.Manifest)
	}

	// 4. It reports the outcome, and only then does observed move.
	result, err := appClient.ReportApplied(ctx, offered.RevisionID, client.AppliedReport{
		ConfigFamilyID: offered.ConfigFamilyID,
		ObservedHash:   content.RootTreeHash,
		Details:        map[string]any{"reload": "SIGHUP", "took_ms": 12},
	})
	if err != nil {
		t.Fatalf("report applied: %v", err)
	}
	if result.Result != projection.ResultApplied {
		t.Fatalf("result %s", result.Result)
	}
	observed := observedRef(t, socket, admin, app, offered.ConfigFamilyID, installation)
	if observed != promotion.Revision.RevisionID {
		t.Fatalf("observed is %q, expected %s", observed, promotion.Revision.RevisionID)
	}

	// 5. Reporting the same result again stays one logical result.
	again, err := appClient.ReportApplied(ctx, offered.RevisionID, client.AppliedReport{
		ConfigFamilyID: offered.ConfigFamilyID,
		ObservedHash:   content.RootTreeHash,
	})
	if err != nil {
		t.Fatalf("duplicate report: %v", err)
	}
	if again.ResultID != result.ResultID {
		t.Fatal("a duplicate report over the wire created a second canonical result")
	}
}

// TestRejectedUpdateFlowsThroughTheProtocol: the node refuses, and the refusal
// comes back as feedback rather than as a governance change.
func TestRejectedUpdateFlowsThroughTheProtocol(t *testing.T) {
	root, socket, stop := startDaemon(t)
	defer stop()
	store := engineAt(t, root)
	app, installation, candidateID := seedCandidate(t, store)
	ctx := context.Background()

	admin := client.New(client.Options{Socket: socket, Timeout: 5e9})
	var candidate projection.Candidate
	if err := admin.Post(ctx, "/v1/candidates/"+candidateID+"/approve",
		map[string]any{"approver": "operator", "installation_id": installation}, &candidate); err != nil {
		t.Fatalf("approve: %v", err)
	}
	var promotion engine.PromotionResult
	if err := admin.Post(ctx, "/v1/candidates/"+candidateID+"/promote",
		map[string]any{"actor": "operator"}, &promotion); err != nil {
		t.Fatalf("promote: %v", err)
	}

	appClient, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket: socket, ApplicationID: app, InstallationID: installation,
	})
	if err != nil {
		t.Fatalf("application client: %v", err)
	}
	if _, err := appClient.Hello(ctx); err != nil {
		t.Fatalf("hello: %v", err)
	}
	result, err := appClient.ReportRejected(ctx, promotion.Revision.RevisionID, client.RejectedReport{
		ConfigFamilyID: promotion.Revision.ConfigFamilyID,
		ReasonCode:     "LOCAL_VALIDATION_FAILED",
		Message:        "the node's schema check refused the bundle",
	})
	if err != nil {
		t.Fatalf("report rejected: %v", err)
	}
	if result.Result != projection.ResultRejected {
		t.Fatalf("result %s", result.Result)
	}

	// Not offered again, and the refs did not move.
	updates, err := appClient.CheckUpdates(ctx)
	if err != nil {
		t.Fatalf("check updates: %v", err)
	}
	if len(updates) != 0 {
		t.Fatalf("a rejected revision is still offered: %+v", updates)
	}
	if observed := observedRef(t, socket, admin, app, promotion.Revision.ConfigFamilyID, installation); observed != "" {
		t.Fatalf("a rejection set observed to %s", observed)
	}
}

// TestTamperedBundleIsNeverAccepted: a corrupted store must not reach a node.
//
// Two independent checks stand between the node and bad bytes: the daemon
// re-hashes every blob it serves, and the SDK re-hashes everything it receives.
// This test corrupts the store on purpose, which means the daemon's check fires
// first — that is the answer we want, and the SDK's own check is exercised
// directly in pkg/client (mission section 29).
func TestTamperedBundleIsNeverAccepted(t *testing.T) {
	root, socket, stop := startDaemon(t)
	defer stop()
	store := engineAt(t, root)
	app, installation, candidateID := seedCandidate(t, store)
	ctx := context.Background()

	admin := client.New(client.Options{Socket: socket, Timeout: 5e9})
	var candidate projection.Candidate
	if err := admin.Post(ctx, "/v1/candidates/"+candidateID+"/approve",
		map[string]any{"approver": "operator", "installation_id": installation}, &candidate); err != nil {
		t.Fatalf("approve: %v", err)
	}
	var promotion engine.PromotionResult
	if err := admin.Post(ctx, "/v1/candidates/"+candidateID+"/promote",
		map[string]any{"actor": "operator"}, &promotion); err != nil {
		t.Fatalf("promote: %v", err)
	}

	// Corrupt the stored blob behind the revision, as a damaged disk would.
	tree, err := store.Objects().GetTree(promotion.Revision.RootTreeHash)
	if err != nil {
		t.Fatalf("tree: %v", err)
	}
	objectPath, err := store.Objects().Path(tree.Entries[0].Hash)
	if err != nil {
		t.Fatalf("object path: %v", err)
	}
	if err := os.WriteFile(objectPath, []byte("rules:\n  - phrase: tampered\n"), 0o644); err != nil {
		t.Fatalf("corrupt the object: %v", err)
	}

	appClient, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket: socket, ApplicationID: app, InstallationID: installation,
	})
	if err != nil {
		t.Fatalf("application client: %v", err)
	}
	if _, err := appClient.Hello(ctx); err != nil {
		t.Fatalf("hello: %v", err)
	}
	if _, err := appClient.FetchUpdate(ctx, promotion.Revision.ConfigFamilyID,
		promotion.Revision.RevisionID); err == nil {
		t.Fatal("a tampered bundle reached the node")
	} else if !strings.Contains(err.Error(), "corrupt") && !strings.Contains(err.Error(), "INTEGRITY_ERROR") {
		t.Fatalf("expected an integrity refusal, got %v", err)
	}
}

// observedRef reads the ref a node has confirmed it is running, through the API
// rather than through a second engine handle.
func observedRef(t *testing.T, socket string, admin *client.Client, app, family, installation string) string {
	t.Helper()
	var out struct {
		Refs []projection.ConfigRef `json:"refs"`
	}
	path := "/v1/refs" + client.Query("application", app, "config_family", family,
		"installation", installation)
	if err := admin.Get(context.Background(), path, &out); err != nil {
		t.Fatalf("list refs: %v", err)
	}
	for _, ref := range out.Refs {
		if ref.Name == projection.RefObserved {
			return ref.RevisionID
		}
	}
	return ""
}

// TestWithdrawOverTheProtocol: withdrawal is an operator action and a worker
// cannot perform it either.
func TestWithdrawOverTheProtocol(t *testing.T) {
	root, socket, stop := startDaemonWithACL(t, policyFor("WORKER"))
	defer stop()
	store := engineAt(t, root)
	_, installation, candidateID := seedCandidate(t, store)

	ctx := context.Background()
	// The test process is a WORKER here, so it cannot approve; seed an approval
	// and a revision through the engine directly, then confirm the endpoint
	// still refuses the withdrawal.
	if _, err := store.ApproveCandidate(ctx, engine.ApprovalRequest{
		CandidateID: candidateID, Approver: "operator", InstallationID: installation,
	}); err != nil {
		t.Fatalf("approve: %v", err)
	}
	promotion, err := store.PromoteCandidate(ctx, candidateID, "operator")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	status, body := unixRequest(t, socket, http.MethodPost,
		"/v1/updates/"+promotion.Revision.RevisionID+"/withdraw",
		map[string]any{"config_family_id": promotion.Revision.ConfigFamilyID},
		map[string]string{"X-Lymph-Session": ""})
	if status != http.StatusForbidden {
		t.Fatalf("a WORKER principal withdrew an update: %d %s", status, body)
	}
}
