package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/ledger"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
	"github.com/ycooi/Lymph/pkg/client"
)

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: lymphctl %s [flags]\n", name)
		fs.PrintDefaults()
	}
	return fs
}

// parseWithPositionals parses flags that may appear before or after positional
// arguments, returning the positionals. Go's flag package stops at the first
// non-flag token, which would silently swallow `lymphctl complete WORK --worker
// alice`, so the arguments are reordered before parsing.
func parseWithPositionals(fs *flag.FlagSet, args []string) []string {
	var positional, flags []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			positional = append(positional, arg)
			continue
		}
		name := strings.TrimLeft(arg, "-")
		if strings.Contains(name, "=") {
			flags = append(flags, arg)
			continue
		}
		f := fs.Lookup(name)
		if f == nil || isBoolFlag(f) {
			flags = append(flags, arg)
			continue
		}
		if i+1 >= len(args) {
			fail(fmt.Errorf("%s needs a value", arg))
		}
		flags = append(flags, arg, args[i+1])
		i++
	}
	if err := fs.Parse(flags); err != nil {
		fail(err)
	}
	return positional
}

type boolFlagger interface{ IsBoolFlag() bool }

func isBoolFlag(f *flag.Flag) bool {
	bf, ok := f.Value.(boolFlagger)
	return ok && bf.IsBoolFlag()
}

// ---------- init ----------

// cmdInit initialises a lymph store on disk without starting the daemon
// (section 5). It is safe to run repeatedly; the instance identity is created
// once and never regenerated.
func (c *ctl) cmdInit(args []string) {
	fs := newFlagSet("init")
	force := fs.Bool("force", false, "create the store even if the socket already answers")
	_ = fs.Parse(args)

	root := c.root
	if _, _, err := identity.Ensure(root); err != nil {
		if errors.Is(err, identity.ErrMissingInstanceIdentity) {
			fail(fmt.Errorf("%w\nhint: restore identity.json from backup, or point --root at the right store", err))
		}
		fail(err)
	}
	ident, err := identity.Load(root)
	if err != nil {
		fail(err)
	}
	if !*force {
		probe := client.New(client.Options{Socket: c.socket, Timeout: time.Second})
		if _, err := probe.Health(context.Background()); err == nil {
			fmt.Printf("store already served by a running lymphd at %s\n", c.socket)
		} else {
			fmt.Printf("store initialised (daemon not running)\n")
		}
	}
	c.print(map[string]any{
		"root":          root,
		"instance_uuid": ident.InstanceUUID,
		"created_at":    ident.CreatedAt,
		"format":        ident.FormatVersion,
		"socket":        c.socket,
	})
}

// ---------- inspection ----------

func (c *ctl) cmdStatus(args []string) {
	_ = newFlagSet("status").Parse(args)
	var status engine.Status
	if err := c.client.Get(context.Background(), "/v1/status", &status); err != nil {
		fail(err)
	}
	c.print(status)
}

func (c *ctl) cmdHealth(args []string) {
	_ = newFlagSet("health").Parse(args)
	out, err := c.client.Health(context.Background())
	if err != nil {
		fail(err)
	}
	c.print(out)
}

func (c *ctl) cmdApps(args []string) {
	_ = newFlagSet("apps").Parse(args)
	var out struct {
		Applications []projection.Application `json:"applications"`
	}
	if err := c.client.Get(context.Background(), "/v1/applications", &out); err != nil {
		fail(err)
	}
	c.print(out.Applications)
}

func (c *ctl) cmdJunctions(args []string) {
	fs := newFlagSet("junctions")
	app := fs.String("application", "", "filter by application")
	_ = fs.Parse(args)
	var out struct {
		Junctions []projection.Junction `json:"junctions"`
	}
	path := "/v1/junctions" + client.Query("application", *app)
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Junctions)
}

func (c *ctl) cmdFamilies(args []string) {
	fs := newFlagSet("families")
	app := fs.String("application", "", "filter by application")
	_ = fs.Parse(args)
	var out struct {
		Families []projection.ConfigFamily `json:"config_families"`
	}
	path := "/v1/config-families" + client.Query("application", *app)
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Families)
}

func (c *ctl) cmdTargets(args []string) {
	fs := newFlagSet("targets")
	family := fs.String("family", "", "filter by config family")
	_ = fs.Parse(args)
	var out struct {
		Targets []projection.ManagedTarget `json:"targets"`
	}
	path := "/v1/targets" + client.Query("config_family", *family)
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Targets)
}

func (c *ctl) cmdEvents(args []string) {
	fs := newFlagSet("events")
	app := fs.String("application", "", "filter by application")
	limit := fs.Int("limit", 25, "maximum events")
	_ = fs.Parse(args)
	var out struct {
		Events []projection.Event `json:"events"`
	}
	path := "/v1/events" + client.Query("application", *app, "limit", fmt.Sprint(*limit))
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Events)
}

func (c *ctl) cmdIssues(args []string) {
	fs := newFlagSet("issues")
	status := fs.String("status", "", "filter by status (OPEN, TRIAGED, ...)")
	order := fs.String("order", "count", "count or recent")
	limit := fs.Int("limit", 25, "maximum issues")
	_ = fs.Parse(args)
	var out struct {
		Issues []projection.Issue `json:"issues"`
	}
	path := "/v1/issues" + client.Query("status", strings.ToUpper(*status), "order", *order, "limit", fmt.Sprint(*limit))
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Issues)
}

func (c *ctl) cmdIssueStatus(args []string) {
	fs := newFlagSet("issue-status")
	status := fs.String("status", "", "target status")
	actor := fs.String("actor", os.Getenv("USER"), "who is making the change")
	comment := fs.String("comment", "", "free-text note")
	rest := parseWithPositionals(fs, args)
	if len(rest) != 1 || *status == "" {
		fail(errors.New("usage: lymphctl issue-status ISSUE_ID --status STATUS [--actor NAME] [--comment TEXT]"))
	}
	var issue projection.Issue
	body := map[string]string{"status": *status, "actor": *actor, "comment": *comment}
	if err := c.client.Post(context.Background(), "/v1/issues/"+rest[0]+"/status", body, &issue); err != nil {
		fail(err)
	}
	c.print(issue)
}

func (c *ctl) cmdWork(args []string) {
	fs := newFlagSet("work")
	state := fs.String("state", "", "filter by state (QUEUED, LEASED, ...)")
	limit := fs.Int("limit", 25, "maximum items")
	_ = fs.Parse(args)
	var out struct {
		Items []projection.WorkItem `json:"work_items"`
	}
	path := "/v1/work-items" + client.Query("state", strings.ToUpper(*state), "limit", fmt.Sprint(*limit))
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Items)
}

// ---------- feedback ----------

func (c *ctl) cmdEmit(args []string) {
	fs := newFlagSet("emit")
	app := fs.String("app", "", "application UUID or name")
	junction := fs.String("junction", "", "junction UUID or name")
	kind := fs.String("type", "", "feedback type, e.g. UNKNOWN")
	reason := fs.String("reason", "", "reason code, e.g. UNKNOWN_EVENT_PHRASE")
	payload := fs.String("payload", "", "JSON payload")
	inputRef := fs.String("input-ref", "", "reference to the input that caused this")
	replayRef := fs.String("replay-ref", "", "reference to a replayable artefact")
	configRev := fs.String("config-revision", "", "config revision in production at the time")
	eventID := fs.String("id", "", "explicit event id (default: a fresh UUIDv7)")
	producer := fs.String("producer", "", "producer instance id")
	durability := fs.String("durability", string(ledger.Async), "ASYNC or DURABLE")
	spool := fs.String("spool", "", "spool directory for undeliverable events")
	_ = fs.Parse(args)

	if *app == "" || *junction == "" || *kind == "" {
		fail(errors.New("emit requires --app, --junction and --type"))
	}
	feedback := protocol.FeedbackType(strings.ToUpper(*kind))
	if !feedback.Valid() {
		fail(fmt.Errorf("unknown feedback type %q; valid types: %s", *kind, strings.Join(feedbackTypeNames(), ", ")))
	}
	var raw json.RawMessage
	if *payload != "" {
		if !json.Valid([]byte(*payload)) {
			fail(fmt.Errorf("--payload is not valid JSON: %s", *payload))
		}
		raw = json.RawMessage(*payload)
	}

	event, err := engine.NewEvent(
		protocol.EventSource(*app, *junction),
		protocol.FeedbackEventType(feedback),
		*junction,
		protocol.EventData{
			FeedbackType:   feedback,
			ReasonCode:     *reason,
			InputRef:       *inputRef,
			ReplayRef:      *replayRef,
			ConfigRevision: *configRev,
			Payload:        raw,
		},
	)
	if err != nil {
		fail(err)
	}
	if *eventID != "" {
		event.ID = *eventID
	}

	cl := c.client
	if *spool != "" {
		cl = client.New(client.Options{
			Socket: c.socket, SpoolDir: *spool, ProducerInstance: "lymphctl",
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := cl.Emit(ctx, client.EmitRequest{
		Event:            event,
		ProducerInstance: *producer,
		Durability:       *durability,
	})
	if err != nil {
		fail(err)
	}
	if resp.Spooled && !c.jsonOut {
		fmt.Fprintf(os.Stderr, "lymph unreachable: event %s spooled locally\n", resp.EventID)
	}
	c.print(resp)
}

func feedbackTypeNames() []string {
	types := protocol.FeedbackTypes()
	out := make([]string, len(types))
	for i, t := range types {
		out[i] = string(t)
	}
	return out
}

// ---------- registration ----------

func (c *ctl) cmdRegisterApp(args []string) {
	fs := newFlagSet("register-app")
	file := fs.String("file", "", "manifest JSON file (- for stdin)")
	_ = fs.Parse(args)
	if *file == "" {
		fail(errors.New("register-app requires --file"))
	}
	raw, err := readInput(*file)
	if err != nil {
		fail(err)
	}
	var manifest engine.Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		fail(fmt.Errorf("parse manifest: %w", err))
	}
	var out engine.Registration
	if err := c.client.Post(context.Background(), "/v1/applications", manifest, &out); err != nil {
		fail(err)
	}
	c.print(out)
}

// ---------- configuration ----------

func (c *ctl) cmdBaseline(args []string) {
	fs := newFlagSet("baseline")
	app := fs.String("app", "", "application UUID or name")
	family := fs.String("family", "", "config family UUID or name")
	installation := fs.String("installation", "", "installation UUID or name (default: the application's default)")
	file := fs.String("file", "", "single file to import")
	dir := fs.String("dir", "", "directory whose files form the bundle")
	author := fs.String("author", os.Getenv("USER"), "who imported it")
	label := fs.String("label", "", "human label, e.g. 2.3.1")
	_ = fs.Parse(args)

	bundle, err := loadBundle(*file, *dir)
	if err != nil {
		fail(err)
	}
	var rev projection.ConfigRevision
	if err := c.client.Post(context.Background(), "/v1/baselines", engine.BaselineRequest{
		Application:    *app,
		ConfigFamily:   *family,
		InstallationID: *installation,
		Bundle:         bundle,
		Author:         *author,
		Label:          *label,
	}, &rev); err != nil {
		fail(err)
	}
	c.print(rev)
}

func (c *ctl) cmdRevision(args []string) {
	fs := newFlagSet("revision")
	app := fs.String("app", "", "application UUID or name")
	family := fs.String("family", "", "config family UUID or name")
	file := fs.String("file", "", "single file to store")
	dir := fs.String("dir", "", "directory whose files form the bundle")
	message := fs.String("message", "", "why this revision exists")
	author := fs.String("author", os.Getenv("USER"), "who created it")
	label := fs.String("label", "", "human label")
	setRefs := fs.String("set-ref", "", "comma-separated refs to move (active, approved, desired, deployed, last_good)")
	expect := fs.String("expect", "", "expected current revision of every ref being moved (empty means create-only)")
	issueIDs := fs.String("issues", "", "comma-separated issue ids this revision addresses")
	_ = fs.Parse(args)

	bundle, err := loadBundle(*file, *dir)
	if err != nil {
		fail(err)
	}
	req := engine.RevisionRequest{
		Application:  *app,
		ConfigFamily: *family,
		Bundle:       bundle,
		Message:      *message,
		Author:       *author,
		Label:        *label,
		IssueIDs:     splitList(*issueIDs),
	}
	if *setRefs != "" {
		req.SetRefs = map[string]bool{}
		req.ExpectedRefs = map[string]*string{}
		expected := *expect
		for _, name := range splitList(*setRefs) {
			req.SetRefs[name] = true
			value := expected
			req.ExpectedRefs[name] = &value
		}
	}
	var rev projection.ConfigRevision
	if err := c.client.Post(context.Background(), "/v1/revisions", req, &rev); err != nil {
		fail(err)
	}
	c.print(rev)
}

func (c *ctl) cmdRevisions(args []string) {
	fs := newFlagSet("revisions")
	app := fs.String("app", "", "application UUID or name")
	family := fs.String("family", "", "config family UUID or name")
	limit := fs.Int("limit", 25, "maximum revisions")
	_ = fs.Parse(args)
	var out struct {
		Revisions []projection.ConfigRevision `json:"revisions"`
	}
	path := "/v1/revisions" + client.Query("application", *app, "config_family", *family, "limit", fmt.Sprint(*limit))
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Revisions)
}

func (c *ctl) cmdCandidate(args []string) {
	fs := newFlagSet("candidate")
	app := fs.String("app", "", "application UUID or name")
	family := fs.String("family", "", "config family UUID or name")
	installation := fs.String("installation", "", "installation UUID or name (default: the item's, or the application's)")
	file := fs.String("file", "", "single file in the proposed bundle")
	dir := fs.String("dir", "", "directory whose files form the proposed bundle")
	explanation := fs.String("explanation", "", "what the candidate changes and why")
	base := fs.String("base", "", "base revision (default: current active)")
	work := fs.String("work-item", "", "work item that produced it")
	issues := fs.String("issues", "", "comma-separated issue ids this candidate addresses")
	_ = fs.Parse(args)

	bundle, err := loadBundle(*file, *dir)
	if err != nil {
		fail(err)
	}
	var cand projection.Candidate
	if err := c.client.Post(context.Background(), "/v1/candidates", engine.CandidateRequest{
		Application:    *app,
		ConfigFamily:   *family,
		InstallationID: *installation,
		Bundle:         bundle,
		BaseRevisionID: *base,
		WorkItemID:     *work,
		IssueIDs:       splitList(*issues),
		Explanation:    *explanation,
	}, &cand); err != nil {
		fail(err)
	}
	c.print(cand)
}

func (c *ctl) cmdCandidates(args []string) {
	fs := newFlagSet("candidates")
	app := fs.String("app", "", "application UUID or name")
	family := fs.String("family", "", "config family UUID or name")
	state := fs.String("state", "", "filter by candidate state")
	limit := fs.Int("limit", 25, "maximum candidates")
	_ = fs.Parse(args)
	var out struct {
		Candidates []projection.Candidate `json:"candidates"`
	}
	path := "/v1/candidates" + client.Query("application", *app, "config_family", *family,
		"state", strings.ToUpper(*state), "limit", fmt.Sprint(*limit))
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Candidates)
}

// cmdLineage walks a revision's ancestry. The test plan's question is "why is
// r43 running?", and the answer has to be reconstructible years later.
func (c *ctl) cmdLineage(args []string) {
	fs := newFlagSet("lineage")
	limit := fs.Int("limit", 100, "maximum ancestors")
	rest := parseWithPositionals(fs, args)
	if len(rest) != 1 {
		fail(errors.New("usage: lymphctl lineage REVISION_ID [--limit N]"))
	}
	var out struct {
		Lineage []projection.ConfigRevision `json:"lineage"`
	}
	path := "/v1/revisions/" + rest[0] + "/lineage" + client.Query("limit", fmt.Sprint(*limit))
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Lineage)
}

// cmdDiff prints the content difference between two revisions.
func (c *ctl) cmdDiff(args []string) {
	fs := newFlagSet("diff")
	rest := parseWithPositionals(fs, args)
	if len(rest) != 2 {
		fail(errors.New("usage: lymphctl diff REVISION_A REVISION_B"))
	}
	var out engine.DiffResult
	path := "/v1/diff" + client.Query("from", rest[0], "to", rest[1])
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out)
}

func (c *ctl) cmdValidate(args []string) {
	fs := newFlagSet("validate")
	candidate := fs.String("candidate", "", "candidate id")
	kind := fs.String("kind", "structural", "structural, schema, replay, regression or holdout")
	result := fs.String("result", "PASS", "PASS or FAIL")
	suite := fs.String("suite", "", "test suite identity")
	worker := fs.String("worker", os.Getenv("USER"), "who ran it")
	inputs := fs.String("inputs-hash", "", "hash of the test inputs")
	_ = fs.Parse(args)
	if *candidate == "" {
		fail(errors.New("validate requires --candidate"))
	}
	var cand projection.Candidate
	if err := c.client.Post(context.Background(), "/v1/candidates/"+*candidate+"/validations", engine.ValidationRequest{
		Kind:       *kind,
		Result:     strings.ToUpper(*result),
		SuiteID:    *suite,
		Worker:     *worker,
		InputsHash: *inputs,
	}, &cand); err != nil {
		fail(err)
	}
	c.print(cand)
}

func (c *ctl) cmdPromote(args []string) {
	fs := newFlagSet("promote")
	candidate := fs.String("candidate", "", "candidate id")
	actor := fs.String("actor", os.Getenv("USER"), "who promotes")
	rest := parseWithPositionals(fs, args)
	candidateID := *candidate
	if candidateID == "" && len(rest) == 1 {
		candidateID = rest[0]
	}
	if candidateID == "" {
		fail(errors.New("usage: lymphctl promote CANDIDATE_ID [--actor NAME]"))
	}
	var out engine.PromotionResult
	if err := c.client.Post(context.Background(), "/v1/candidates/"+candidateID+"/promote", map[string]string{"actor": *actor}, &out); err != nil {
		fail(err)
	}
	c.print(out)
}

func (c *ctl) cmdRefs(args []string) {
	fs := newFlagSet("refs")
	app := fs.String("app", "", "application UUID or name")
	family := fs.String("family", "", "config family UUID or name")
	installation := fs.String("installation", "", "installation UUID or name (default: all)")
	_ = fs.Parse(args)
	var out struct {
		Refs []projection.ConfigRef `json:"refs"`
	}
	path := "/v1/refs" + client.Query("application", *app, "config_family", *family, "installation", *installation)
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Refs)
}

func (c *ctl) cmdRef(args []string) {
	if len(args) == 0 || args[0] != "set" {
		fail(errors.New("usage: lymphctl ref set --app A --family F --name REF --revision ID [--expect ID]"))
	}
	fs := newFlagSet("ref set")
	app := fs.String("app", "", "application UUID or name")
	family := fs.String("family", "", "config family UUID or name")
	installation := fs.String("installation", "", "installation UUID or name (default: the application's)")
	name := fs.String("name", "", "ref name, e.g. active")
	revision := fs.String("revision", "", "target revision id")
	expect := fs.String("expect", "", "expected current revision id")
	reason := fs.String("reason", "", "why the ref moved")
	actor := fs.String("actor", os.Getenv("USER"), "who moved it")
	_ = fs.Parse(args[1:])

	req := engine.SetRefRequest{
		Application:    *app,
		ConfigFamily:   *family,
		InstallationID: *installation,
		RefName:        *name,
		RevisionID:     *revision,
		Reason:         *reason,
		Actor:          *actor,
	}
	if *expect != "" {
		req.ExpectedOld = expect
	}
	var ref projection.ConfigRef
	if err := c.client.Post(context.Background(), "/v1/refs", req, &ref); err != nil {
		fail(err)
	}
	c.print(ref)
}

func (c *ctl) cmdReflog(args []string) {
	fs := newFlagSet("reflog")
	app := fs.String("app", "", "application UUID or name")
	family := fs.String("family", "", "config family UUID or name")
	installation := fs.String("installation", "", "installation UUID or name (default: all)")
	limit := fs.Int("limit", 25, "maximum entries")
	_ = fs.Parse(args)
	var out struct {
		Entries []projection.ReflogEntry `json:"reflog"`
	}
	path := "/v1/reflog" + client.Query("application", *app, "config_family", *family,
		"installation", *installation, "limit", fmt.Sprint(*limit))
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Entries)
}

func (c *ctl) cmdAudit(args []string) {
	fs := newFlagSet("audit")
	limit := fs.Int("limit", 25, "maximum entries")
	_ = fs.Parse(args)
	var out struct {
		Entries []projection.AuditEvent `json:"audit"`
	}
	path := "/v1/audit" + client.Query("limit", fmt.Sprint(*limit))
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Entries)
}

// ---------- queue and work ----------

func (c *ctl) cmdQueue(args []string) {
	fs := newFlagSet("queue")
	issues := fs.String("issues", "", "comma-separated issue ids")
	workflow := fs.String("workflow", "", "workflow type (default: the junction's)")
	installation := fs.String("installation", "", "installation (default: inferred from the issues)")
	_ = fs.Parse(args)
	idList := splitList(*issues)
	if len(idList) == 0 {
		fail(errors.New("queue requires --issues"))
	}
	var item projection.WorkItem
	if err := c.client.Post(context.Background(), "/v1/work-items", engine.QueueIssueRequest{
		IssueIDs:       idList,
		WorkflowType:   *workflow,
		InstallationID: *installation,
	}, &item); err != nil {
		fail(err)
	}
	c.print(item)
}

func (c *ctl) cmdClaim(args []string) {
	fs := newFlagSet("claim")
	worker := fs.String("worker", "", "worker identity")
	workflows := fs.String("workflow", "", "comma-separated workflow types this worker handles")
	ttl := fs.Int("ttl", 900, "lease seconds")
	_ = fs.Parse(args)
	if *worker == "" {
		fail(errors.New("claim requires --worker"))
	}
	var item projection.WorkItem
	if err := c.client.Post(context.Background(), "/v1/work-items/claim", map[string]any{
		"worker":         *worker,
		"workflow_types": splitList(*workflows),
		"ttl_seconds":    *ttl,
	}, &item); err != nil {
		fail(err)
	}
	c.print(item)
}

func (c *ctl) cmdComplete(args []string) {
	fs := newFlagSet("complete")
	worker := fs.String("worker", "", "worker identity")
	state := fs.String("state", "RETURNED", "RETURNED, FAILED or CANCELLED")
	result := fs.String("result", "", "JSON result payload")
	rest := parseWithPositionals(fs, args)
	if len(rest) != 1 {
		fail(errors.New("usage: lymphctl complete WORK_ID --worker NAME [--state RETURNED] [--result JSON]"))
	}
	body := map[string]any{"worker": *worker, "state": strings.ToUpper(*state)}
	if *result != "" {
		if !json.Valid([]byte(*result)) {
			fail(fmt.Errorf("--result is not valid JSON: %s", *result))
		}
		body["result"] = json.RawMessage(*result)
	}
	var item projection.WorkItem
	if err := c.client.Post(context.Background(), "/v1/work-items/"+rest[0]+"/complete", body, &item); err != nil {
		fail(err)
	}
	c.print(item)
}

// ---------- maintenance ----------

func (c *ctl) cmdLedger(args []string) {
	if len(args) == 0 || args[0] != "verify" {
		fail(errors.New("usage: lymphctl ledger verify"))
	}
	var report ledger.VerifyReport
	if err := c.client.Get(context.Background(), "/v1/ledger/verify", &report); err != nil {
		fail(err)
	}
	c.print(report)
}

func (c *ctl) cmdRebuild(args []string) {
	fs := newFlagSet("rebuild")
	yes := fs.Bool("yes", false, "confirm rebuilding the projection from the ledger")
	_ = fs.Parse(args)
	if !*yes {
		fail(errors.New("rebuild discards the SQLite projection; pass --yes to confirm (canonical ledger and objects are untouched)"))
	}
	var out map[string]any
	if err := c.client.Post(context.Background(), "/v1/projection/rebuild", map[string]any{}, &out); err != nil {
		fail(err)
	}
	c.print(out)
}

func (c *ctl) cmdObject(args []string) {
	if len(args) == 0 || args[0] != "get" {
		fail(errors.New("usage: lymphctl object get HASH [--out FILE]"))
	}
	fs := newFlagSet("object get")
	out := fs.String("out", "", "write to this file instead of stdout")
	rest := parseWithPositionals(fs, args[1:])
	if len(rest) != 1 {
		fail(errors.New("usage: lymphctl object get HASH [--out FILE]"))
	}
	hash := rest[0]
	raw, err := c.client.GetRaw(context.Background(), "/v1/objects/"+hash)
	if err != nil {
		fail(err)
	}
	if *out != "" {
		if err := os.WriteFile(*out, raw, 0o640); err != nil {
			fail(err)
		}
		fmt.Fprintf(os.Stderr, "wrote %d bytes to %s\n", len(raw), *out)
		return
	}
	os.Stdout.Write(raw)
	if len(raw) > 0 && raw[len(raw)-1] != '\n' {
		fmt.Println()
	}
}

// ---------- helpers ----------

// loadBundle builds a content bundle from either a single file or a directory.
// Bundle paths are relative and slash-separated so the same bundle verifies on
// any machine.
func loadBundle(file, dir string) (objectstore.Bundle, error) {
	switch {
	case file != "" && dir != "":
		return nil, errors.New("give either --file or --dir, not both")
	case file == "" && dir == "":
		return nil, errors.New("a bundle needs --file or --dir")
	case file != "":
		raw, err := readInput(file)
		if err != nil {
			return nil, err
		}
		return objectstore.Bundle{filepath.Base(file): raw}, nil
	default:
		bundle := objectstore.Bundle{}
		err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			rel, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			bundle[filepath.ToSlash(rel)] = raw
			return nil
		})
		if err != nil {
			return nil, err
		}
		if len(bundle) == 0 {
			return nil, fmt.Errorf("%s contains no files", dir)
		}
		return bundle, nil
	}
}

func readInput(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

func splitList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
