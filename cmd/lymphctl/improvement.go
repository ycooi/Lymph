package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/pkg/client"
)

// Worker-return commands. A return can carry any of five artifact kinds, which
// is far too much to express as shell flags, so the payload comes from a file.

func (c *ctl) cmdReturnResult(args []string) {
	fs := newFlagSet("return-result")
	worker := fs.String("worker", "", "worker identity that holds the lease")
	attempt := fs.Int("attempt", 0, "lease attempt number the worker holds")
	file := fs.String("file", "", "JSON file describing the return (- for stdin)")
	runID := fs.String("workflow-run", "", "optional workflow run identity")
	summary := fs.String("summary", "", "optional one-line summary")
	rest := parseWithPositionals(fs, args)
	if len(rest) != 1 || *file == "" || *worker == "" {
		fail(errors.New("usage: lymphctl return-result WORK_ID --worker NAME --attempt N --file result.json"))
	}

	raw, err := readInput(*file)
	if err != nil {
		fail(err)
	}
	var req engine.ReturnImprovementRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		fail(fmt.Errorf("parse %s: %w", *file, err))
	}
	// Flags win over the file, so an operator can correct one field without
	// rewriting the artifact document.
	req.WorkID = rest[0]
	if *worker != "" {
		req.Worker = *worker
	}
	if *attempt > 0 {
		req.Attempt = *attempt
	}
	if *runID != "" {
		req.WorkflowRunID = *runID
	}
	if *summary != "" {
		req.Summary = *summary
	}
	if req.Attempt <= 0 {
		fail(errors.New("--attempt is required and must be positive"))
	}

	var out engine.ReturnImprovementResponse
	if err := c.client.Post(context.Background(), "/v1/work-items/"+req.WorkID+"/return", req, &out); err != nil {
		fail(err)
	}
	c.print(out)
}

func (c *ctl) cmdResults(args []string) {
	fs := newFlagSet("results")
	app := fs.String("application", "", "filter by application")
	limit := fs.Int("limit", 25, "maximum results")
	_ = fs.Parse(args)
	var out struct {
		Results []projection.ImprovementResult `json:"improvement_results"`
	}
	path := "/v1/improvement-results" + client.Query("application", *app, "limit", fmt.Sprint(*limit))
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Results)
}

func (c *ctl) cmdResult(args []string) {
	fs := newFlagSet("result")
	rest := parseWithPositionals(fs, args)
	if len(rest) != 1 {
		fail(errors.New("usage: lymphctl result RESULT_ID"))
	}
	var out resultWithArtifacts
	if err := c.client.Get(context.Background(), "/v1/improvement-results/"+rest[0], &out); err != nil {
		fail(err)
	}
	c.print(out)
}

// resultWithArtifacts is the shape of GET /v1/improvement-results/{id}.
type resultWithArtifacts struct {
	Result    projection.ImprovementResult     `json:"result"`
	Artifacts []projection.ImprovementArtifact `json:"artifacts"`
}

func (c *ctl) cmdArtifacts(args []string) {
	fs := newFlagSet("artifacts")
	app := fs.String("application", "", "filter by application")
	kind := fs.String("kind", "", "filter by kind")
	work := fs.String("work-item", "", "filter by the work item that produced them")
	limit := fs.Int("limit", 25, "maximum artifacts")
	_ = fs.Parse(args)

	if *kind != "" && !engine.ValidArtifactKind(engine.ArtifactKind(strings.ToUpper(*kind))) {
		fail(fmt.Errorf("unknown kind %q; known kinds: %s", *kind, kindNames()))
	}
	var out struct {
		Artifacts []projection.ImprovementArtifact `json:"artifacts"`
	}
	path := "/v1/artifacts" + client.Query("application", *app,
		"kind", strings.ToUpper(*kind), "work_item", *work, "limit", fmt.Sprint(*limit))
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Artifacts)
}

func (c *ctl) cmdArtifact(args []string) {
	fs := newFlagSet("artifact")
	rest := parseWithPositionals(fs, args)
	if len(rest) != 1 {
		fail(errors.New("usage: lymphctl artifact ARTIFACT_ID"))
	}
	var out projection.ImprovementArtifact
	if err := c.client.Get(context.Background(), "/v1/artifacts/"+rest[0], &out); err != nil {
		fail(err)
	}
	c.print(out)
}

func (c *ctl) cmdInstallations(args []string) {
	fs := newFlagSet("installations")
	app := fs.String("application", "", "filter by application")
	_ = fs.Parse(args)
	var out struct {
		Installations []projection.Installation `json:"installations"`
	}
	path := "/v1/installations" + client.Query("application", *app)
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Installations)
}

func kindNames() string {
	kinds := engine.ArtifactKinds()
	out := make([]string, len(kinds))
	for i, kind := range kinds {
		out[i] = string(kind)
	}
	return strings.Join(out, ", ")
}
