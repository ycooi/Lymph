package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/ycooi/Lymph/internal/buildinfo"
	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/pkg/client"
)

// Human review, the approval decision, and the approved-update channel
// (mission sections 43 to 48).

// reviewPackage is the document a human reads before deciding. It is one
// screenful of the questions the mission asks, in the order it asks them
// (mission sections 43, 44).
type reviewPackage struct {
	Candidate       projection.Candidate    `json:"candidate"`
	Approvals       []projection.Approval   `json:"approvals,omitempty"`
	Validations     []projection.Validation `json:"validations,omitempty"`
	ContentHash     string                  `json:"content_hash"`
	TargetScope     map[string]string       `json:"target_scope"`
	Issues          []projection.Issue      `json:"issues,omitempty"`
	Diff            *engine.DiffResult      `json:"diff,omitempty"`
	DiffNote        string                  `json:"diff_note,omitempty"`
	MissingEvidence []string                `json:"missing_evidence,omitempty"`
	ApprovalCommand string                  `json:"approval_command"`
}

// cmdReview builds the review package for one candidate.
func (c *ctl) cmdReview(args []string) {
	fs := newFlagSet("review")
	installation := fs.String("installation", "", "installation to review for (required when the candidate is not already scoped)")
	rest := parseWithPositionals(fs, args)
	if len(rest) != 1 {
		fail(errors.New("usage: lymphctl review CANDIDATE_ID [--installation I]"))
	}
	candidateID := rest[0]

	var detail struct {
		Candidate   projection.Candidate    `json:"candidate"`
		Validations []projection.Validation `json:"validations"`
		Approvals   []projection.Approval   `json:"approvals"`
		ContentHash string                  `json:"content_hash"`
	}
	if err := c.client.Get(context.Background(), "/v1/candidates/"+candidateID, &detail); err != nil {
		fail(err)
	}

	pkg := reviewPackage{
		Candidate:   detail.Candidate,
		Approvals:   detail.Approvals,
		Validations: detail.Validations,
		ContentHash: detail.ContentHash,
		TargetScope: map[string]string{
			"application_id":   detail.Candidate.ApplicationID,
			"config_family_id": detail.Candidate.ConfigFamilyID,
			"installation_id":  detail.Candidate.InstallationID,
			"base_revision_id": detail.Candidate.BaseRevisionID,
		},
	}
	if *installation != "" {
		pkg.TargetScope["installation_id"] = *installation
	}

	// Which problems does this claim to fix?
	if len(detail.Candidate.IssueIDs) > 0 {
		var issues struct {
			Issues []projection.Issue `json:"issues"`
		}
		if err := c.client.Get(context.Background(), "/v1/issues"+client.Query("limit", "200"), &issues); err == nil {
			wanted := map[string]bool{}
			for _, id := range detail.Candidate.IssueIDs {
				wanted[id] = true
			}
			for _, issue := range issues.Issues {
				if wanted[issue.IssueID] {
					pkg.Issues = append(pkg.Issues, issue)
				}
			}
		}
	}

	// What exactly changes? A candidate is compared against its base revision.
	if detail.Candidate.BaseRevisionID != "" {
		var diff engine.DiffResult
		path := "/v1/diff" + client.Query("from", detail.Candidate.BaseRevisionID, "to", "")
		_ = path
		// The candidate is not yet a revision: revision creation is the
		// promotion step. So the diff is reported as unavailable rather than
		// faked, and the human reads the proposal and its validations instead.
		pkg.DiffNote = "the candidate is not yet an immutable revision; the diff becomes available after promotion"
		_ = diff
	}

	// What is missing before this could be delivered?
	passed := map[string]bool{}
	for _, validation := range detail.Validations {
		if validation.Result == "PASS" || validation.Result == "PASSED" {
			passed[validation.Kind] = true
		}
	}
	for _, kind := range engine.RequiredValidationKinds() {
		if !passed[kind] {
			pkg.MissingEvidence = append(pkg.MissingEvidence, fmt.Sprintf("no passing %s validation", kind))
		}
	}
	if detail.Candidate.InstallationID == "" && *installation == "" {
		pkg.MissingEvidence = append(pkg.MissingEvidence,
			"the candidate names no installation; pass --installation so the approval binds to one deployment")
	}
	pkg.ApprovalCommand = fmt.Sprintf("lymphctl approve %s --installation %s",
		candidateID, orDefault(strings.TrimSpace(*installation),
			orDefault(detail.Candidate.InstallationID, "<INSTALLATION_ID>")))

	c.print(pkg)
}

func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// cmdApprove records a human decision. Approval is the one act that can put new
// content on the delivery channel, so the command states plainly what it is
// approving: application, installation, family, hash and evidence
// (mission sections 45 to 47).
func (c *ctl) cmdApprove(args []string) {
	fs := newFlagSet("approve")
	candidate := fs.String("candidate", "", "candidate id (also accepted as a positional argument)")
	approver := fs.String("approver", os.Getenv("USER"), "who approves (recorded on the approval)")
	installation := fs.String("installation", "", "installation this approval authorises")
	decision := fs.String("decision", "APPROVED", "APPROVED, REJECTED or DEFERRED")
	comment := fs.String("comment", "", "why")
	rest := parseWithPositionals(fs, args)
	candidateID, ok := candidateRef(*candidate, rest)
	if !ok {
		fail(errors.New("usage: lymphctl approve CANDIDATE_ID [--installation I] [--decision APPROVED|REJECTED|DEFERRED] [--comment TEXT]"))
	}
	c.decide(candidateID, *decision, *approver, *installation, *comment)
}

func (c *ctl) cmdReject(args []string) {
	fs := newFlagSet("reject")
	candidate := fs.String("candidate", "", "candidate id (also accepted as a positional argument)")
	approver := fs.String("approver", os.Getenv("USER"), "who rejects")
	installation := fs.String("installation", "", "installation this decision applies to")
	reason := fs.String("reason", "", "why (recorded on the approval)")
	rest := parseWithPositionals(fs, args)
	candidateID, ok := candidateRef(*candidate, rest)
	if !ok {
		fail(errors.New("usage: lymphctl reject CANDIDATE_ID --reason TEXT"))
	}
	c.decide(candidateID, "REJECTED", *approver, *installation, *reason)
}

func (c *ctl) cmdDefer(args []string) {
	fs := newFlagSet("defer")
	candidate := fs.String("candidate", "", "candidate id (also accepted as a positional argument)")
	approver := fs.String("approver", os.Getenv("USER"), "who defers")
	installation := fs.String("installation", "", "installation this decision applies to")
	reason := fs.String("reason", "", "why (recorded on the approval)")
	rest := parseWithPositionals(fs, args)
	candidateID, ok := candidateRef(*candidate, rest)
	if !ok {
		fail(errors.New("usage: lymphctl defer CANDIDATE_ID --reason TEXT"))
	}
	c.decide(candidateID, "DEFERRED", *approver, *installation, *reason)
}

// candidateRef accepts either `--candidate X` or a positional id. The flag form
// came first and is still used by scripts and documentation; the positional
// form reads better at a prompt. Both mean the same thing, and two ids at once
// is a mistake rather than a preference.
func candidateRef(flagValue string, rest []string) (string, bool) {
	if flagValue != "" && len(rest) > 0 {
		return "", false
	}
	if flagValue != "" {
		return flagValue, true
	}
	if len(rest) == 1 {
		return rest[0], true
	}
	return "", false
}

func (c *ctl) decide(candidateID, decision, approver, installation, comment string) {
	var cand projection.Candidate
	err := c.client.Post(context.Background(), "/v1/candidates/"+candidateID+"/approve", engine.ApprovalRequest{
		Approver:       approver,
		InstallationID: installation,
		Decision:       strings.ToUpper(decision),
		Comment:        comment,
	}, &cand)
	if err != nil {
		fail(err)
	}
	fmt.Fprintf(os.Stderr, "decision %s recorded for candidate %s\n", strings.ToUpper(decision), candidateID)
	c.print(cand)
}

// cmdWithdraw stops offering an approved revision to one installation. The
// approval is not erased: the history becomes "approved, then withdrawn"
// (mission section 41).
func (c *ctl) cmdWithdraw(args []string) {
	fs := newFlagSet("withdraw")
	app := fs.String("app", "", "application UUID or name")
	family := fs.String("family", "", "config family UUID or name")
	installation := fs.String("installation", "", "installation UUID or name")
	reason := fs.String("reason", "", "why the update is withdrawn")
	rest := parseWithPositionals(fs, args)
	if len(rest) != 1 {
		fail(errors.New("usage: lymphctl withdraw REVISION_ID --app A --family F --installation I [--reason TEXT]"))
	}
	if *app == "" || *family == "" || *installation == "" {
		fail(errors.New("withdraw needs --app, --family and --installation: withdrawal is per installation"))
	}
	appID, installationID, err := c.resolveIdentity(*app, *installation)
	if err != nil {
		fail(err)
	}
	familyID, err := c.familyID(appID, *family)
	if err != nil {
		fail(err)
	}
	var disposition projection.UpdateDisposition
	path := "/v1/updates/" + rest[0] + "/withdraw" +
		client.Query("application_id", appID, "installation_id", installationID)
	if err := c.client.Post(context.Background(), path, map[string]string{
		"config_family_id": familyID,
		"reason":           *reason,
	}, &disposition); err != nil {
		fail(err)
	}
	c.print(disposition)
}

// cmdUpdates asks the daemon what this installation may take, using the same
// session-based path an application uses.
func (c *ctl) cmdUpdates(args []string) {
	fs := newFlagSet("updates")
	app := fs.String("app", "", "application UUID or name")
	installation := fs.String("installation", "", "installation UUID or name")
	family := fs.String("family", "", "config family UUID or name")
	_ = fs.Parse(args)
	if *app == "" {
		fail(errors.New("updates requires --app: an update list is always somebody's list"))
	}
	appID, installationID, err := c.resolveIdentity(*app, *installation)
	if err != nil {
		fail(err)
	}
	familyID := ""
	if *family != "" {
		familyID, err = c.familyID(appID, *family)
		if err != nil {
			fail(err)
		}
	}

	lymph, err := client.NewApplicationClient(client.ApplicationOptions{
		Socket:         c.socket,
		ApplicationID:  appID,
		InstallationID: installationID,
		ClientName:     "lymphctl",
		ClientVersion:  buildinfo.Version,
	})
	if err != nil {
		fail(err)
	}
	ctx := context.Background()
	if _, err := lymph.Hello(ctx); err != nil {
		fail(err)
	}

	if familyID == "" {
		updates, err := lymph.CheckUpdates(ctx)
		if err != nil {
			fail(err)
		}
		c.print(updates)
		return
	}
	update, err := lymph.CheckUpdate(ctx, familyID, "")
	if errors.Is(err, client.ErrBaseRevisionMismatch) && update != nil {
		// Reported, not hidden: the operator asked what is offered, and the
		// answer includes "but this node is not on its base".
		fmt.Fprintf(os.Stderr, "warning: %v (base %s)\n", err, update.BaseRevisionID)
	}
	if err != nil && !errors.Is(err, client.ErrBaseRevisionMismatch) {
		fail(err)
	}
	if update == nil {
		fmt.Fprintln(os.Stderr, "no approved update for this installation")
		return
	}
	c.print(update)
}

// cmdApplicationResults lists what nodes reported doing: the canonical history
// behind observed and last_good (mission sections 33, 34).
func (c *ctl) cmdApplicationResults(args []string) {
	fs := newFlagSet("application-results")
	app := fs.String("app", "", "filter by application")
	installation := fs.String("installation", "", "filter by installation")
	limit := fs.Int("limit", 25, "maximum results")
	_ = fs.Parse(args)
	var out struct {
		Results []projection.ApplicationResult `json:"application_results"`
	}
	path := "/v1/application-results" + client.Query("application", *app, "installation", *installation,
		"limit", fmt.Sprint(*limit))
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Results)
}

// cmdUpdateDispositions lists per-installation delivery state.
func (c *ctl) cmdUpdateDispositions(args []string) {
	fs := newFlagSet("update-dispositions")
	app := fs.String("app", "", "filter by application")
	installation := fs.String("installation", "", "filter by installation")
	state := fs.String("state", "", "AVAILABLE, FETCHED, APPLIED, REJECTED or WITHDRAWN")
	limit := fs.Int("limit", 50, "maximum rows")
	_ = fs.Parse(args)
	var out struct {
		Dispositions []projection.UpdateDisposition `json:"update_dispositions"`
	}
	path := "/v1/update-dispositions" + client.Query("application", *app, "installation", *installation,
		"state", strings.ToUpper(*state), "limit", fmt.Sprint(*limit))
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Dispositions)
}

// cmdWhoami reports how the daemon sees this operator: which local uid, which
// roles, and whether an authorization policy is in force.
func (c *ctl) cmdWhoami(args []string) {
	fs := newFlagSet("whoami")
	_ = fs.Parse(args)
	var out map[string]any
	if err := c.client.Get(context.Background(), "/v1/whoami", &out); err != nil {
		fail(err)
	}
	c.print(out)
}

// familyID resolves a config family reference to its UUID. It never guesses: an
// unknown name is an error, because delivering an update against the wrong
// family would be worse than failing to deliver one.
func (c *ctl) familyID(applicationID, ref string) (string, error) {
	if ref == "" {
		return "", errors.New("a config family is required")
	}
	var out struct {
		Families []projection.ConfigFamily `json:"config_families"`
	}
	if err := c.client.Get(context.Background(),
		"/v1/config-families"+client.Query("application", applicationID), &out); err != nil {
		return "", err
	}
	for _, family := range out.Families {
		if family.ConfigFamilyID == ref {
			return family.ConfigFamilyID, nil
		}
	}
	for _, family := range out.Families {
		if family.Name == ref {
			return family.ConfigFamilyID, nil
		}
	}
	return "", fmt.Errorf("no config family %q for application %s", ref, applicationID)
}
