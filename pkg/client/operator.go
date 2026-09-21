package client

import (
	"context"
	"fmt"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/projection"
)

// Typed access to the parts of the protocol the update loop depends on.
//
// The SDK deliberately does not re-export projection internals; these are
// methods for the calls a worker, an operator tool or a test actually makes,
// with the wire shapes named once. An application itself needs none of them:
// it needs CheckUpdate, FetchUpdate, ReportApplied and ReportRejected.

// RegisterApplication registers or updates an application manifest.
func (c *Client) RegisterApplication(ctx context.Context, manifest engine.Manifest) (engine.Registration, error) {
	var out engine.Registration
	err := c.post(ctx, "/v1/applications", manifest, &out)
	return out, err
}

// PostBaseline imports a config family's starting revision.
func (c *Client) PostBaseline(ctx context.Context, req engine.BaselineRequest) (projection.ConfigRevision, error) {
	var out projection.ConfigRevision
	err := c.post(ctx, "/v1/baselines", req, &out)
	return out, err
}

// PostCandidate proposes a change.
func (c *Client) PostCandidate(ctx context.Context, req engine.CandidateRequest) (projection.Candidate, error) {
	var out projection.Candidate
	err := c.post(ctx, "/v1/candidates", req, &out)
	return out, err
}

// PostValidation records one piece of test evidence about a candidate.
func (c *Client) PostValidation(ctx context.Context, candidateID string, req engine.ValidationRequest) (projection.Candidate, error) {
	var out projection.Candidate
	err := c.post(ctx, "/v1/candidates/"+candidateID+"/validations", req, &out)
	return out, err
}

// PostApprove records a human decision. The daemon decides the actor type from
// the caller's local credentials, so this method never sends one.
func (c *Client) PostApprove(ctx context.Context, candidateID string, req engine.ApprovalRequest) (projection.Candidate, error) {
	var out projection.Candidate
	err := c.post(ctx, "/v1/candidates/"+candidateID+"/approve", req, &out)
	return out, err
}

// PostPromote turns an approved candidate into an immutable revision.
func (c *Client) PostPromote(ctx context.Context, candidateID, actor string) (engine.PromotionResult, error) {
	var out engine.PromotionResult
	err := c.post(ctx, "/v1/candidates/"+candidateID+"/promote", map[string]string{"actor": actor}, &out)
	return out, err
}

// CandidateDetail is one candidate with everything a reviewer needs.
type CandidateDetail struct {
	Candidate   projection.Candidate    `json:"candidate"`
	Validations []projection.Validation `json:"validations"`
	Approvals   []projection.Approval   `json:"approvals"`
	ContentHash string                  `json:"content_hash"`
}

// Candidate returns one candidate's proposal, evidence and decision history.
func (c *Client) Candidate(ctx context.Context, candidateID string) (CandidateDetail, error) {
	var out CandidateDetail
	err := c.get(ctx, "/v1/candidates/"+candidateID, &out)
	return out, err
}

// WithdrawUpdate stops offering an approved revision to one installation.
func (c *Client) WithdrawUpdate(ctx context.Context, applicationID, installationID, configFamilyID, revisionID, reason string) (projection.UpdateDisposition, error) {
	var out projection.UpdateDisposition
	path := "/v1/updates/" + revisionID + "/withdraw" +
		Query("application_id", applicationID, "installation_id", installationID)
	err := c.post(ctx, path, map[string]string{
		"config_family_id": configFamilyID,
		"reason":           reason,
	}, &out)
	return out, err
}

// ApplicationResults lists what nodes reported doing.
func (c *Client) ApplicationResults(ctx context.Context, applicationID, installationID string, limit int) ([]ApplicationResult, error) {
	var out struct {
		Results []ApplicationResult `json:"application_results"`
	}
	path := "/v1/application-results" + Query("application", applicationID,
		"installation", installationID, "limit", fmt.Sprint(limit))
	if err := c.get(ctx, path, &out); err != nil {
		return nil, err
	}
	return out.Results, nil
}

// UpdateDispositions lists per-installation delivery state.
func (c *Client) UpdateDispositions(ctx context.Context, applicationID, installationID string, limit int) ([]projection.UpdateDisposition, error) {
	var out struct {
		Dispositions []projection.UpdateDisposition `json:"update_dispositions"`
	}
	path := "/v1/update-dispositions" + Query("application", applicationID,
		"installation", installationID, "limit", fmt.Sprint(limit))
	if err := c.get(ctx, path, &out); err != nil {
		return nil, err
	}
	return out.Dispositions, nil
}

// Rebuild replays the canonical ledger into a fresh projection. It is the
// disaster-recovery gesture: nothing that matters may live only in SQLite.
func (c *Client) Rebuild(ctx context.Context) (map[string]any, error) {
	var out map[string]any
	err := c.post(ctx, "/v1/projection/rebuild", nil, &out)
	return out, err
}
