package engine

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
)

// Installation identity is deployment state, not learning state.
//
// Two deployments of one application hold their own configuration refs, their
// own candidates and their own managed targets — but the feedback they emit
// still aggregates into one application's issues, because a semantic unknown in
// Region A and the same unknown in Region B are one problem to understand and
// possibly two deployments to fix (mission section 72).

// resolveInstallationTx turns a requested installation into the one to use.
//
// An explicit installation must belong to the application. An omitted one falls
// back to the application's default, which for applications registered before
// installations existed is the application identity itself — a stable value
// that replay can recompute without inventing anything (mission sections 40, 50).
func (e *Engine) resolveInstallationTx(tx *sql.Tx, app projection.Application, requested string) (string, error) {
	if requested == "" {
		return projection.DefaultInstallationID(app), nil
	}
	if !identity.Valid(requested) {
		return "", fmt.Errorf("installation %q is not a UUID", requested)
	}
	if _, err := projection.GetInstallationTx(tx, app.ApplicationID, requested); err != nil {
		if errors.Is(err, projection.ErrNotFound) {
			return "", fmt.Errorf("%w: installation %s does not belong to application %s",
				projection.ErrNotFound, requested, app.ApplicationID)
		}
		return "", err
	}
	return requested, nil
}

// InstallationSpec declares a deployment in a registration manifest.
type InstallationSpec struct {
	InstallationID string `json:"installation_id,omitempty"`
	Name           string `json:"name,omitempty"`
	Environment    string `json:"environment,omitempty"`
	// SessionPolicy is MULTI_PROCESS_ALLOWED (default) or
	// SINGLE_PROCESS_EXPECTED.
	SessionPolicy string `json:"session_policy,omitempty"`
}

// normalizeInstallations applies the rules from mission section 40 and returns
// the installations to record plus the default.
//
// The rules exist so that a single-deployment application never has to think
// about installations, while a multi-deployment application cannot be
// ambiguous by accident.
func normalizeInstallations(applicationID, legacyInstallationID string, specs []InstallationSpec) ([]InstallationSpec, string, error) {
	// The default installation may be derived from the application identity, so
	// the application identity has to be valid before anything is derived from
	// it. Without this check a malformed application ID would silently become an
	// installation ID.
	if !identity.Valid(applicationID) {
		return nil, "", fmt.Errorf("application %q is not a UUID", applicationID)
	}

	clone := func(specs []InstallationSpec) []InstallationSpec {
		out := make([]InstallationSpec, len(specs))
		copy(out, specs)
		return out
	}

	switch {
	case len(specs) == 0 && legacyInstallationID == "":
		// Deterministic default: the application identity. Not a fresh UUID,
		// so replay reaches the same answer without inventing identity.
		return []InstallationSpec{{InstallationID: applicationID, Name: "default"}}, applicationID, nil

	case len(specs) == 0:
		if !identity.Valid(legacyInstallationID) {
			return nil, "", fmt.Errorf("installation_id %q is not a UUID", legacyInstallationID)
		}
		return []InstallationSpec{{InstallationID: legacyInstallationID, Name: "default"}}, legacyInstallationID, nil
	}

	out := clone(specs)
	seen := map[string]bool{}
	for i := range out {
		if out[i].InstallationID == "" {
			out[i].InstallationID = identity.NewID()
		}
		if !identity.Valid(out[i].InstallationID) {
			return nil, "", fmt.Errorf("installation_id %q is not a UUID", out[i].InstallationID)
		}
		if seen[out[i].InstallationID] {
			return nil, "", fmt.Errorf("installation %s is declared twice", out[i].InstallationID)
		}
		seen[out[i].InstallationID] = true
	}

	if legacyInstallationID == "" {
		if len(out) == 1 {
			return out, out[0].InstallationID, nil
		}
		return nil, "", errors.New("registration declares several installations; declare the default with installation_id")
	}
	if !identity.Valid(legacyInstallationID) {
		return nil, "", fmt.Errorf("installation_id %q is not a UUID", legacyInstallationID)
	}
	if !seen[legacyInstallationID] {
		return nil, "", fmt.Errorf("default installation %s is not in the declared installation list", legacyInstallationID)
	}
	return out, legacyInstallationID, nil
}

// sessionPolicyOfSpec validates a declared session policy, defaulting to
// multi-process: an installation that says nothing must not become accidentally
// single-process, because that would refuse a legitimate second worker.
func sessionPolicyOfSpec(spec InstallationSpec) string {
	switch spec.SessionPolicy {
	case projection.SessionPolicySingle:
		return projection.SessionPolicySingle
	case projection.SessionPolicyMulti, "":
		return projection.SessionPolicyMulti
	default:
		return projection.SessionPolicyMulti
	}
}

// candidatePlan is everything a candidate needs, after the facts have been
// resolved but before a UUID has been minted.
//
// Splitting resolution from construction is what lets a worker return create a
// candidate inside the same transaction as its result, without nested
// transactions and without a window where a candidate exists alone.
type candidatePlan struct {
	ApplicationID  string
	ConfigFamilyID string
	InstallationID string
	Bundle         objectstore.Bundle
	TreeHash       string
	Blobs          []objectstore.Entry
	BaseRevisionID string
	IssueIDs       []string
	WorkItemID     string
	WorkflowRunID  string
	Explanation    string
}

// resolveCandidatePlanTx fills in and checks a plan against stored state:
// installation, base revision and issue ownership.
func (e *Engine) resolveCandidatePlanTx(tx *sql.Tx, app projection.Application, plan candidatePlan) (candidatePlan, error) {
	familyID := plan.ConfigFamilyID
	if familyID == "" {
		return plan, errors.New("candidate plan requires a config family")
	}
	family, err := projection.GetConfigFamilyTx(tx, familyID)
	if err != nil {
		return plan, err
	}
	if family.ApplicationID != app.ApplicationID {
		return plan, fmt.Errorf("config family %s belongs to another application", family.Name)
	}
	plan.ConfigFamilyID = familyID
	plan.ApplicationID = app.ApplicationID

	installationID, err := e.resolveInstallationTx(tx, app, plan.InstallationID)
	if err != nil {
		return plan, err
	}
	plan.InstallationID = installationID

	base := plan.BaseRevisionID
	if base == "" {
		current, err := projection.CurrentRefRevision(tx, app.ApplicationID, familyID, installationID, RefActive)
		if err != nil {
			return plan, err
		}
		if current == "" {
			return plan, fmt.Errorf("config family %s has no active revision for installation %s; import a baseline first",
				family.Name, installationID)
		}
		base = current
	} else if _, err := projection.GetRevisionTx(tx, base); err != nil {
		return plan, fmt.Errorf("base revision %s: %w", base, err)
	}
	plan.BaseRevisionID = base

	// A candidate may only claim issues that belong to its own application;
	// cross-application claims would corrupt the lineage.
	for _, issueID := range plan.IssueIDs {
		issue, err := projection.GetIssueTx(tx, issueID)
		if err != nil {
			return plan, fmt.Errorf("issue %s: %w", issueID, err)
		}
		if issue.ApplicationID != app.ApplicationID {
			return plan, fmt.Errorf("issue %s belongs to application %s, not %s",
				issueID, issue.ApplicationID, app.ApplicationID)
		}
	}
	return plan, nil
}

// buildCandidate mints the candidate identity from a resolved plan.
func buildCandidate(plan candidatePlan, resultID string, now time.Time) projection.Candidate {
	return projection.Candidate{
		CandidateID:    identity.NewID(),
		ApplicationID:  plan.ApplicationID,
		ConfigFamilyID: plan.ConfigFamilyID,
		InstallationID: plan.InstallationID,
		BaseRevisionID: plan.BaseRevisionID,
		RootTreeHash:   plan.TreeHash,
		State:          string(CandidateDraft),
		IssueIDs:       plan.IssueIDs,
		WorkItemID:     plan.WorkItemID,
		WorkflowRunID:  plan.WorkflowRunID,
		Explanation:    plan.Explanation,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}
