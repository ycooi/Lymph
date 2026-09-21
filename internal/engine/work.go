package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/ledger"
	"github.com/ycooi/Lymph/internal/projection"
)

// recordCandidateStateTx journals one candidate state transition. The state
// itself is a projection column; the transition is canonical (section 94,
// section 95).
func (e *Engine) recordCandidateStateTx(tx *sql.Tx, cand projection.Candidate, from, to CandidateState, reason string, at time.Time) error {
	rec, err := e.ledger.Append(ledger.KindCandidate, cand.CandidateID, candidatePayload{
		Candidate: cand,
		FromState: string(from),
		Reason:    reason,
		At:        at,
	}, ledger.Durable)
	if err != nil {
		return err
	}
	if err := projection.InsertCandidate(tx, cand); err != nil {
		return err
	}
	if err := projection.IndexLedgerRecordTx(tx, rec); err != nil {
		return err
	}
	return checkpoint(tx, rec)
}

// QueueIssue turns one or more issues into a WorkItem for an external workflow
// (section 39, section 38).
//
// Lymph decides *where* the work goes, never *how* to fix it.
type QueueIssueRequest struct {
	IssueIDs       []string `json:"issue_ids"`
	Application    string   `json:"application"`
	ConfigFamily   string   `json:"config_family"`
	InstallationID string   `json:"installation_id"`
	WorkflowType   string   `json:"workflow_type"`
	BaseRevisionID string   `json:"base_revision_id"`
	Package        string   `json:"package"`
	Actor          string   `json:"actor"`
}

// QueueWork creates a queued work item and moves the issues into
// QUEUED_FOR_IMPROVEMENT.
func (e *Engine) QueueWork(ctx context.Context, req QueueIssueRequest) (projection.WorkItem, error) {
	if len(req.IssueIDs) == 0 {
		return projection.WorkItem{}, errors.New("work item requires at least one issue")
	}
	now := time.Now().UTC()
	var out projection.WorkItem

	err := e.withTx(func(tx *sql.Tx) error {
		var applicationID, junctionID, familyID string
		packages := map[string]any{}
		issueIDs := make([]string, 0, len(req.IssueIDs))

		for i, issueID := range req.IssueIDs {
			issue, err := projection.GetIssueTx(tx, issueID)
			if err != nil {
				return err
			}
			if i == 0 {
				applicationID = issue.ApplicationID
				junctionID = issue.JunctionID
			} else if issue.ApplicationID != applicationID {
				return errors.New("a work item may only group issues from one application")
			}
			issueIDs = append(issueIDs, issue.IssueID)
			packages[issue.IssueID] = map[string]any{
				"fingerprint":        issue.Fingerprint,
				"pattern":            issue.Pattern,
				"feedback_type":      issue.FeedbackType,
				"reason_code":        issue.ReasonCode,
				"occurrence_count":   issue.OccurrenceCount,
				"controlling_config": issue.ControllingRevision,
			}
		}

		family := req.ConfigFamily
		if family == "" {
			if j, err := projection.GetJunctionTx(tx, junctionID); err == nil && j.ConfigFamilyID != "" {
				family = j.ConfigFamilyID
			}
		}
		if family != "" {
			f, err := e.resolveFamilyTx(tx, firstNonEmpty(req.Application, applicationID), family)
			if err != nil {
				return err
			}
			familyID = f.ConfigFamilyID
		}

		workflowType := req.WorkflowType
		if workflowType == "" {
			if j, err := projection.GetJunctionTx(tx, junctionID); err == nil && j.WorkflowType != "" {
				workflowType = j.WorkflowType
			}
		}
		if workflowType == "" {
			workflowType = "manual_domain_review"
		}

		// Which installation's configuration state does this work target?
		//
		// An explicit choice wins. Otherwise the issues decide: if every
		// occurrence came from one deployment, that is the answer; if none said,
		// the application default applies; if several disagree and the work
		// controls a config family, guessing would be worse than refusing
		// (mission sections 59, 60).
		app, err := projection.GetApplicationTx(tx, applicationID)
		if err != nil {
			return err
		}
		installationID := req.InstallationID
		if installationID != "" {
			installationID, err = e.resolveInstallationTx(tx, app, installationID)
			if err != nil {
				return err
			}
		} else {
			seen, err := projection.DistinctIssueInstallations(tx, issueIDs)
			if err != nil {
				return err
			}
			switch {
			case len(seen) == 1:
				installationID = seen[0]
			case len(seen) == 0:
				installationID = projection.DefaultInstallationID(app)
			case familyID != "":
				return fmt.Errorf("%w: issues span installations %v; queue one work item per installation or name one explicitly",
					ErrAmbiguousInstallation, seen)
			default:
				installationID = projection.DefaultInstallationID(app)
			}
		}

		base := req.BaseRevisionID
		if base == "" && familyID != "" {
			current, err := projection.CurrentRefRevision(tx, applicationID, familyID, installationID, RefActive)
			if err != nil {
				return err
			}
			base = current
		}

		pkg, err := json.Marshal(map[string]any{
			"application_id":   applicationID,
			"junction_id":      junctionID,
			"config_family_id": familyID,
			"installation_id":  installationID,
			"base_revision_id": base,
			"issues":           packages,
			"workflow_type":    workflowType,
			"required_output": "ImprovementResult with typed ImprovementArtifacts; " +
				"CONFIG_BUNDLE may create a ConfigCandidate; " +
				"the worker must never write a managed production target",
			"requested_by": req.Actor,
			"requested_at": now,
		})
		if err != nil {
			return err
		}

		work := projection.WorkItem{
			WorkID:         identity.NewID(),
			WorkflowType:   workflowType,
			ApplicationID:  applicationID,
			JunctionID:     junctionID,
			ConfigFamilyID: familyID,
			InstallationID: installationID,
			BaseRevisionID: base,
			IssueIDs:       issueIDs,
			Package:        string(pkg),
			State:          string(WorkQueued),
			CreatedAt:      now,
			UpdatedAt:      now,
		}

		rec, err := e.ledger.Append(ledger.KindWorkItem, work.WorkID, workPayload{
			WorkItem: work,
			Reason:   "queued from issues",
			At:       now,
		}, ledger.Durable)
		if err != nil {
			return err
		}
		if err := projection.InsertWorkItem(tx, work); err != nil {
			return err
		}
		for _, issueID := range issueIDs {
			if err := e.transitionIssueTx(tx, issueID, IssueQueuedForImprovement, req.Actor, now); err != nil {
				return err
			}
		}
		if err := projection.IndexLedgerRecordTx(tx, rec); err != nil {
			return err
		}
		if err := checkpoint(tx, rec); err != nil {
			return err
		}
		out = work
		return nil
	})
	if err != nil {
		return projection.WorkItem{}, err
	}
	return out, nil
}

// resolveFamilyTx resolves an application and family reference inside a
// transaction.
func (e *Engine) resolveFamilyTx(tx *sql.Tx, appRef, familyRef string) (projection.ConfigFamily, error) {
	var app projection.Application
	var err error
	if identity.Valid(appRef) {
		app, err = projection.GetApplicationTx(tx, appRef)
	} else {
		app, err = projection.GetApplicationByNameTx(tx, appRef)
	}
	if err != nil {
		return projection.ConfigFamily{}, err
	}
	if identity.Valid(familyRef) {
		return projection.GetConfigFamilyTx(tx, familyRef)
	}
	return projection.GetConfigFamilyByNameTx(tx, app.ApplicationID, familyRef)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// transitionIssueTx moves an issue's status, journaling the transition as an
// immutable record (section 37).
func (e *Engine) transitionIssueTx(tx *sql.Tx, issueID string, to IssueStatus, actor string, at time.Time) error {
	issue, err := projection.GetIssueTx(tx, issueID)
	if err != nil {
		return err
	}
	if err := CheckIssueTransition(IssueStatus(issue.Status), to); err != nil {
		return err
	}
	if issue.Status == string(to) {
		return nil
	}
	rec, err := e.ledger.Append(ledger.KindIssue, issueID, issueStatusPayload{
		IssueID: issueID,
		Status:  string(to),
		Actor:   actor,
		At:      at,
	}, ledger.Durable)
	if err != nil {
		return err
	}
	if err := projection.SetIssueStatus(tx, issueID, string(to), at); err != nil {
		return err
	}
	if err := projection.IndexLedgerRecordTx(tx, rec); err != nil {
		return err
	}
	return checkpoint(tx, rec)
}

// SetIssueStatus is the public form of a status transition.
func (e *Engine) SetIssueStatus(ctx context.Context, issueID string, to IssueStatus, actor, comment string) (projection.Issue, error) {
	if !ValidIssueStatus(to) {
		return projection.Issue{}, fmt.Errorf("unknown issue status %q", to)
	}
	now := time.Now().UTC()
	var out projection.Issue
	err := e.withTx(func(tx *sql.Tx) error {
		if err := e.transitionIssueTx(tx, issueID, to, actor, now); err != nil {
			return err
		}
		issue, err := projection.GetIssueTx(tx, issueID)
		if err != nil {
			return err
		}
		out = issue
		if comment != "" {
			return appendAuditTx(e, tx, projection.AuditEvent{
				AuditID:       identity.NewID(),
				Action:        "issue.status_changed",
				Actor:         actor,
				ApplicationID: issue.ApplicationID,
				SubjectKind:   "issue",
				SubjectID:     issue.IssueID,
				Detail:        fmt.Sprintf(`{"status":%q,"comment":%q}`, to, comment),
				CreatedAt:     now,
			}, ledger.Async)
		}
		return nil
	})
	if err != nil {
		return projection.Issue{}, err
	}
	return out, nil
}

// ClaimRequest asks for a lease on a queued work item (section 41).
type ClaimRequest struct {
	Worker        string        `json:"worker"`
	WorkflowTypes []string      `json:"workflow_types"`
	TTL           time.Duration `json:"ttl"`
}

// ClaimWork leases the oldest matching queued item. Leases rather than
// fire-and-forget delivery are what make worker crashes survivable
// (section 42).
func (e *Engine) ClaimWork(ctx context.Context, req ClaimRequest) (projection.WorkItem, error) {
	if req.Worker == "" {
		return projection.WorkItem{}, errors.New("claim requires a worker identity")
	}
	if req.TTL <= 0 {
		req.TTL = 15 * time.Minute
	}
	now := time.Now().UTC()
	var out projection.WorkItem

	err := e.withTx(func(tx *sql.Tx) error {
		work, err := projection.ClaimableWorkItem(tx, req.WorkflowTypes, now)
		if err != nil {
			return err
		}
		from := WorkState(work.State)
		if err := CheckWorkTransition(from, WorkLeased); err != nil {
			return err
		}
		work.State = string(WorkLeased)
		work.LeaseOwner = req.Worker
		work.LeaseExpiresAt = now.Add(req.TTL)
		work.Attempts++
		work.UpdatedAt = now

		rec, err := e.ledger.Append(ledger.KindWorkItem, work.WorkID, workPayload{
			WorkItem:  work,
			FromState: string(from),
			Reason:    "leased to " + req.Worker,
			At:        now,
		}, ledger.Durable)
		if err != nil {
			return err
		}
		if err := projection.InsertWorkItem(tx, work); err != nil {
			return err
		}
		if err := projection.IndexLedgerRecordTx(tx, rec); err != nil {
			return err
		}
		if err := checkpoint(tx, rec); err != nil {
			return err
		}
		out = work
		return nil
	})
	if err != nil {
		return projection.WorkItem{}, err
	}
	return out, nil
}

// RenewLease extends a lease. A worker that disappears simply stops renewing
// and the item returns to the queue.
func (e *Engine) RenewLease(ctx context.Context, workID, worker string, ttl time.Duration) (projection.WorkItem, error) {
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	now := time.Now().UTC()
	var out projection.WorkItem
	err := e.withTx(func(tx *sql.Tx) error {
		work, err := projection.GetWorkItemTx(tx, workID)
		if err != nil {
			return err
		}
		if work.LeaseOwner != worker {
			return fmt.Errorf("work item %s is leased to %s, not %s", workID, work.LeaseOwner, worker)
		}
		work.LeaseExpiresAt = now.Add(ttl)
		work.UpdatedAt = now
		rec, err := e.ledger.Append(ledger.KindWorkItem, work.WorkID, workPayload{
			WorkItem:  work,
			FromState: work.State,
			Reason:    "lease renewed",
			At:        now,
		}, ledger.Async)
		if err != nil {
			return err
		}
		if err := projection.InsertWorkItem(tx, work); err != nil {
			return err
		}
		if err := projection.IndexLedgerRecordTx(tx, rec); err != nil {
			return err
		}
		if err := checkpoint(tx, rec); err != nil {
			return err
		}
		out = work
		return nil
	})
	if err != nil {
		return projection.WorkItem{}, err
	}
	return out, nil
}

// CompleteWorkRequest finishes a leased work item.
type CompleteWorkRequest struct {
	WorkID string    `json:"work_id"`
	Worker string    `json:"worker"`
	State  WorkState `json:"state"`
	Result string    `json:"result"`
}

// CompleteWork records the outcome. RETURNED means a candidate came back;
// FAILED and CANCELLED are terminal for this attempt (section 43).
func (e *Engine) CompleteWork(ctx context.Context, req CompleteWorkRequest) (projection.WorkItem, error) {
	if req.State == "" {
		req.State = WorkReturned
	}
	if !ValidWorkState(req.State) {
		return projection.WorkItem{}, fmt.Errorf("unknown work state %q", req.State)
	}
	now := time.Now().UTC()
	var out projection.WorkItem

	err := e.withTx(func(tx *sql.Tx) error {
		work, err := projection.GetWorkItemTx(tx, req.WorkID)
		if err != nil {
			return err
		}
		if req.Worker != "" && work.LeaseOwner != "" && work.LeaseOwner != req.Worker {
			return fmt.Errorf("work item %s is leased to %s, not %s", req.WorkID, work.LeaseOwner, req.Worker)
		}
		from := WorkState(work.State)
		if err := CheckWorkTransition(from, req.State); err != nil {
			return err
		}
		work.State = string(req.State)
		work.Result = req.Result
		work.LeaseOwner = ""
		work.LeaseExpiresAt = time.Time{}
		work.UpdatedAt = now

		rec, err := e.ledger.Append(ledger.KindWorkItem, work.WorkID, workPayload{
			WorkItem:  work,
			FromState: string(from),
			Reason:    "completed by " + req.Worker,
			At:        now,
		}, ledger.Durable)
		if err != nil {
			return err
		}
		if err := projection.InsertWorkItem(tx, work); err != nil {
			return err
		}
		if err := projection.IndexLedgerRecordTx(tx, rec); err != nil {
			return err
		}
		if err := checkpoint(tx, rec); err != nil {
			return err
		}
		out = work
		return nil
	})
	if err != nil {
		return projection.WorkItem{}, err
	}
	return out, nil
}

// SweepLeases returns expired leases to the queue (section 87). It runs at
// startup and may be called periodically.
func (e *Engine) SweepLeases(ctx context.Context) (int, error) {
	now := time.Now().UTC()
	released := 0

	err := e.withTx(func(tx *sql.Tx) error {
		expired, err := projection.ExpiredLeases(tx, now)
		if err != nil {
			return err
		}
		for _, work := range expired {
			if err := CheckWorkTransition(WorkState(work.State), WorkExpired); err != nil {
				return err
			}
			work.State = string(WorkExpired)
			work.LeaseOwner = ""
			work.LeaseExpiresAt = time.Time{}
			work.UpdatedAt = now
			expiryRecord, err := e.ledger.Append(ledger.KindWorkItem, work.WorkID, workPayload{
				WorkItem:  work,
				FromState: string(WorkLeased),
				Reason:    "lease expired",
				At:        now,
			}, ledger.Async)
			if err != nil {
				return err
			}
			if err := projection.InsertWorkItem(tx, work); err != nil {
				return err
			}
			if err := projection.IndexLedgerRecordTx(tx, expiryRecord); err != nil {
				return err
			}
			// Expiry is not failure: the item goes back to the queue so another
			// attempt can be made (at-least-once delivery).
			if err := CheckWorkTransition(WorkExpired, WorkQueued); err != nil {
				return err
			}
			work.State = string(WorkQueued)
			work.UpdatedAt = now
			rec, err := e.ledger.Append(ledger.KindWorkItem, work.WorkID, workPayload{
				WorkItem:  work,
				FromState: string(WorkExpired),
				Reason:    "requeued after lease expiry",
				At:        now,
			}, ledger.Async)
			if err != nil {
				return err
			}
			if err := projection.InsertWorkItem(tx, work); err != nil {
				return err
			}
			if err := projection.IndexLedgerRecordTx(tx, rec); err != nil {
				return err
			}
			released++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return released, nil
}

// Issues lists issues for the query API.
func (e *Engine) Issues(status string, limit int, order string) ([]projection.Issue, error) {
	return e.db.ListIssues(status, limit, order)
}

// Events lists recent events.
func (e *Engine) Events(applicationID string, limit int) ([]projection.Event, error) {
	return e.db.ListEvents(applicationID, limit)
}

// WorkItems lists work items by state.
func (e *Engine) WorkItems(state string, limit int) ([]projection.WorkItem, error) {
	return e.db.ListWorkItems(state, limit)
}
