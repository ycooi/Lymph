package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/ledger"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
)

// RevisionRequest creates an immutable config revision (section 23, section 18).
type RevisionRequest struct {
	Application    string             `json:"application"`
	ConfigFamily   string             `json:"config_family"`
	InstallationID string             `json:"installation_id,omitempty"`
	Bundle         objectstore.Bundle `json:"bundle"`
	Message        string             `json:"message,omitempty"`
	Label          string             `json:"label,omitempty"`
	Author         string             `json:"author,omitempty"`
	IssueIDs       []string           `json:"issue_ids,omitempty"`
	WorkItemID     string             `json:"work_item_id,omitempty"`
	CandidateID    string             `json:"candidate_id,omitempty"`
	SchemaRevision string             `json:"schema_revision,omitempty"`
	// SetRefs names refs that should point at the new revision. Compare-and-swap
	// still applies through ExpectedRefs.
	SetRefs map[string]bool `json:"set_refs,omitempty"`
	// ExpectedRefs pins the previous value of each ref being moved. A nil entry
	// means "create only", a pointer means "must currently equal".
	ExpectedRefs map[string]*string `json:"expected_refs,omitempty"`
	Parent       string             `json:"parent_revision_id,omitempty"`
}

// CreateRevision stores a bundle as content-addressed objects and records an
// immutable revision. Raw bytes are canonical (section 33): the exact bytes
// given are the bytes hashed and stored.
func (e *Engine) CreateRevision(ctx context.Context, req RevisionRequest) (projection.ConfigRevision, error) {
	if len(req.Bundle) == 0 {
		return projection.ConfigRevision{}, errors.New("revision requires a non-empty bundle")
	}

	app, family, err := e.resolveFamily(ctx, req.Application, req.ConfigFamily)
	if err != nil {
		return projection.ConfigRevision{}, err
	}

	treeHash, err := e.objects.PutBundle(req.Bundle)
	if err != nil {
		return projection.ConfigRevision{}, err
	}
	tree, err := e.objects.GetTree(treeHash)
	if err != nil {
		return projection.ConfigRevision{}, err
	}

	now := time.Now().UTC()
	var out projection.ConfigRevision

	err = e.withTx(func(tx *sql.Tx) error {
		seq, err := projection.NextRevisionSequence(tx, family.ConfigFamilyID)
		if err != nil {
			return err
		}
		installationID, err := e.resolveInstallationTx(tx, app, req.InstallationID)
		if err != nil {
			return err
		}
		parent := req.Parent
		if parent == "" {
			if current, err := projection.CurrentRefRevision(tx, app.ApplicationID, family.ConfigFamilyID, installationID, RefActive); err == nil {
				parent = current
			}
		}

		revID := identity.NewID()
		schema := req.SchemaRevision
		if schema == "" {
			schema = family.SchemaRevision
		}

		obj := objectstore.Revision{
			RevisionID:     revID,
			ApplicationID:  app.ApplicationID,
			ConfigFamilyID: family.ConfigFamilyID,
			Sequence:       seq,
			RootTreeHash:   treeHash,
			ParentRevision: parent,
			SchemaRevision: schema,
			Label:          req.Label,
			Message:        req.Message,
			IssueIDs:       req.IssueIDs,
			WorkItemID:     req.WorkItemID,
			CandidateID:    req.CandidateID,
			Author:         req.Author,
			CreatedAt:      now,
		}
		contentHash, err := e.objects.PutRevision(obj)
		if err != nil {
			return err
		}

		rev := projection.ConfigRevision{
			RevisionID:       revID,
			ApplicationID:    app.ApplicationID,
			ConfigFamilyID:   family.ConfigFamilyID,
			Sequence:         seq,
			ContentHash:      contentHash,
			RootTreeHash:     treeHash,
			ParentRevisionID: parent,
			SchemaRevision:   schema,
			Label:            req.Label,
			Message:          req.Message,
			IssueIDs:         req.IssueIDs,
			WorkItemID:       req.WorkItemID,
			CandidateID:      req.CandidateID,
			Author:           req.Author,
			CreatedAt:        now,
		}

		rec, err := e.ledger.Append(ledger.KindRevision, revID, revisionPayload{
			Revision:   rev,
			Blobs:      tree.Entries,
			ObjectHash: contentHash,
		}, ledger.Durable)
		if err != nil {
			return err
		}

		if err := projection.InsertRevision(tx, rev, tree.Entries); err != nil {
			return err
		}
		if err := projection.IndexLedgerRecordTx(tx, rec); err != nil {
			return err
		}

		refNames := make([]string, 0, len(req.SetRefs))
		for name, set := range req.SetRefs {
			if set {
				refNames = append(refNames, name)
			}
		}
		for _, name := range refNames {
			expected := req.ExpectedRefs[name]
			if expected == nil {
				// nil means "create only": the ref must not exist yet.
				empty := ""
				expected = &empty
			}
			installationID, err := e.resolveInstallationTx(tx, app, req.InstallationID)
			if err != nil {
				return err
			}
			if err := e.moveRefTx(tx, refMove{
				ApplicationID:  app.ApplicationID,
				ConfigFamilyID: family.ConfigFamilyID,
				InstallationID: installationID,
				RefName:        name,
				NewRevision:    rev,
				Expected:       expected,
				Reason:         req.Message,
				IssueIDs:       req.IssueIDs,
				CandidateID:    req.CandidateID,
				Actor:          req.Author,
				At:             now,
			}); err != nil {
				return err
			}
		}
		if err := checkpoint(tx, rec); err != nil {
			return err
		}
		out = rev
		return nil
	})
	if err != nil {
		return projection.ConfigRevision{}, err
	}
	return out, nil
}

// BaselineRequest imports an application's existing configuration
// (section 27). The first join never overwrites anything: it reads reality,
// hashes it, and starts history from there.
type BaselineRequest struct {
	Application    string             `json:"application"`
	ConfigFamily   string             `json:"config_family"`
	InstallationID string             `json:"installation_id"`
	Bundle         objectstore.Bundle `json:"bundle"`
	Author         string             `json:"author"`
	Label          string             `json:"label"`
}

// ImportBaseline creates revision r1 and points active, approved, desired,
// deployed and last_good at it.
func (e *Engine) ImportBaseline(ctx context.Context, req BaselineRequest) (projection.ConfigRevision, error) {
	rev, err := e.CreateRevision(ctx, RevisionRequest{
		Application:    req.Application,
		ConfigFamily:   req.ConfigFamily,
		InstallationID: req.InstallationID,
		Bundle:         req.Bundle,
		Message:        "baseline import: configuration as found on disk",
		Label:          req.Label,
		Author:         req.Author,
		SetRefs: map[string]bool{
			RefActive: true, RefApproved: true, RefDesired: true,
			RefDeployed: true, RefLastGood: true,
		},
		ExpectedRefs: map[string]*string{
			RefActive: nil, RefApproved: nil, RefDesired: nil,
			RefDeployed: nil, RefLastGood: nil,
		},
	})
	if err != nil {
		return projection.ConfigRevision{}, err
	}
	if err := e.audit(ctx, projection.AuditEvent{
		Action:        "config.baseline_imported",
		Actor:         req.Author,
		ApplicationID: rev.ApplicationID,
		SubjectKind:   "revision",
		SubjectID:     rev.RevisionID,
		Detail:        fmt.Sprintf(`{"family":%q,"sequence":%d}`, rev.ConfigFamilyID, rev.Sequence),
	}); err != nil {
		return projection.ConfigRevision{}, err
	}
	return rev, nil
}

// CandidateRequest proposes a change without touching production
// (section 44, section 45).
type CandidateRequest struct {
	Application    string             `json:"application"`
	ConfigFamily   string             `json:"config_family"`
	InstallationID string             `json:"installation_id"`
	Bundle         objectstore.Bundle `json:"bundle"`
	BaseRevisionID string             `json:"base_revision_id"`
	// IssueIDs names every issue this candidate claims to address. A candidate
	// may fix many issues (section 29 of the test plan), and the claim travels
	// with it into the promoted revision.
	IssueIDs      []string `json:"issue_ids"`
	WorkItemID    string   `json:"work_item_id"`
	WorkflowRunID string   `json:"workflow_run_id"`
	Explanation   string   `json:"explanation"`
}

// CreateCandidate validates structure, stores the bundle, and records a
// candidate in state DRAFT. It never writes to a managed target.
func (e *Engine) CreateCandidate(ctx context.Context, req CandidateRequest) (projection.Candidate, error) {
	app, family, err := e.resolveFamily(ctx, req.Application, req.ConfigFamily)
	if err != nil {
		return projection.Candidate{}, err
	}
	if len(req.Bundle) == 0 {
		return projection.Candidate{}, errors.New("candidate requires a non-empty bundle")
	}

	// An unparseable artifact is rejected here, before it can become a
	// candidate and before any test workflow is asked to evaluate it
	// (section 32). The rejection is audited so the attempt is not invisible.
	if err := ValidateBundleStructure(req.Bundle); err != nil {
		if auditErr := e.audit(ctx, projection.AuditEvent{
			Action:        "candidate.rejected",
			Actor:         req.WorkflowRunID,
			ApplicationID: req.Application,
			SubjectKind:   "config_family",
			SubjectID:     req.ConfigFamily,
			Detail:        fmt.Sprintf(`{"reason":%q}`, err.Error()),
		}); auditErr != nil {
			e.log.Warn("could not audit rejected candidate", "error", auditErr)
		}
		return projection.Candidate{}, err
	}

	treeHash, err := e.objects.PutBundle(req.Bundle)
	if err != nil {
		return projection.Candidate{}, err
	}
	tree, err := e.objects.GetTree(treeHash)
	if err != nil {
		return projection.Candidate{}, err
	}
	now := time.Now().UTC()
	var out projection.Candidate

	err = e.withTx(func(tx *sql.Tx) error {
		plan, err := e.resolveCandidatePlanTx(tx, app, candidatePlan{
			ApplicationID:  app.ApplicationID,
			ConfigFamilyID: family.ConfigFamilyID,
			InstallationID: req.InstallationID,
			Bundle:         req.Bundle,
			TreeHash:       treeHash,
			Blobs:          tree.Entries,
			BaseRevisionID: req.BaseRevisionID,
			IssueIDs:       req.IssueIDs,
			WorkItemID:     req.WorkItemID,
			WorkflowRunID:  req.WorkflowRunID,
			Explanation:    req.Explanation,
		})
		if err != nil {
			return err
		}
		cand := buildCandidate(plan, "", now)

		rec, err := e.ledger.Append(ledger.KindCandidate, cand.CandidateID, candidatePayload{
			Candidate: cand,
			FromState: "",
			Reason:    "candidate created",
			At:        now,
		}, ledger.Durable)
		if err != nil {
			return err
		}
		if err := projection.InsertCandidate(tx, cand); err != nil {
			return err
		}
		if err := e.moveRefTx(tx, refMove{
			ApplicationID:    app.ApplicationID,
			ConfigFamilyID:   family.ConfigFamilyID,
			InstallationID:   cand.InstallationID,
			RefName:          CandidateRefName(cand.CandidateID),
			RevisionRootTree: treeHash,
			Reason:           "candidate branch created",
			CandidateID:      cand.CandidateID,
			At:               now,
		}); err != nil {
			return err
		}
		if err := projection.IndexLedgerRecordTx(tx, rec); err != nil {
			return err
		}
		if err := checkpoint(tx, rec); err != nil {
			return err
		}
		out = cand
		return nil
	})
	if err != nil {
		return projection.Candidate{}, err
	}
	return out, nil
}

// ValidationRequest records a test workflow result (section 48).
type ValidationRequest struct {
	CandidateID  string `json:"candidate_id"`
	Kind         string `json:"kind"`
	Result       string `json:"result"`
	SuiteID      string `json:"suite_id"`
	SuiteVersion string `json:"suite_version"`
	InputsHash   string `json:"inputs_hash"`
	Environment  string `json:"environment"`
	Worker       string `json:"worker"`
	Metrics      string `json:"metrics"`
	Artifacts    string `json:"artifacts"`
}

// RecordValidation indexes a validation result and advances the candidate
// state machine accordingly (section 47, section 49, section 94).
func (e *Engine) RecordValidation(ctx context.Context, req ValidationRequest) (projection.Candidate, error) {
	if req.CandidateID == "" {
		return projection.Candidate{}, errors.New("validation requires a candidate")
	}
	if req.Result == "" {
		return projection.Candidate{}, errors.New("validation requires a result")
	}
	now := time.Now().UTC()
	var out projection.Candidate

	err := e.withTx(func(tx *sql.Tx) error {
		cand, err := projection.GetCandidateTx(tx, req.CandidateID)
		if err != nil {
			return err
		}
		family, err := projection.GetConfigFamilyTx(tx, cand.ConfigFamilyID)
		if err != nil {
			return err
		}

		v := projection.Validation{
			ValidationID:   identity.NewID(),
			CandidateID:    cand.CandidateID,
			ApplicationID:  cand.ApplicationID,
			ConfigFamilyID: cand.ConfigFamilyID,
			SuiteID:        req.SuiteID,
			SuiteVersion:   req.SuiteVersion,
			Kind:           req.Kind,
			Result:         req.Result,
			Metrics:        req.Metrics,
			Artifacts:      req.Artifacts,
			InputsHash:     req.InputsHash,
			Environment:    req.Environment,
			Worker:         req.Worker,
			CreatedAt:      now,
		}

		// Evidence first, then the index: a validation result is canonical.
		// Losing it in a rebuild would mean losing the proof that a promotion
		// was justified (section 48, section 72).
		rec, err := e.ledger.Append(ledger.KindValidation, v.ValidationID, validationPayload{Validation: v}, ledger.Durable)
		if err != nil {
			return err
		}
		if err := projection.InsertValidation(tx, v); err != nil {
			return err
		}
		if err := projection.IndexLedgerRecordTx(tx, rec); err != nil {
			return err
		}
		if err := checkpoint(tx, rec); err != nil {
			return err
		}

		next := nextCandidateState(CandidateState(cand.State), req.Kind, req.Result, family.RequiresHoldout)
		if next == "" || next == CandidateState(cand.State) {
			out = cand
			return nil
		}
		if err := CheckCandidateTransition(CandidateState(cand.State), next); err != nil {
			return err
		}
		if err := projection.SetCandidateState(tx, cand.CandidateID, string(next), now); err != nil {
			return err
		}
		cand.State = string(next)
		cand.UpdatedAt = now
		if err := e.recordCandidateStateTx(tx, cand, CandidateState(cand.State), next, req.Kind+"="+req.Result, now); err != nil {
			return err
		}
		out = cand
		return nil
	})
	if err != nil {
		return projection.Candidate{}, err
	}
	return out, nil
}

// nextCandidateState maps a validation result onto the candidate state machine.
//
// A family that declares requires_holdout cannot reach PASSED without a
// holdout result (section 49).
func nextCandidateState(current CandidateState, kind, result string, requiresHoldout bool) CandidateState {
	passed := result == "PASS" || result == "PASSED" || result == "OK"
	if !passed {
		switch current {
		case CandidateRejected, CandidateSuperseded, CandidateActive:
			return ""
		default:
			return CandidateRejected
		}
	}
	switch kind {
	case "structural", "schema":
		if current == CandidateDraft {
			return CandidateStructurallyOK
		}
		return ""
	case "holdout":
		// A holdout run is what lets a holdout-requiring family reach PASSED
		// (section 49).
		if current == CandidateTesting || current == CandidateStructurallyOK {
			return CandidatePassed
		}
		return ""
	default:
		// The state machine from section 45 runs DRAFT -> STRUCTURALLY_VALID ->
		// TESTING -> PASSED. A family that requires a holdout stays in TESTING
		// until a holdout result arrives.
		switch current {
		case CandidateStructurallyOK:
			return CandidateTesting
		case CandidateTesting:
			if requiresHoldout {
				return ""
			}
			return CandidatePassed
		}
		return ""
	}
}

// ApprovalRequest records the human decision to promote (section 50).
type ApprovalRequest struct {
	CandidateID string `json:"candidate_id"`
	Approver    string `json:"approver"`
	Comment     string `json:"comment"`
	// ActorType separates a human decision from a machine one. Only HUMAN
	// approvals authorise delivery, and an unlabelled request is treated as
	// SYSTEM: unknown origin must never be read as human (mission section 20).
	ActorType string `json:"actor_type,omitempty"`
	// PeerUID is the OS uid that made the decision, when the transport knows it.
	PeerUID int `json:"-"`
	// Decision is APPROVED, REJECTED or DEFERRED.
	Decision string `json:"decision,omitempty"`
	// InstallationID narrows the approval to one deployment. An approval is one
	// application, one installation, one family and one exact revision: the
	// daemon refuses to approve "all installations" implicitly
	// (mission sections 47, 48).
	InstallationID string `json:"installation_id,omitempty"`
}

// actorTypeOrDefault treats an unlabelled decision as SYSTEM, never HUMAN.
func actorTypeOrDefault(actorType string) string {
	if actorType == projection.ActorHuman {
		return projection.ActorHuman
	}
	return projection.ActorSystem
}

// decisionOrDefault defaults to APPROVED: the endpoint that reaches this code
// is the approval endpoint.
func decisionOrDefault(decision string) string {
	switch decision {
	case "REJECTED", "DEFERRED":
		return decision
	default:
		return "APPROVED"
	}
}

// ApproveCandidate moves a PASSED candidate to APPROVED. Manual approval is
// required for production promotion (section 50, section 102).
func (e *Engine) ApproveCandidate(ctx context.Context, req ApprovalRequest) (projection.Candidate, error) {
	if req.Approver == "" {
		return projection.Candidate{}, errors.New("approval requires an approver")
	}
	now := time.Now().UTC()
	var out projection.Candidate

	err := e.withTx(func(tx *sql.Tx) error {
		cand, err := projection.GetCandidateTx(tx, req.CandidateID)
		if err != nil {
			return err
		}
		// A decision may be revised while the candidate is still awaiting
		// delivery, including after promotion: an operator who defers an
		// already-approved revision is saying "do not hand this to the node
		// yet", which is a decision, not a withdrawal. It may not be revised
		// after rejection, which needs a new candidate (mission sections 4, 46).
		switch CandidateState(cand.State) {
		case CandidatePassed, CandidateApproved, CandidateDeployable:
		default:
			return fmt.Errorf("candidate %s is %s: only PASSED candidates can be approved", cand.CandidateID, cand.State)
		}

		decision := decisionOrDefault(req.Decision)
		installationID := req.InstallationID
		if installationID == "" {
			installationID = cand.InstallationID
		}
		// One approval authorises one installation. If neither the candidate nor
		// the operator names one, the approval would have to cover every
		// deployment of the application, which is exactly what this stage
		// refuses to do implicitly (mission sections 47, 48).
		if installationID == "" {
			return fmt.Errorf("%w: approval must name one installation", ErrAmbiguousInstallation)
		}

		// The state a decision implies. A deferral deliberately changes nothing:
		// it is a decision recorded, not a lifecycle move, and the delivery gate
		// reads the latest decision anyway (mission section 46).
		target := CandidateState(cand.State)
		switch decision {
		case "APPROVED":
			// Only a candidate that has not been approved yet moves state; one
			// that was already promoted stays DEPLOYABLE, because the revision it
			// produced is already immutable.
			if CandidateState(cand.State) == CandidatePassed {
				target = CandidateApproved
			}
		case "REJECTED":
			target = CandidateRejected
		}
		if err := CheckCandidateTransition(CandidateState(cand.State), target); err != nil {
			return err
		}
		approval := projection.Approval{
			ApprovalID:     identity.NewID(),
			CandidateID:    cand.CandidateID,
			Approver:       req.Approver,
			ActorType:      actorTypeOrDefault(req.ActorType),
			Decision:       decision,
			Comment:        req.Comment,
			ApplicationID:  cand.ApplicationID,
			InstallationID: installationID,
			ConfigFamilyID: cand.ConfigFamilyID,
			// The content address the node will verify for itself. Binding to
			// anything mutable would let one approval authorise different
			// content (mission sections 3, 4).
			ContentHash:    cand.RootTreeHash,
			BaseRevisionID: cand.BaseRevisionID,
			PeerUID:        req.PeerUID,
			CreatedAt:      now,
		}
		// The validations the decision is made on the strength of.
		if validations, err := e.validationsTx(tx, cand.CandidateID); err == nil {
			for _, validation := range validations {
				approval.ValidationIDs = append(approval.ValidationIDs, validation.ValidationID)
			}
		}
		rec, err := e.ledger.Append(ledger.KindApproval, approval.ApprovalID, approvalPayload{Approval: approval}, ledger.Durable)
		if err != nil {
			return err
		}
		if err := projection.InsertApproval(tx, approval); err != nil {
			return err
		}
		if err := projection.IndexLedgerRecordTx(tx, rec); err != nil {
			return err
		}
		if err := checkpoint(tx, rec); err != nil {
			return err
		}
		if target != CandidateState(cand.State) {
			if err := projection.SetCandidateState(tx, cand.CandidateID, string(target), now); err != nil {
				return err
			}
			previous := cand.State
			cand.State = string(target)
			cand.UpdatedAt = now
			if err := e.recordCandidateStateTx(tx, cand, CandidateState(previous), target, req.Comment, now); err != nil {
				return err
			}
		}
		out = cand
		return nil
	})
	if err != nil {
		return projection.Candidate{}, err
	}
	return out, nil
}

// PromotionResult reports what promotion did.
type PromotionResult struct {
	Revision  projection.ConfigRevision `json:"revision"`
	Candidate projection.Candidate      `json:"candidate"`
	Refs      []projection.ConfigRef    `json:"refs"`
}

// PromoteCandidate turns an approved candidate into an immutable revision and
// moves the active ref with compare-and-swap (section 51, section 22).
//
// The stale-base check is the point of the whole exercise: if production moved
// from r42 to r43 while this candidate was being tested, the candidate is
// marked STALE and nothing is overwritten.
func (e *Engine) PromoteCandidate(ctx context.Context, candidateID, actor string) (PromotionResult, error) {
	now := time.Now().UTC()
	var out PromotionResult
	// staleErr is raised after the transaction commits: marking the candidate
	// STALE is itself a fact worth keeping, so it must not be rolled back along
	// with the refused promotion (section 81).
	var staleErr error

	err := e.withTx(func(tx *sql.Tx) error {
		cand, err := projection.GetCandidateTx(tx, candidateID)
		if err != nil {
			return err
		}
		if CandidateState(cand.State) != CandidateApproved {
			return fmt.Errorf("candidate %s is %s: only APPROVED candidates can be promoted", cand.CandidateID, cand.State)
		}
		family, err := projection.GetConfigFamilyTx(tx, cand.ConfigFamilyID)
		if err != nil {
			return err
		}
		app, err := projection.GetApplicationTx(tx, cand.ApplicationID)
		if err != nil {
			return err
		}

		// Staleness is judged against this candidate's own installation. A
		// candidate built on Mac's r42 is not stale because Region B is on r40
		// (mission sections 57, 94).
		installationID, err := e.resolveInstallationTx(tx, app, cand.InstallationID)
		if err != nil {
			return err
		}
		current, err := projection.CurrentRefRevision(tx, cand.ApplicationID, cand.ConfigFamilyID, installationID, RefActive)
		if err != nil {
			return err
		}
		if current != cand.BaseRevisionID {
			// Preserve the candidate; never silently reapply (section 81).
			if err := projection.SetCandidateState(tx, cand.CandidateID, string(CandidateStale), now); err != nil {
				return err
			}
			stale := cand
			stale.State = string(CandidateStale)
			stale.UpdatedAt = now
			if err := e.recordCandidateStateTx(tx, stale, CandidateState(cand.State), CandidateStale,
				fmt.Sprintf("active moved from %s to %s", cand.BaseRevisionID, current), now); err != nil {
				return err
			}
			staleErr = fmt.Errorf("%w: candidate base %s, active %s", projection.ErrStaleCandidate,
				cand.BaseRevisionID, current)
			return nil
		}

		tree, err := e.objects.GetTree(cand.RootTreeHash)
		if err != nil {
			return err
		}
		seq, err := projection.NextRevisionSequence(tx, family.ConfigFamilyID)
		if err != nil {
			return err
		}
		revID := identity.NewID()
		obj := objectstore.Revision{
			RevisionID:     revID,
			ApplicationID:  app.ApplicationID,
			ConfigFamilyID: family.ConfigFamilyID,
			Sequence:       seq,
			RootTreeHash:   cand.RootTreeHash,
			ParentRevision: cand.BaseRevisionID,
			SchemaRevision: family.SchemaRevision,
			IssueIDs:       cand.IssueIDs,
			Message:        cand.Explanation,
			CandidateID:    cand.CandidateID,
			WorkItemID:     cand.WorkItemID,
			Author:         actor,
			CreatedAt:      now,
		}
		contentHash, err := e.objects.PutRevision(obj)
		if err != nil {
			return err
		}
		rev := projection.ConfigRevision{
			RevisionID:       revID,
			ApplicationID:    app.ApplicationID,
			ConfigFamilyID:   family.ConfigFamilyID,
			Sequence:         seq,
			ContentHash:      contentHash,
			RootTreeHash:     cand.RootTreeHash,
			ParentRevisionID: cand.BaseRevisionID,
			SchemaRevision:   family.SchemaRevision,
			IssueIDs:         cand.IssueIDs,
			Message:          cand.Explanation,
			CandidateID:      cand.CandidateID,
			WorkItemID:       cand.WorkItemID,
			Author:           actor,
			CreatedAt:        now,
		}

		rec, err := e.ledger.Append(ledger.KindRevision, revID, revisionPayload{
			Revision:   rev,
			Blobs:      tree.Entries,
			ObjectHash: contentHash,
		}, ledger.Durable)
		if err != nil {
			return err
		}
		if err := projection.InsertRevision(tx, rev, tree.Entries); err != nil {
			return err
		}
		if err := projection.IndexLedgerRecordTx(tx, rec); err != nil {
			return err
		}
		if err := checkpoint(tx, rec); err != nil {
			return err
		}

		expectedBase := cand.BaseRevisionID
		// `active` is the safety anchor: it must still point at the revision
		// this candidate was built on, or the candidate is stale (section 22,
		// section 51).
		if err := e.moveRefTx(tx, refMove{
			ApplicationID:  app.ApplicationID,
			ConfigFamilyID: family.ConfigFamilyID,
			InstallationID: installationID,
			RefName:        RefActive,
			NewRevision:    rev,
			Expected:       &expectedBase,
			Reason:         "candidate promoted",
			IssueIDs:       cand.IssueIDs,
			CandidateID:    cand.CandidateID,
			Actor:          actor,
			At:             now,
		}); err != nil {
			return err
		}
		// `approved` and `desired` follow the promotion. They are deliberately
		// not compared against the candidate's base: a store whose history was
		// imported may have them lagging `active`, and that is accurate rather
		// than corrupt.
		for _, name := range []string{RefApproved, RefDesired} {
			current, err := projection.CurrentRefRevision(tx, app.ApplicationID, family.ConfigFamilyID, installationID, name)
			if err != nil {
				return err
			}
			expectedCurrent := current
			if err := e.moveRefTx(tx, refMove{
				ApplicationID:  app.ApplicationID,
				ConfigFamilyID: family.ConfigFamilyID,
				InstallationID: installationID,
				RefName:        name,
				NewRevision:    rev,
				Expected:       &expectedCurrent,
				Reason:         "candidate promoted",
				IssueIDs:       cand.IssueIDs,
				CandidateID:    cand.CandidateID,
				Actor:          actor,
				At:             now,
			}); err != nil {
				return err
			}
		}
		if err := projection.SetCandidateState(tx, cand.CandidateID, string(CandidateDeployable), now); err != nil {
			return err
		}
		cand.State = string(CandidateDeployable)
		cand.UpdatedAt = now
		if err := e.recordCandidateStateTx(tx, cand, CandidateApproved, CandidateDeployable, "promoted", now); err != nil {
			return err
		}

		out = PromotionResult{Revision: rev, Candidate: cand}
		return nil
	})
	if err != nil {
		return PromotionResult{}, err
	}
	if staleErr != nil {
		return PromotionResult{}, staleErr
	}
	// Read the refs after the transaction commits: the promotion holds the
	// single write connection, so a pooled query inside the transaction would
	// wait on itself.
	refs, err := e.db.ListRefs(out.Revision.ApplicationID, out.Revision.ConfigFamilyID, out.Candidate.InstallationID)
	if err != nil {
		return PromotionResult{}, err
	}
	out.Refs = refs
	return out, nil
}

// SetRefRequest moves one ref with compare-and-swap (section 22).
type SetRefRequest struct {
	Application    string   `json:"application"`
	ConfigFamily   string   `json:"config_family"`
	InstallationID string   `json:"installation_id"`
	RefName        string   `json:"ref_name"`
	RevisionID     string   `json:"revision_id"`
	ExpectedOld    *string  `json:"expected_old_revision_id"`
	Reason         string   `json:"reason"`
	Actor          string   `json:"actor"`
	IssueIDs       []string `json:"issue_ids"`
	CandidateID    string   `json:"candidate_id"`
	ValidationID   string   `json:"validation_id"`
	ApprovalID     string   `json:"approval_id"`
	DeploymentID   string   `json:"deployment_id"`
}

// SetRef moves a ref to a revision, refusing the move when the ref is not where
// the caller believed it was.
func (e *Engine) SetRef(ctx context.Context, req SetRefRequest) (projection.ConfigRef, error) {
	app, family, err := e.resolveFamily(ctx, req.Application, req.ConfigFamily)
	if err != nil {
		return projection.ConfigRef{}, err
	}
	if req.RefName == "" || req.RevisionID == "" {
		return projection.ConfigRef{}, errors.New("ref move requires a ref name and a revision")
	}
	now := time.Now().UTC()
	var out projection.ConfigRef

	err = e.withTx(func(tx *sql.Tx) error {
		rev, err := projection.GetRevisionTx(tx, req.RevisionID)
		if err != nil {
			return err
		}
		installationID, err := e.resolveInstallationTx(tx, app, req.InstallationID)
		if err != nil {
			return err
		}
		if err := e.moveRefTx(tx, refMove{
			ApplicationID:  app.ApplicationID,
			ConfigFamilyID: family.ConfigFamilyID,
			InstallationID: installationID,
			RefName:        req.RefName,
			NewRevision:    rev,
			Expected:       req.ExpectedOld,
			Reason:         req.Reason,
			IssueIDs:       req.IssueIDs,
			CandidateID:    req.CandidateID,
			ValidationID:   req.ValidationID,
			ApprovalID:     req.ApprovalID,
			DeploymentID:   req.DeploymentID,
			Actor:          req.Actor,
			At:             now,
		}); err != nil {
			return err
		}
		out, err = projection.GetRefTx(tx, app.ApplicationID, family.ConfigFamilyID, installationID, req.RefName)
		return err
	})
	if err != nil {
		return projection.ConfigRef{}, err
	}
	return out, nil
}

// refMove is the internal description of one ref movement.
type refMove struct {
	ApplicationID    string
	ConfigFamilyID   string
	InstallationID   string `json:"installation_id"`
	RefName          string `json:"ref_name"`
	NewRevision      projection.ConfigRevision
	RevisionRootTree string
	Expected         *string
	Reason           string   `json:"reason"`
	IssueIDs         []string `json:"issue_ids"`
	CandidateID      string   `json:"candidate_id"`
	ValidationID     string   `json:"validation_id"`
	ApprovalID       string   `json:"approval_id"`
	DeploymentID     string   `json:"deployment_id"`
	Actor            string   `json:"actor"`
	At               time.Time
}

// moveRefTx performs the compare-and-swap, writes the reflog and journals both.
func (e *Engine) moveRefTx(tx *sql.Tx, m refMove) error {
	if m.InstallationID == "" {
		// Every move is scoped explicitly before it reaches here; the low-level
		// writer never guesses which installation is affected (mission 52).
		return fmt.Errorf("ref move for %s/%s/%s has no installation", m.ApplicationID, m.ConfigFamilyID, m.RefName)
	}
	old, err := projection.CurrentRefRevision(tx, m.ApplicationID, m.ConfigFamilyID, m.InstallationID, m.RefName)
	if err != nil {
		return err
	}
	ref := projection.ConfigRef{
		ApplicationID:  m.ApplicationID,
		ConfigFamilyID: m.ConfigFamilyID,
		InstallationID: m.InstallationID,
		Name:           m.RefName,
		RevisionID:     m.NewRevision.RevisionID,
		ContentHash:    m.NewRevision.ContentHash,
		UpdatedAt:      m.At,
	}
	if ref.RevisionID == "" {
		// Candidate branches point at a tree rather than a revision.
		ref.RevisionID = m.RevisionRootTree
		ref.ContentHash = m.RevisionRootTree
	}
	if err := projection.SetRefTx(tx, ref, m.Expected); err != nil {
		return err
	}
	entry := projection.ReflogEntry{
		LogID:          identity.NewID(),
		ApplicationID:  m.ApplicationID,
		ConfigFamilyID: m.ConfigFamilyID,
		InstallationID: m.InstallationID,
		RefName:        m.RefName,
		OldRevisionID:  old,
		NewRevisionID:  ref.RevisionID,
		Reason:         m.Reason,
		IssueIDs:       m.IssueIDs,
		CandidateID:    m.CandidateID,
		ValidationID:   m.ValidationID,
		ApprovalID:     m.ApprovalID,
		DeploymentID:   m.DeploymentID,
		CreatedAt:      m.At,
	}

	rec, err := e.ledger.Append(ledger.KindRef, entry.LogID, refPayload{Ref: ref, Reflog: entry}, ledger.Durable)
	if err != nil {
		return err
	}
	entry.LedgerSequence = rec.Sequence
	if err := projection.InsertReflog(tx, entry); err != nil {
		return err
	}
	return projection.IndexLedgerRecordTx(tx, rec)
}

// resolveFamily turns an application and config family reference (UUID or name)
// into their identities.
// ResolveFamilyID turns a config family reference — a UUID or, conveniently, a
// name — into the UUID that is the actual identity, scoped to one application.
//
// Names are metadata (design section 4), so nothing may be stored by name; but
// an application naturally has a name in hand when it asks what it may take, and
// making it carry UUIDs around would push identity bookkeeping into application
// code for no safety gain.
func (e *Engine) ResolveFamilyID(ctx context.Context, applicationRef, familyRef string) (string, error) {
	_, family, err := e.resolveFamily(ctx, applicationRef, familyRef)
	if err != nil {
		return "", err
	}
	return family.ConfigFamilyID, nil
}

func (e *Engine) resolveFamily(ctx context.Context, appRef, familyRef string) (projection.Application, projection.ConfigFamily, error) {
	var app projection.Application
	var err error
	if identity.Valid(appRef) {
		app, err = e.db.GetApplication(appRef)
	} else {
		app, err = e.db.GetApplicationByName(appRef)
	}
	if err != nil {
		return app, projection.ConfigFamily{}, err
	}

	var family projection.ConfigFamily
	if identity.Valid(familyRef) {
		family, err = e.db.GetConfigFamily(familyRef)
	} else {
		family, err = e.db.GetConfigFamilyByName(app.ApplicationID, familyRef)
	}
	if err != nil {
		return app, family, err
	}
	if family.ApplicationID != app.ApplicationID {
		return app, family, fmt.Errorf("config family %s belongs to another application", family.Name)
	}
	return app, family, nil
}

// audit writes a standalone audit record (section 95).
func (e *Engine) audit(ctx context.Context, ev projection.AuditEvent) error {
	if ev.AuditID == "" {
		ev.AuditID = identity.NewID()
	}
	ev.CreatedAt = time.Now().UTC()
	return e.withTx(func(tx *sql.Tx) error {
		return appendAuditTx(e, tx, ev, ledger.Durable)
	})
}
