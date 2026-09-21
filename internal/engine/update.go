package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/ledger"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
)

// Approved update delivery.
//
// The loop is a pull: Lymph says an exact revision is eligible for an exact
// installation; the node decides whether to take it, verifies the bytes itself,
// applies them with its own logic, and reports the outcome. Lymph never writes
// to an application's filesystem (mission sections 16, 17, 30).
//
// The one invariant everything here exists to protect:
//
//	NO NEW IMPROVEMENT MAY ENTER THE DELIVERY CHANNEL WITHOUT AN EXPLICIT HUMAN
//	APPROVAL FOR THAT EXACT IMMUTABLE CONTENT.

// ErrNotDeliverable is returned when a revision is not eligible for an
// installation. The reason code says which condition failed, so an operator can
// fix the right thing (mission section 19).
type ErrNotDeliverable struct {
	ReasonCode string
	Message    string
}

func (e *ErrNotDeliverable) Error() string {
	return fmt.Sprintf("DELIVERY_NOT_AUTHORIZED (%s): %s", e.ReasonCode, e.Message)
}

// Delivery reason codes.
const (
	NotFamilyNotFound     = "CONFIG_FAMILY_UNKNOWN"
	NotFamilyExternal     = "CONFIG_FAMILY_EXTERNAL"
	NotRevisionUnknown    = "REVISION_UNKNOWN"
	NotFamilyMismatch     = "REVISION_FAMILY_MISMATCH"
	NotNoApproval         = "NO_APPROVAL"
	NotApprovalNotHuman   = "APPROVAL_NOT_HUMAN"
	NotApprovalRejected   = "APPROVAL_DECISION_NOT_APPROVED"
	NotApprovalTarget     = "APPROVAL_INSTALLATION_MISMATCH"
	NotApprovalContent    = "APPROVAL_CONTENT_HASH_MISMATCH"
	NotApprovalRevision   = "APPROVAL_REVISION_MISMATCH"
	NotValidationMissing  = "REQUIRED_VALIDATION_MISSING"
	NotValidationFailed   = "REQUIRED_VALIDATION_FAILED"
	NotWithdrawn          = "UPDATE_WITHDRAWN"
	NotPreviouslyRejected = "UPDATE_PREVIOUSLY_REJECTED"
	NotBaseMismatch       = "BASE_REVISION_MISMATCH"
)

// Eligibility is the answer to "may this node take this revision?", including
// the parts that are warnings rather than refusals.
type Eligibility struct {
	Deliverable bool   `json:"deliverable"`
	ReasonCode  string `json:"reason_code,omitempty"`
	Message     string `json:"message,omitempty"`

	Approval       projection.Approval          `json:"approval,omitempty"`
	Revision       projection.ConfigRevision    `json:"revision,omitempty"`
	Family         projection.ConfigFamily      `json:"family,omitempty"`
	Disposition    projection.UpdateDisposition `json:"disposition,omitempty"`
	HasDisposition bool                         `json:"has_disposition"`

	// BaseCompatible reports whether the node's currently observed revision is
	// the base this revision was built on. An update built on r42 offered to a
	// node on r40 is not delivered by default (mission section 55).
	BaseCompatible bool   `json:"base_compatible"`
	BaseReasonCode string `json:"base_reason_code,omitempty"`
	ObservedAt     string `json:"observed_revision_id,omitempty"`
	// SkippedRevisions are the revisions between the node's observed revision
	// and this revision's base that the node has already refused, or that an
	// operator withdrew. See the base-compatibility rule below.
	SkippedRevisions []string `json:"skipped_revisions,omitempty"`
}

// requiredValidationKinds are the gates a candidate must have passed before its
// revision may be delivered. They mirror the promotion pipeline.
var requiredValidationKinds = []string{"structural", "replay", "regression"}

// RequiredValidationKinds reports the validation kinds a revision must have
// passed before it may be delivered. It is exported so a review package can say
// what is still missing instead of leaving a human to guess
// (mission sections 19, 43).
func RequiredValidationKinds() []string {
	return append([]string(nil), requiredValidationKinds...)
}

// IsRevisionDeliverableTx is the single place that decides whether an approved
// revision may be handed to a node. Every delivery path calls it; no handler
// re-implements it (mission section 19).
func (e *Engine) IsRevisionDeliverableTx(
	tx *sql.Tx,
	applicationID, installationID, configFamilyID, revisionID string,
	observedRevisionID string,
) (Eligibility, error) {
	result := Eligibility{}

	// The family must exist and must be explicitly managed. An EXTERNAL family
	// is never delivered, which is the management boundary that keeps
	// credentials and tokens out of Lymph's reach (mission sections 5, 6).
	family, err := projection.GetConfigFamilyTx(tx, configFamilyID)
	if err != nil {
		if errors.Is(err, projection.ErrNotFound) {
			return result, &ErrNotDeliverable{ReasonCode: NotFamilyNotFound, Message: configFamilyID}
		}
		return result, err
	}
	result.Family = family
	if family.ManagementMode != projection.ManagementLymphManaged {
		return result, &ErrNotDeliverable{
			ReasonCode: NotFamilyExternal,
			Message:    fmt.Sprintf("config family %s is %s, not LYMPH_MANAGED", family.Name, family.ManagementMode),
		}
	}
	if family.ApplicationID != applicationID {
		return result, &ErrNotDeliverable{ReasonCode: NotFamilyMismatch,
			Message: fmt.Sprintf("family %s belongs to another application", family.Name)}
	}

	revision, err := projection.GetRevisionTx(tx, revisionID)
	if err != nil {
		if errors.Is(err, projection.ErrNotFound) {
			return result, &ErrNotDeliverable{ReasonCode: NotRevisionUnknown, Message: revisionID}
		}
		return result, err
	}
	result.Revision = revision
	if revision.ConfigFamilyID != configFamilyID {
		return result, &ErrNotDeliverable{ReasonCode: NotFamilyMismatch,
			Message: fmt.Sprintf("revision %s belongs to family %s", revisionID, revision.ConfigFamilyID)}
	}

	// A human approval, bound to this exact content and this exact installation.
	if revision.CandidateID == "" {
		return result, &ErrNotDeliverable{ReasonCode: NotNoApproval,
			Message: "revision was not produced by an approved candidate"}
	}
	approval, err := projection.LatestApprovalTx(tx, revision.CandidateID)
	if err != nil {
		if errors.Is(err, projection.ErrNotFound) {
			return result, &ErrNotDeliverable{ReasonCode: NotNoApproval,
				Message: fmt.Sprintf("candidate %s has no approval", revision.CandidateID)}
		}
		return result, err
	}
	result.Approval = approval

	if approval.ActorType != projection.ActorHuman {
		return result, &ErrNotDeliverable{ReasonCode: NotApprovalNotHuman,
			Message: fmt.Sprintf("approval %s is %s, not HUMAN", approval.ApprovalID, approval.ActorType)}
	}
	if approval.Decision != "APPROVED" {
		return result, &ErrNotDeliverable{ReasonCode: NotApprovalRejected,
			Message: fmt.Sprintf("approval %s decided %s", approval.ApprovalID, approval.Decision)}
	}
	if approval.InstallationID != "" && approval.InstallationID != installationID {
		return result, &ErrNotDeliverable{ReasonCode: NotApprovalTarget,
			Message: fmt.Sprintf("approval %s authorises installation %s, not %s",
				approval.ApprovalID, approval.InstallationID, installationID)}
	}
	if approval.ApplicationID != "" && approval.ApplicationID != applicationID {
		return result, &ErrNotDeliverable{ReasonCode: NotApprovalTarget,
			Message: "approval authorises another application"}
	}
	if approval.ConfigFamilyID != "" && approval.ConfigFamilyID != configFamilyID {
		return result, &ErrNotDeliverable{ReasonCode: NotApprovalTarget,
			Message: "approval authorises another config family"}
	}
	// The content hash is part of authorisation: an approval for a hash that no
	// longer matches the revision cannot be reused (mission sections 3, 4).
	// The approval binds to a content address: the tree the node will fetch and
	// hash for itself. Comparing against the revision *object* hash instead
	// would authorise nothing meaningful.
	if approval.ContentHash != "" && approval.ContentHash != revision.RootTreeHash {
		return result, &ErrNotDeliverable{ReasonCode: NotApprovalContent,
			Message: fmt.Sprintf("approval authorises %s, revision is %s",
				approval.ContentHash, revision.RootTreeHash)}
	}

	// Required validations must exist and have passed. Read through the
	// transaction: the daemon holds a single connection.
	validations, err := e.validationsTx(tx, revision.CandidateID)
	if err != nil {
		return result, err
	}
	passed := map[string]bool{}
	for _, validation := range validations {
		if validation.Result == "PASS" || validation.Result == "PASSED" || validation.Result == "OK" {
			passed[validation.Kind] = true
		}
	}
	for _, kind := range requiredValidationKinds {
		if !passed[kind] {
			return result, &ErrNotDeliverable{ReasonCode: NotValidationMissing,
				Message: fmt.Sprintf("candidate %s has no passing %s validation", revision.CandidateID, kind)}
		}
	}

	// Withdrawal and per-installation disposition.
	if disposition, err := projection.GetUpdateDispositionTx(tx, applicationID, installationID, revisionID); err == nil {
		result.Disposition = disposition
		result.HasDisposition = true
		switch disposition.State {
		case projection.UpdateWithdrawn:
			return result, &ErrNotDeliverable{ReasonCode: NotWithdrawn,
				Message: "this installation's update was withdrawn by an operator"}
		case projection.UpdateRejected:
			// A rejected revision is not offered again until something changes:
			// an operator retry, a new approval, or a newer revision
			// (mission section 39).
			return result, &ErrNotDeliverable{ReasonCode: NotPreviouslyRejected,
				Message: fmt.Sprintf("this installation rejected %s (%s)", revisionID, disposition.ReasonCode)}
		}
	} else if !errors.Is(err, projection.ErrNotFound) {
		return result, err
	}

	// Base compatibility: the node's observed revision should be the base this
	// revision was built on.
	//
	// With one exception, and the exception is the whole reason this is not a
	// one-line comparison. A node that refused a revision will never apply it —
	// that is what a refusal means — and delivery of that revision is closed. If
	// a later revision is built on top of it (promotion is compare-and-swap, so
	// it must be), then requiring the refused revision as the base would require
	// a state the node cannot reach, and the delivery channel would deadlock
	// after the first rejection. So a chain of revisions that the node has
	// already refused, or that an operator has withdrawn, may be skipped. Only
	// revisions the node could still apply block the jump (mission sections 39,
	// 55, 57).
	result.BaseCompatible = true
	if observedRevisionID != "" && revision.ParentRevisionID != "" && observedRevisionID != revision.ParentRevisionID {
		skipped, skippable, err := e.interveningRevisionsSkippableTx(tx,
			applicationID, installationID, observedRevisionID, revision.ParentRevisionID)
		if err != nil {
			return result, err
		}
		if !skippable {
			result.BaseCompatible = false
			result.BaseReasonCode = NotBaseMismatch
			result.ObservedAt = observedRevisionID
		} else {
			result.SkippedRevisions = skipped
		}
	}

	result.Deliverable = true
	return result, nil
}

// interveningRevisionsSkippableTx walks from a revision's base back to the
// revision the node says it is running, and reports whether every step in
// between is one the node can never apply: a revision it refused, or one an
// operator withdrew.
//
// It returns the revisions that would be skipped, so the operator can see what
// the node is stepping over rather than being told only that it may.
func (e *Engine) interveningRevisionsSkippableTx(
	tx *sql.Tx,
	applicationID, installationID, observedRevisionID, baseRevisionID string,
) ([]string, bool, error) {
	// A ceiling, not a policy: a longer chain means something is wrong with the
	// history, and refusing is the safe answer.
	const maxDepth = 64

	var skipped []string
	current := baseRevisionID
	for depth := 0; depth < maxDepth; depth++ {
		if current == "" {
			// The chain does not reach the observed revision at all.
			return nil, false, nil
		}
		if current == observedRevisionID {
			return skipped, true, nil
		}
		disposition, err := projection.GetUpdateDispositionTx(tx, applicationID, installationID, current)
		if err != nil {
			if errors.Is(err, projection.ErrNotFound) {
				// The node never saw this revision, so it might still apply it.
				return nil, false, nil
			}
			return nil, false, err
		}
		switch disposition.State {
		case projection.UpdateRejected, projection.UpdateWithdrawn:
		default:
			// Fetching is not deciding: a fetched revision is still deliverable,
			// so the node is not stuck past it.
			return nil, false, nil
		}
		skipped = append(skipped, current)
		revision, err := projection.GetRevisionTx(tx, current)
		if err != nil {
			return nil, false, err
		}
		current = revision.ParentRevisionID
	}
	return nil, false, nil
}

// IsRevisionDeliverable is the read-only form of the delivery gate, for
// callers that are not already inside a transaction (tests, review tooling).
// The gate itself lives in exactly one place: IsRevisionDeliverableTx.
func (e *Engine) IsRevisionDeliverable(
	ctx context.Context,
	applicationID, installationID, configFamilyID, revisionID, observedRevisionID string,
) (Eligibility, error) {
	var result Eligibility
	err := e.withTx(func(tx *sql.Tx) error {
		eligibility, err := e.IsRevisionDeliverableTx(tx, applicationID, installationID,
			configFamilyID, revisionID, observedRevisionID)
		result = eligibility
		return err
	})
	return result, err
}

// Updates lists the revisions this installation may take, newest first.
//
// Only human-approved, LYPH_MANAGED, validated, not-withdrawn, not-rejected
// revisions appear. The node's own observed revision decides base
// compatibility, which is reported rather than used to hide the update
// (mission sections 25, 55, 56).
func (e *Engine) Updates(ctx context.Context, applicationID, installationID, configFamilyFilter string) ([]projection.ApprovedUpdate, error) {
	var out []projection.ApprovedUpdate
	now := time.Now().UTC()

	err := e.withTx(func(tx *sql.Tx) error {
		families, err := e.familiesForInstallationTx(tx, applicationID, installationID, configFamilyFilter)
		if err != nil {
			return err
		}

		for _, family := range families {
			if family.ManagementMode != projection.ManagementLymphManaged {
				continue
			}
			observed, err := projection.CurrentRefRevision(tx, applicationID, family.ConfigFamilyID, installationID, projection.RefObserved)
			if err != nil {
				return err
			}
			candidates, err := e.deliveryCandidatesTx(tx, applicationID, installationID, family.ConfigFamilyID)
			if err != nil {
				return err
			}
			for _, revisionID := range candidates {
				eligibility, err := e.IsRevisionDeliverableTx(tx, applicationID, installationID, family.ConfigFamilyID, revisionID, observed)
				if err != nil {
					// Not deliverable is the normal case for most revisions; a
					// hard error is a real failure.
					var notDeliverable *ErrNotDeliverable
					if errors.As(err, &notDeliverable) {
						continue
					}
					return err
				}
				revision := eligibility.Revision
				out = append(out, projection.ApprovedUpdate{
					ApplicationID:    applicationID,
					InstallationID:   installationID,
					ConfigFamilyID:   family.ConfigFamilyID,
					RevisionID:       revision.RevisionID,
					ContentHash:      revision.RootTreeHash,
					ApprovalID:       eligibility.Approval.ApprovalID,
					BaseRevisionID:   revision.ParentRevisionID,
					SchemaRevision:   revision.SchemaRevision,
					RevisionSequence: revision.Sequence,
					BaseCompatible:   eligibility.BaseCompatible,
					BaseReasonCode:   eligibility.BaseReasonCode,
					SkippedRevisions: eligibility.SkippedRevisions,
					CreatedAt:        now,
				})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Newest first: if several revisions became approved before the node
	// looked, the newest is the one it should consider (mission section 56).
	sort.Slice(out, func(i, j int) bool { return out[i].RevisionSequence > out[j].RevisionSequence })
	return out, nil
}

// familiesForInstallationTx lists the families an installation could receive.
func (e *Engine) familiesForInstallationTx(tx *sql.Tx, applicationID, installationID, filter string) ([]projection.ConfigFamily, error) {
	if filter != "" {
		family, err := projection.GetConfigFamilyTx(tx, filter)
		if err != nil {
			return nil, err
		}
		if family.ApplicationID != applicationID {
			return nil, &ErrNotDeliverable{ReasonCode: NotFamilyMismatch,
				Message: fmt.Sprintf("family %s belongs to another application", family.Name)}
		}
		return []projection.ConfigFamily{family}, nil
	}
	return projection.ListConfigFamiliesTx(tx, applicationID)
}

// deliveryCandidatesTx lists revisions of a family that came from an approved
// candidate, newest first. It is deliberately narrower than "all revisions":
// a revision that no human ever approved is not a candidate for delivery.
func (e *Engine) deliveryCandidatesTx(tx *sql.Tx, applicationID, installationID, configFamilyID string) ([]string, error) {
	rows, err := tx.Query(`SELECT revision_id FROM config_revisions
		WHERE application_id = ? AND config_family_id = ? AND candidate_id <> ''
		ORDER BY sequence DESC LIMIT 20`, applicationID, configFamilyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var revisionID string
		if err := rows.Scan(&revisionID); err != nil {
			return nil, err
		}
		out = append(out, revisionID)
	}
	return out, rows.Err()
}

// FetchUpdate returns the exact bytes of a deliverable revision, plus the
// metadata the node needs to verify them itself.
//
// The eligibility check runs again here: a successful CheckUpdate is not an
// authorisation for a later fetch (mission section 27).
type FetchedUpdate struct {
	Update         projection.ApprovedUpdate `json:"update"`
	Bundle         map[string][]byte         `json:"bundle"`
	Manifest       []objectstore.Entry       `json:"manifest"`
	TreeHash       string                    `json:"root_tree_hash"`
	SchemaRevision string                    `json:"schema_revision,omitempty"`
}

// FetchUpdate reads the immutable content of an approved revision.
func (e *Engine) FetchUpdate(ctx context.Context, applicationID, installationID, configFamilyID, revisionID, observedRevisionID string) (FetchedUpdate, error) {
	var out FetchedUpdate
	now := time.Now().UTC()

	err := e.withTx(func(tx *sql.Tx) error {
		eligibility, err := e.IsRevisionDeliverableTx(tx, applicationID, installationID, configFamilyID, revisionID, observedRevisionID)
		if err != nil {
			return err
		}
		revision := eligibility.Revision

		tree, err := e.objects.GetTree(revision.RootTreeHash)
		if err != nil {
			return fmt.Errorf("revision %s references a missing tree: %w", revisionID, err)
		}
		bundle := make(map[string][]byte, len(tree.Entries))
		manifest := make([]objectstore.Entry, 0, len(tree.Entries))
		for _, entry := range tree.Entries {
			content, err := e.objects.Get(entry.Hash)
			if err != nil {
				return fmt.Errorf("revision %s references a missing blob %s: %w", revisionID, entry.Path, err)
			}
			bundle[entry.Path] = content
			manifest = append(manifest, entry)
		}

		out = FetchedUpdate{
			Update: projection.ApprovedUpdate{
				ApplicationID:    applicationID,
				InstallationID:   installationID,
				ConfigFamilyID:   configFamilyID,
				RevisionID:       revision.RevisionID,
				ContentHash:      revision.RootTreeHash,
				ApprovalID:       eligibility.Approval.ApprovalID,
				BaseRevisionID:   revision.ParentRevisionID,
				SchemaRevision:   revision.SchemaRevision,
				RevisionSequence: revision.Sequence,
				BaseCompatible:   eligibility.BaseCompatible,
				BaseReasonCode:   eligibility.BaseReasonCode,
				SkippedRevisions: eligibility.SkippedRevisions,
				CreatedAt:        now,
			},
			Bundle:         bundle,
			Manifest:       manifest,
			TreeHash:       revision.RootTreeHash,
			SchemaRevision: revision.SchemaRevision,
		}

		// Fetching is recorded, not acknowledged: the node has not applied
		// anything yet (mission section 65).
		return projection.UpsertUpdateDisposition(tx, projection.UpdateDisposition{
			ApplicationID:  applicationID,
			InstallationID: installationID,
			ConfigFamilyID: configFamilyID,
			RevisionID:     revisionID,
			ContentHash:    revision.RootTreeHash,
			ApprovalID:     eligibility.Approval.ApprovalID,
			State:          projection.UpdateFetched,
			FetchedAt:      now,
			UpdatedAt:      now,
		})
	})
	if err != nil {
		return FetchedUpdate{}, err
	}
	return out, nil
}

// ReportAppliedRequest is a node saying it is now running a revision.
type ReportAppliedRequest struct {
	ApplicationID  string `json:"application_id"`
	InstallationID string `json:"installation_id"`
	ConfigFamilyID string `json:"config_family_id"`
	RevisionID     string `json:"revision_id"`

	ObservedHash       string          `json:"observed_hash,omitempty"`
	PreviousRevisionID string          `json:"previous_revision_id,omitempty"`
	Details            json.RawMessage `json:"details,omitempty"`

	SessionID string `json:"-"`
	ProcessID string `json:"-"`
	PeerUID   int    `json:"-"`
}

// ReportRejectedRequest is a node saying it will not run a revision.
type ReportRejectedRequest struct {
	ApplicationID  string `json:"application_id"`
	InstallationID string `json:"installation_id"`
	ConfigFamilyID string `json:"config_family_id"`
	RevisionID     string `json:"revision_id"`

	ReasonCode string          `json:"reason_code"`
	Message    string          `json:"message,omitempty"`
	Details    json.RawMessage `json:"details,omitempty"`

	SessionID string `json:"-"`
	ProcessID string `json:"-"`
	PeerUID   int    `json:"-"`
}

// ReportApplied records a node's confirmation and moves the refs that mean
// "what the node is actually running" (mission section 36).
func (e *Engine) ReportApplied(ctx context.Context, req ReportAppliedRequest) (projection.ApplicationResult, error) {
	return e.reportApplicationResult(ctx, applicationResultInput{
		ApplicationID:      req.ApplicationID,
		InstallationID:     req.InstallationID,
		ConfigFamilyID:     req.ConfigFamilyID,
		RevisionID:         req.RevisionID,
		ObservedHash:       req.ObservedHash,
		PreviousRevisionID: req.PreviousRevisionID,
		Result:             projection.ResultApplied,
		Details:            string(req.Details),
		SessionID:          req.SessionID,
		ProcessID:          req.ProcessID,
		PeerUID:            req.PeerUID,
	})
}

// ReportRejected records a node's refusal, which is evidence rather than a
// governance change: nothing about the approval is rewritten
// (mission sections 37, 38).
func (e *Engine) ReportRejected(ctx context.Context, req ReportRejectedRequest) (projection.ApplicationResult, error) {
	if !projection.ValidRejectionReason(req.ReasonCode) {
		return projection.ApplicationResult{}, fmt.Errorf(
			"%w: reason_code %q is not one of %v", ErrInvalidArtifact, req.ReasonCode, projection.RejectionReasonCodes)
	}
	return e.reportApplicationResult(ctx, applicationResultInput{
		ApplicationID:  req.ApplicationID,
		InstallationID: req.InstallationID,
		ConfigFamilyID: req.ConfigFamilyID,
		RevisionID:     req.RevisionID,
		Result:         projection.ResultRejected,
		ReasonCode:     req.ReasonCode,
		Message:        req.Message,
		Details:        string(req.Details),
		SessionID:      req.SessionID,
		ProcessID:      req.ProcessID,
		PeerUID:        req.PeerUID,
	})
}

type applicationResultInput struct {
	ApplicationID      string
	InstallationID     string
	ConfigFamilyID     string
	RevisionID         string
	ObservedHash       string
	PreviousRevisionID string
	Result             string
	ReasonCode         string
	Message            string
	Details            string
	SessionID          string
	ProcessID          string
	PeerUID            int
}

func (e *Engine) reportApplicationResult(ctx context.Context, in applicationResultInput) (projection.ApplicationResult, error) {
	now := time.Now().UTC()
	var out projection.ApplicationResult

	err := e.withTx(func(tx *sql.Tx) error {
		// Integrity is checked before idempotency. A second report that claims a
		// different hash than the revision is corruption or a mistake, and
		// answering "already applied" would bury it (mission section 31).
		revision, err := projection.GetRevisionTx(tx, in.RevisionID)
		if err != nil {
			return err
		}
		if in.Result == projection.ResultApplied && in.ObservedHash != "" && in.ObservedHash != revision.RootTreeHash {
			return fmt.Errorf("INTEGRITY_ERROR: node reports %s, revision %s hashes to %s",
				in.ObservedHash, in.RevisionID, revision.RootTreeHash)
		}

		// Idempotency: the same node reporting the same outcome for the same
		// revision is one logical result, not two (mission section 54).
		if existing, err := projection.LatestApplicationResultForRevision(tx, in.ApplicationID, in.InstallationID, in.RevisionID); err == nil {
			if existing.Result == in.Result && existing.ReasonCode == in.ReasonCode {
				out = existing
				return nil
			}
		} else if !errors.Is(err, projection.ErrNotFound) {
			return err
		}

		eligibility, err := e.IsRevisionDeliverableTx(tx, in.ApplicationID, in.InstallationID, in.ConfigFamilyID, in.RevisionID, in.PreviousRevisionID)
		if err != nil {
			return err
		}
		revision = eligibility.Revision

		result := projection.ApplicationResult{
			ResultID:           identity.NewID(),
			ApplicationID:      in.ApplicationID,
			InstallationID:     in.InstallationID,
			ConfigFamilyID:     in.ConfigFamilyID,
			RevisionID:         revision.RevisionID,
			ContentHash:        revision.RootTreeHash,
			ApprovalID:         eligibility.Approval.ApprovalID,
			PreviousRevisionID: in.PreviousRevisionID,
			ObservedHash:       in.ObservedHash,
			Result:             in.Result,
			ReasonCode:         in.ReasonCode,
			Message:            in.Message,
			Details:            in.Details,
			SessionID:          in.SessionID,
			ProcessID:          in.ProcessID,
			PeerUID:            in.PeerUID,
			CreatedAt:          now,
		}

		record, err := e.ledger.Append(ledger.KindUpdateApplicationResult, result.ResultID,
			updateApplicationResultPayload{Result: result}, ledger.Durable)
		if err != nil {
			return err
		}

		if err := projection.InsertApplicationResult(tx, result); err != nil {
			return err
		}
		state := projection.UpdateApplied
		if in.Result == projection.ResultRejected {
			state = projection.UpdateRejected
		}
		if err := projection.UpsertUpdateDisposition(tx, projection.UpdateDisposition{
			ApplicationID:  in.ApplicationID,
			InstallationID: in.InstallationID,
			ConfigFamilyID: in.ConfigFamilyID,
			RevisionID:     revision.RevisionID,
			ContentHash:    revision.RootTreeHash,
			ApprovalID:     eligibility.Approval.ApprovalID,
			State:          state,
			ReasonCode:     in.ReasonCode,
			ObservedAt:     now,
			UpdatedAt:      now,
		}); err != nil {
			return err
		}

		if in.Result == projection.ResultApplied {
			// observed: what the node is actually running.
			// last_good: the last node-confirmed good revision.
			// `deployed` is deliberately untouched: it stays reserved for a
			// future push adapter (mission section 35).
			for _, refName := range []string{projection.RefObserved, RefLastGood} {
				current, err := projection.CurrentRefRevision(tx, in.ApplicationID, in.ConfigFamilyID, in.InstallationID, refName)
				if err != nil {
					return err
				}
				expected := current
				if err := e.moveRefTx(tx, refMove{
					ApplicationID:  in.ApplicationID,
					ConfigFamilyID: in.ConfigFamilyID,
					InstallationID: in.InstallationID,
					RefName:        refName,
					NewRevision:    revision,
					Expected:       &expected,
					Reason:         "node reported the revision applied",
					ValidationID:   result.ResultID,
					Actor:          in.ProcessID,
					At:             now,
				}); err != nil {
					return err
				}
			}
		} else {
			// Rejection is feedback: it becomes an adaptation signal linked to
			// the revision and the issues the candidate claimed to fix, so the
			// learning loop can act on it (mission section 38).
			if err := e.rejectionFeedbackTx(tx, eligibility, result); err != nil {
				return err
			}
		}

		if err := projection.IndexLedgerRecordTx(tx, record); err != nil {
			return err
		}
		if err := e.auditTxResult(tx, result, record); err != nil {
			return err
		}
		if err := checkpoint(tx, record); err != nil {
			return err
		}
		out = result
		return nil
	})
	if err != nil {
		return projection.ApplicationResult{}, err
	}
	// Rejection feedback is emitted after the commit, never inside it.
	e.FlushDeferredFeedback(ctx)

	e.log.Info("application result recorded",
		"result", out.Result, "installation", in.InstallationID,
		"revision", out.RevisionID, "reason", out.ReasonCode)
	return out, nil
}

// rejectionFeedbackTx turns a rejection into a first-class adaptation signal.
func (e *Engine) rejectionFeedbackTx(tx *sql.Tx, eligibility Eligibility, result projection.ApplicationResult) error {
	candidate, err := projection.GetCandidateTx(tx, eligibility.Revision.CandidateID)
	if err != nil {
		// A missing candidate is not worth failing the whole report over; the
		// result itself is already canonical.
		e.log.Warn("rejected update has no candidate to link", "revision", result.RevisionID, "error", err)
		return nil
	}

	issueIDs := candidate.IssueIDs
	payload, err := json.Marshal(map[string]any{
		"revision_id":           result.RevisionID,
		"approval_id":           result.ApprovalID,
		"application_result_id": result.ResultID,
		"node_reason_code":      result.ReasonCode,
		"message":               result.Message,
		"original_issues":       issueIDs,
	})
	if err != nil {
		return err
	}

	// Feedback must belong to a junction, because a junction is where an
	// application can be repaired. A rejection belongs to the junction whose
	// problem the revision claimed to fix, so it is attributed there rather than
	// to an invented "delivery" junction the application never declared.
	junctionID, err := e.rejectionJunctionTx(tx, candidate)
	if err != nil {
		return err
	}
	if junctionID == "" {
		// Nothing to attach to. The rejection is still canonical — the
		// application result and the audit record both exist — but it cannot
		// enter the grouping loop, and saying so is better than inventing a
		// junction or dropping it silently.
		e.log.Warn("approved update rejected, but the candidate is not linked to a junction; "+
			"the rejection is recorded canonically and will not create an issue",
			"revision", result.RevisionID, "candidate", candidate.CandidateID)
		return nil
	}

	event, err := NewEvent(
		protocol.EventSource(result.ApplicationID, junctionID),
		protocol.FeedbackEventType(protocol.FeedbackQualityFailure),
		junctionID,
		protocol.EventData{
			FeedbackType:   protocol.FeedbackQualityFailure,
			ReasonCode:     "APPROVED_UPDATE_REJECTED",
			ConfigRevision: result.RevisionID,
			Payload:        payload,
		},
	)
	if err != nil {
		return err
	}
	// The node's rejection is real production feedback and should travel the
	// same ingress path everything else uses. That path opens its own
	// transaction, so it cannot run here: the event is queued and emitted by the
	// caller once this transaction has committed.
	e.deferFeedback(EmitRequest{
		Event:            event,
		InstallationID:   result.InstallationID,
		ProducerInstance: result.ProcessID,
		Durability:       ledger.Durable,
	})
	return nil
}

// validationsTx lists a candidate's validations inside a transaction.
// rejectionJunctionTx finds the junction a rejection belongs to: the junction
// whose problem the candidate set out to fix.
//
// The work item is the first choice, because that is the item a worker actually
// worked on. The issues the candidate cites are the fallback. An empty answer
// means the candidate was never tied to a place where the application can be
// repaired.
func (e *Engine) rejectionJunctionTx(tx *sql.Tx, candidate projection.Candidate) (string, error) {
	if candidate.WorkItemID != "" {
		item, err := projection.GetWorkItemTx(tx, candidate.WorkItemID)
		if err != nil && !errors.Is(err, projection.ErrNotFound) {
			return "", err
		}
		if item.JunctionID != "" {
			return item.JunctionID, nil
		}
	}
	for _, issueID := range candidate.IssueIDs {
		issue, err := projection.GetIssueTx(tx, issueID)
		if err != nil {
			if errors.Is(err, projection.ErrNotFound) {
				continue
			}
			return "", err
		}
		if issue.JunctionID != "" {
			return issue.JunctionID, nil
		}
	}
	return "", nil
}

func (e *Engine) validationsTx(tx *sql.Tx, candidateID string) ([]projection.Validation, error) {
	rows, err := tx.Query(`SELECT validation_id, candidate_id, application_id, config_family_id,
		suite_id, suite_version, kind, result, metrics, artifacts, inputs_hash, environment,
		worker, created_at FROM validations WHERE candidate_id = ? ORDER BY created_at`, candidateID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []projection.Validation
	for rows.Next() {
		var v projection.Validation
		var created string
		if err := rows.Scan(&v.ValidationID, &v.CandidateID, &v.ApplicationID, &v.ConfigFamilyID,
			&v.SuiteID, &v.SuiteVersion, &v.Kind, &v.Result, &v.Metrics, &v.Artifacts,
			&v.InputsHash, &v.Environment, &v.Worker, &created); err != nil {
			return nil, err
		}
		if t, err := time.Parse(time.RFC3339Nano, created); err == nil {
			v.CreatedAt = t
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// auditTxResult records the application result as an audit event, which is what
// makes it visible in `lymphctl audit` next to approvals and ref moves.
func (e *Engine) auditTxResult(tx *sql.Tx, result projection.ApplicationResult, rec ledger.Record) error {
	return projection.InsertAuditTx(tx, projection.AuditEvent{
		AuditID:       identity.NewID(),
		Action:        "update." + strings.ToLower(result.Result),
		Actor:         result.ProcessID,
		ApplicationID: result.ApplicationID,
		SubjectKind:   "revision",
		SubjectID:     result.RevisionID,
		Detail: fmt.Sprintf(`{"installation_id":%q,"approval_id":%q,"reason_code":%q,"result_id":%q}`,
			result.InstallationID, result.ApprovalID, result.ReasonCode, result.ResultID),
		LedgerSequence: rec.Sequence,
		CreatedAt:      result.CreatedAt,
	})
}

// WithdrawUpdate stops offering an approved revision to one installation. It is
// canonical: the approval is not erased, the history is "approved, then
// withdrawn" (mission section 41).
func (e *Engine) WithdrawUpdate(ctx context.Context, applicationID, installationID, configFamilyID, revisionID, actor, reason string) (projection.UpdateDisposition, error) {
	now := time.Now().UTC()
	var out projection.UpdateDisposition

	err := e.withTx(func(tx *sql.Tx) error {
		family, err := projection.GetConfigFamilyTx(tx, configFamilyID)
		if err != nil {
			return err
		}
		if family.ApplicationID != applicationID {
			return &ErrNotDeliverable{ReasonCode: NotFamilyMismatch, Message: family.Name}
		}
		revision, err := projection.GetRevisionTx(tx, revisionID)
		if err != nil {
			return err
		}
		disposition := projection.UpdateDisposition{
			ApplicationID:  applicationID,
			InstallationID: installationID,
			ConfigFamilyID: configFamilyID,
			RevisionID:     revisionID,
			ContentHash:    revision.RootTreeHash,
			State:          projection.UpdateWithdrawn,
			ReasonCode:     "OPERATOR_WITHDRAWN",
			UpdatedAt:      now,
		}
		if prior, err := projection.GetUpdateDispositionTx(tx, applicationID, installationID, revisionID); err == nil {
			disposition.ApprovalID = prior.ApprovalID
			disposition.FetchedAt = prior.FetchedAt
			disposition.ObservedAt = prior.ObservedAt
		}
		record, err := e.ledger.Append(ledger.KindUpdateWithdrawn, revisionID, updateWithdrawalPayload{
			ApplicationID:  applicationID,
			InstallationID: installationID,
			ConfigFamilyID: configFamilyID,
			RevisionID:     revisionID,
			Actor:          actor,
			Reason:         reason,
			At:             now,
		}, ledger.Durable)
		if err != nil {
			return err
		}
		if err := projection.UpsertUpdateDisposition(tx, disposition); err != nil {
			return err
		}
		if err := projection.IndexLedgerRecordTx(tx, record); err != nil {
			return err
		}
		if err := e.auditTxResult(tx, projection.ApplicationResult{
			ApplicationID: applicationID, InstallationID: installationID,
			ConfigFamilyID: configFamilyID, RevisionID: revisionID,
			Result: "WITHDRAWN", ReasonCode: disposition.ReasonCode,
			ProcessID: actor, CreatedAt: now,
		}, record); err != nil {
			return err
		}
		if err := checkpoint(tx, record); err != nil {
			return err
		}
		out = disposition
		return nil
	})
	if err != nil {
		return projection.UpdateDisposition{}, err
	}
	return out, nil
}

// ApplicationResults lists a node's reported outcomes.
func (e *Engine) ApplicationResults(applicationID, installationID string, limit int) ([]projection.ApplicationResult, error) {
	return e.db.ListApplicationResults(applicationID, installationID, limit)
}

// UpdateDispositions lists delivery state.
func (e *Engine) UpdateDispositions(applicationID, installationID, state string, limit int) ([]projection.UpdateDisposition, error) {
	return e.db.ListUpdateDispositions(applicationID, installationID, state, limit)
}

// FlushDeferredFeedback emits feedback that had to be queued until a
// transaction committed. Failure is logged, never fatal: the application result
// is already canonical.
func (e *Engine) FlushDeferredFeedback(ctx context.Context) {
	e.deferredMu.Lock()
	pending := e.deferredFeedback
	e.deferredFeedback = nil
	e.deferredMu.Unlock()

	for _, request := range pending {
		if _, err := e.Emit(ctx, request); err != nil {
			e.log.Warn("could not emit deferred feedback", "event", request.Event.ID, "error", err)
		}
	}
}

// deferFeedback queues a request to emit once the current transaction is done.
func (e *Engine) deferFeedback(request EmitRequest) {
	e.deferredMu.Lock()
	e.deferredFeedback = append(e.deferredFeedback, request)
	e.deferredMu.Unlock()
}
