package engine

import (
	"time"

	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
)

// Ledger records are the canonical form of every mutation. Each payload below
// is self-contained: replaying nothing but these records rebuilds the whole
// SQLite projection (section 12, section 88).

// eventPayload is written for kind EVENT. It carries the accepted event, the
// issue it was grouped into, and the occurrence link, so grouping survives a
// rebuild without re-running fingerprint logic.
type eventPayload struct {
	Event        projection.Event      `json:"event"`
	Issue        projection.Issue      `json:"issue"`
	IssueCreated bool                  `json:"issue_created"`
	Occurrence   projection.Occurrence `json:"occurrence"`
}

// issueStatusPayload is written for kind ISSUE: issue status transitions are
// immutable records even though the status itself is mutable projection state
// (section 37).
type issueStatusPayload struct {
	IssueID string    `json:"issue_id"`
	Status  string    `json:"status"`
	Actor   string    `json:"actor,omitempty"`
	At      time.Time `json:"at"`
}

// registrationPayload is written for kind REGISTRATION. Registration history is
// append-only: changes create another revision (section 7).
type registrationPayload struct {
	Application   projection.Application     `json:"application"`
	Registration  projection.Registration    `json:"registration"`
	Installations []projection.Installation  `json:"installations,omitempty"`
	Junctions     []projection.Junction      `json:"junctions,omitempty"`
	Families      []projection.ConfigFamily  `json:"config_families,omitempty"`
	Targets       []projection.ManagedTarget `json:"targets,omitempty"`
}

// revisionPayload is written for kind REVISION.
type revisionPayload struct {
	Revision   projection.ConfigRevision `json:"revision"`
	Blobs      []objectstore.Entry       `json:"blobs"`
	ObjectHash string                    `json:"object_hash"`
}

// refPayload is written for kind REF: the new ref value plus the reflog entry
// that explains the movement (section 21).
type refPayload struct {
	Ref    projection.ConfigRef   `json:"ref"`
	Reflog projection.ReflogEntry `json:"reflog"`
}

// candidatePayload is written for kind CANDIDATE, covering both creation and
// state transitions.
type candidatePayload struct {
	Candidate projection.Candidate `json:"candidate"`
	FromState string               `json:"from_state,omitempty"`
	Reason    string               `json:"reason,omitempty"`
	At        time.Time            `json:"at"`
}

// validationPayload is written for kind VALIDATION. Test evidence is canonical:
// "why was r43 considered safe" must survive the projection being thrown away
// (section 48, section 72).
type validationPayload struct {
	Validation projection.Validation `json:"validation"`
}

// approvalPayload is written for kind APPROVAL, for the same reason: who
// approved what, and when, is not derived state (section 50).
type approvalPayload struct {
	Approval projection.Approval `json:"approval"`
}

// workPayload is written for kind WORK_ITEM.
type workPayload struct {
	WorkItem  projection.WorkItem `json:"work_item"`
	FromState string              `json:"from_state,omitempty"`
	Reason    string              `json:"reason,omitempty"`
	At        time.Time           `json:"at"`
}

// improvementResultPayload is written for kind IMPROVEMENT_RESULT.
//
// One worker return is one canonical record: the result, every artifact, any
// config candidates the artifacts created, and the work item's RETURNED state.
// Replaying this single record must be enough to reconstruct all of it, so that
// a crash can never leave a candidate without its result, or an artifact
// without its result (mission sections 16 to 18).
type improvementResultPayload struct {
	Result     projection.ImprovementResult     `json:"result"`
	Artifacts  []projection.ImprovementArtifact `json:"artifacts"`
	Candidates []projection.Candidate           `json:"candidates,omitempty"`
	WorkItem   projection.WorkItem              `json:"work_item"`
}

// auditPayload is written for kind AUDIT.
type auditPayload struct {
	Audit projection.AuditEvent `json:"audit"`
}

// updateApplicationResultPayload is written for kind UPDATE_APPLICATION_RESULT.
//
// A node's applied/rejected report is history, not runtime state: it must
// survive a rebuild and it is the evidence behind the `observed` and
// `last_good` refs (mission section 33).
type updateApplicationResultPayload struct {
	Result projection.ApplicationResult `json:"result"`
}

// updateWithdrawalPayload is written for kind UPDATE_WITHDRAWN. Withdrawal is
// canonical and does not erase the approval it withdraws (mission section 41).
type updateWithdrawalPayload struct {
	ApplicationID  string    `json:"application_id"`
	InstallationID string    `json:"installation_id"`
	ConfigFamilyID string    `json:"config_family_id"`
	RevisionID     string    `json:"revision_id"`
	Actor          string    `json:"actor,omitempty"`
	Reason         string    `json:"reason,omitempty"`
	At             time.Time `json:"at"`
}
