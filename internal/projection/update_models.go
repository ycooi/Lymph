package projection

import "time"

// Update delivery is a pull model: Lymph publishes that an exact revision is
// eligible for an exact installation, and the node decides whether and when to
// take it. Lymph never writes to an application's filesystem (mission section 17).

// Approval actor types.
const (
	ActorHuman  = "HUMAN"
	ActorSystem = "SYSTEM"
)

// RefObserved is the ref a node confirms it is actually running. It is
// deliberately separate from `deployed`, which stays reserved for a future push
// adapter (mission sections 34, 35).
const RefObserved = "observed"

// Update disposition states (mission section 40).
const (
	UpdateAvailable = "AVAILABLE"
	UpdateFetched   = "FETCHED"
	UpdateApplied   = "APPLIED"
	UpdateRejected  = "REJECTED"
	UpdateWithdrawn = "WITHDRAWN"
)

// UpdateDisposition is one installation's relationship to one approved
// revision.
//
// It exists so a rejected revision is not offered again forever (mission
// section 39). Canonical truth remains the approval plus the application-result
// records; this is their queryable projection.
type UpdateDisposition struct {
	ApplicationID  string    `json:"application_id"`
	InstallationID string    `json:"installation_id"`
	ConfigFamilyID string    `json:"config_family_id"`
	RevisionID     string    `json:"revision_id"`
	ContentHash    string    `json:"content_hash"`
	ApprovalID     string    `json:"approval_id"`
	State          string    `json:"state"`
	ReasonCode     string    `json:"reason_code,omitempty"`
	FetchedAt      time.Time `json:"fetched_at,omitempty"`
	ObservedAt     time.Time `json:"observed_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// ApplicationResult is the canonical record of a node applying or rejecting an
// approved revision (mission section 33).
//
// Unlike a session, this is history: it survives a projection rebuild and is
// part of the state digest.
type ApplicationResult struct {
	ResultID string `json:"result_id"`

	ApplicationID  string `json:"application_id"`
	InstallationID string `json:"installation_id"`
	ConfigFamilyID string `json:"config_family_id"`

	RevisionID  string `json:"revision_id"`
	ContentHash string `json:"content_hash"`
	ApprovalID  string `json:"approval_id,omitempty"`

	PreviousRevisionID string `json:"previous_revision_id,omitempty"`
	ObservedHash       string `json:"observed_hash,omitempty"`

	// Result is APPLIED or REJECTED.
	Result     string `json:"result"`
	ReasonCode string `json:"reason_code,omitempty"`
	Message    string `json:"message,omitempty"`
	Details    string `json:"details,omitempty"`

	// Provenance: which session and process reported it, and from which uid.
	SessionID string `json:"session_id,omitempty"`
	ProcessID string `json:"process_id,omitempty"`
	PeerUID   int    `json:"peer_uid,omitempty"`

	CreatedAt time.Time `json:"created_at"`
}

// Application result values.
const (
	ResultApplied  = "APPLIED"
	ResultRejected = "REJECTED"
)

// RejectionReasonCodes are the machine-readable reasons a node may give
// (mission section 32). A free-form reason code is refused.
var RejectionReasonCodes = []string{
	"LOCAL_VALIDATION_FAILED",
	"SCHEMA_UNSUPPORTED",
	"RELOAD_FAILED",
	"HEALTH_CHECK_FAILED",
	"DEPENDENCY_MISSING",
	"OPERATOR_CANCELLED",
	"OTHER",
}

// ValidRejectionReason reports whether a node's reason code is known.
func ValidRejectionReason(code string) bool {
	for _, known := range RejectionReasonCodes {
		if code == known {
			return true
		}
	}
	return false
}

// ApprovedUpdate is the delivery view: this exact revision, at this exact
// content hash, is eligible for this exact installation to consume.
//
// It is not a copy of the revision. It references immutable content that the
// node fetches and verifies itself (mission section 18).
type ApprovedUpdate struct {
	ApplicationID  string `json:"application_id"`
	InstallationID string `json:"installation_id"`
	ConfigFamilyID string `json:"config_family_id"`

	RevisionID  string `json:"revision_id"`
	ContentHash string `json:"content_hash"`

	ApprovalID string `json:"approval_id"`

	BaseRevisionID string `json:"base_revision_id,omitempty"`
	SchemaRevision string `json:"schema_revision,omitempty"`

	// RevisionSequence is the per-family revision number, for operators.
	RevisionSequence int `json:"revision_sequence,omitempty"`

	// BaseCompatible is false when the node's reported revision is not the base
	// this update expects. CheckUpdate reports it rather than refusing silently
	// (mission section 55).
	BaseCompatible bool   `json:"base_compatible"`
	BaseReasonCode string `json:"base_reason_code,omitempty"`

	// SkippedRevisions names the revisions between what the node is running and
	// this update's base that the node has already refused or an operator has
	// withdrawn. They can never be applied, so requiring one of them as a base
	// would require a state that cannot be reached (mission sections 39, 55).
	SkippedRevisions []string `json:"skipped_revisions,omitempty"`

	CreatedAt time.Time `json:"created_at"`
}
