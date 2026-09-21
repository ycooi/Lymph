// Package projection is Lymph's queryable index over canonical truth
// (section 12, section 16).
//
// The ledger plus the object store are canonical. SQLite is derived: if the
// database dies, `lymphctl rebuild` replays the ledger and rebuilds every row
// here. Nothing in this package may be the only place a fact is recorded.
package projection

import "time"

func ts(t time.Time) string {
	if t.IsZero() {
		t = time.Now()
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTS(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// Application is a registered application (section 7, section 79).
type Application struct {
	ApplicationID         string    `json:"application_id"`
	Name                  string    `json:"name"`
	AppType               string    `json:"app_type,omitempty"`
	Owner                 string    `json:"owner,omitempty"`
	Repository            string    `json:"repository,omitempty"`
	InstallationID        string    `json:"installation_id,omitempty"`
	DefaultInstallationID string    `json:"default_installation_id,omitempty"`
	RegistrationRevision  int       `json:"registration_revision"`
	CreatedAt             time.Time `json:"created_at"`
	UpdatedAt             time.Time `json:"updated_at"`
}

// Installation is one deployment of an application (mission section 38).
//
// The identity is a UUID. A hostname is metadata at best: two deployments can
// share a hostname over time, and one deployment can move hosts.
type Installation struct {
	InstallationID string `json:"installation_id"`
	ApplicationID  string `json:"application_id"`
	Name           string `json:"name,omitempty"`
	Environment    string `json:"environment,omitempty"`
	// SessionPolicy governs how many processes may hold a session for this
	// installation at once. MULTI_PROCESS_ALLOWED is the default because a
	// deployment may legitimately run several workers or replicas.
	SessionPolicy string    `json:"session_policy,omitempty"`
	Active        bool      `json:"active"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Session policy values.
const (
	SessionPolicyMulti  = "MULTI_PROCESS_ALLOWED"
	SessionPolicySingle = "SINGLE_PROCESS_EXPECTED"
)

// Session is one transient runtime connection: a process of an installed
// application, talking to this daemon right now.
//
// Sessions are operational state, not canonical history. They are deliberately
// excluded from the state digest used for disaster-recovery equivalence,
// because after a daemon restart the correct answer is "no sessions" (mission
// sections 8, 43, 44).
type Session struct {
	SessionID string `json:"session_id"`

	ApplicationID  string `json:"application_id"`
	InstallationID string `json:"installation_id"`
	ProcessID      string `json:"process_id"`

	ProtocolVersion string `json:"protocol_version"`
	ClientName      string `json:"client_name,omitempty"`
	ClientVersion   string `json:"client_version,omitempty"`
	ManifestHash    string `json:"manifest_hash,omitempty"`

	PID      int    `json:"pid,omitempty"`
	Hostname string `json:"hostname,omitempty"`

	// PeerUID and PeerGID come from kernel peer credentials on Linux and macOS.
	// -1 means "not available on this platform".
	PeerUID int `json:"peer_uid"`
	PeerGID int `json:"peer_gid"`

	State string `json:"state"`

	ConnectedAt time.Time `json:"connected_at"`
	LastSeenAt  time.Time `json:"last_seen_at"`
	ClosedAt    time.Time `json:"closed_at,omitempty"`
}

// Session states.
const (
	SessionActive   = "ACTIVE"
	SessionClosed   = "CLOSED"
	SessionStale    = "STALE"
	SessionRejected = "REJECTED"
)

// ValidSessionState reports whether a state is one this build uses.
func ValidSessionState(state string) bool {
	switch state {
	case SessionActive, SessionClosed, SessionStale, SessionRejected:
		return true
	}
	return false
}

// Registration is one immutable registration revision (section 7).
type Registration struct {
	RegistrationID string    `json:"registration_id"`
	ApplicationID  string    `json:"application_id"`
	Revision       int       `json:"revision"`
	Manifest       string    `json:"manifest"`
	ContentHash    string    `json:"content_hash"`
	LedgerSequence uint64    `json:"ledger_sequence"`
	CreatedAt      time.Time `json:"created_at"`
}

// Junction is a place where an application may discover that its current
// behaviour is inadequate (section 8).
type Junction struct {
	JunctionID     string    `json:"junction_id"`
	ApplicationID  string    `json:"application_id"`
	Name           string    `json:"name"`
	WorkflowType   string    `json:"improvement_workflow,omitempty"`
	ConfigFamilyID string    `json:"config_family_id,omitempty"`
	FeedbackTypes  []string  `json:"feedback_types,omitempty"`
	Contract       string    `json:"contract,omitempty"`
	ReplayAdapter  string    `json:"replay_adapter,omitempty"`
	Active         bool      `json:"active"`
	CreatedAt      time.Time `json:"created_at"`
}

// ConfigFamily is one independently versioned configuration domain
// (section 24).
type ConfigFamily struct {
	ConfigFamilyID  string `json:"config_family_id"`
	ApplicationID   string `json:"application_id"`
	Name            string `json:"name"`
	SchemaRevision  string `json:"schema_revision,omitempty"`
	WorkflowType    string `json:"workflow_type,omitempty"`
	RequiresHoldout bool   `json:"requires_holdout"`
	// ManagementMode decides whether Lymph may ever deliver this family to a
	// node. It defaults to EXTERNAL: a family that did not explicitly opt in
	// is never managed, and anything a node keeps outside the boundary
	// (credentials, tokens) therefore cannot leak into delivery
	// (mission sections 5, 6).
	ManagementMode string    `json:"management_mode"`
	CreatedAt      time.Time `json:"created_at"`
}

// Management modes.
const (
	ManagementLymphManaged = "LYMPH_MANAGED"
	ManagementExternal     = "EXTERNAL"
)

// ValidManagementMode reports whether a mode is one this build knows.
func ValidManagementMode(mode string) bool {
	return mode == ManagementLymphManaged || mode == ManagementExternal
}

// NormalizeManagementMode defaults to the conservative value.
func NormalizeManagementMode(mode string) string {
	if mode == ManagementLymphManaged {
		return ManagementLymphManaged
	}
	return ManagementExternal
}

// ManagedTarget is a pre-registered deployment destination (section 25).
// Events have no authority to invent targets.
type ManagedTarget struct {
	TargetID       string    `json:"target_id"`
	ApplicationID  string    `json:"application_id"`
	ConfigFamilyID string    `json:"config_family_id"`
	InstallationID string    `json:"installation_id,omitempty"`
	TargetType     string    `json:"target_type"`
	Path           string    `json:"path"`
	ReloadPolicy   string    `json:"reload_policy,omitempty"`
	Atomicity      string    `json:"atomicity,omitempty"`
	Health         string    `json:"health,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

// PathOwner records which application and family own a managed path
// (section 26). Two applications may not own the same path.
type PathOwner struct {
	Path           string    `json:"path"`
	ApplicationID  string    `json:"application_id"`
	ConfigFamilyID string    `json:"config_family_id"`
	InstallationID string    `json:"installation_id,omitempty"`
	TargetID       string    `json:"target_id"`
	CreatedAt      time.Time `json:"created_at"`
}

// Event is the indexed form of an accepted LymphEvent (section 10).
type Event struct {
	EventID          string `json:"event_id"`
	ApplicationID    string `json:"application_id"`
	JunctionID       string `json:"junction_id"`
	InstallationID   string `json:"installation_id,omitempty"`
	Type             string `json:"type"`
	Source           string `json:"source"`
	Subject          string `json:"subject,omitempty"`
	FeedbackType     string `json:"feedback_type"`
	ReasonCode       string `json:"reason_code,omitempty"`
	Fingerprint      string `json:"fingerprint"`
	ProducerInstance string `json:"producer_instance,omitempty"`
	ContractRevision string `json:"contract_revision,omitempty"`
	ConfigRevision   string `json:"config_revision,omitempty"`
	ConfigHash       string `json:"config_hash,omitempty"`
	InputRef         string `json:"input_ref,omitempty"`
	ReplayRef        string `json:"replay_ref,omitempty"`
	// SessionID is the session the event arrived under, empty for a legacy
	// sessionless client. Session identity is operational context: it never
	// becomes part of a fingerprint or an issue (mission sections 63, 120).
	SessionID string `json:"session_id,omitempty"`
	// ReplayedBySessionID is set when this event was replayed from a client
	// spool, whose producer may be an earlier process (mission section 63).
	ReplayedBySessionID string    `json:"replayed_by_session_id,omitempty"`
	Durability          string    `json:"durability"`
	Data                string    `json:"data"`
	OccurredAt          time.Time `json:"time"`
	ReceivedAt          time.Time `json:"received_at"`
	LedgerSequence      uint64    `json:"ledger_sequence"`
}

// Issue groups recurring events (section 36).
type Issue struct {
	IssueID             string    `json:"issue_id"`
	ApplicationID       string    `json:"application_id"`
	JunctionID          string    `json:"junction_id"`
	FeedbackType        string    `json:"feedback_type"`
	ReasonCode          string    `json:"reason_code,omitempty"`
	Fingerprint         string    `json:"fingerprint"`
	Pattern             string    `json:"pattern,omitempty"`
	Status              string    `json:"status"`
	Severity            string    `json:"severity,omitempty"`
	Priority            int       `json:"priority"`
	OccurrenceCount     int       `json:"occurrence_count"`
	UniqueSources       int       `json:"unique_sources"`
	ControllingRevision string    `json:"controlling_config_revision,omitempty"`
	FirstSeen           time.Time `json:"first_seen"`
	LastSeen            time.Time `json:"last_seen"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// Occurrence links one event to one issue. Raw events stay preserved
// separately (section 36).
type Occurrence struct {
	IssueID  string    `json:"issue_id"`
	EventID  string    `json:"event_id"`
	SeenAt   time.Time `json:"seen_at"`
	Producer string    `json:"producer_instance,omitempty"`
}

// ConfigRevision is the indexed form of a revision object (section 23).
type ConfigRevision struct {
	RevisionID       string    `json:"revision_id"`
	ApplicationID    string    `json:"application_id"`
	ConfigFamilyID   string    `json:"config_family_id"`
	Sequence         int       `json:"sequence"`
	ContentHash      string    `json:"content_hash"`
	RootTreeHash     string    `json:"root_tree_hash"`
	ParentRevisionID string    `json:"parent_revision_id,omitempty"`
	SchemaRevision   string    `json:"schema_revision,omitempty"`
	Label            string    `json:"label,omitempty"`
	Message          string    `json:"message,omitempty"`
	IssueIDs         []string  `json:"issue_ids,omitempty"`
	WorkItemID       string    `json:"work_item_id,omitempty"`
	CandidateID      string    `json:"candidate_id,omitempty"`
	Author           string    `json:"author,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
}

// ConfigRef is a movable pointer to a revision (section 20).
//
// The identity is (application, config family, installation, name): two
// deployments of one application hold their own configuration state.
type ConfigRef struct {
	ApplicationID  string    `json:"application_id"`
	ConfigFamilyID string    `json:"config_family_id"`
	InstallationID string    `json:"installation_id"`
	Name           string    `json:"name"`
	RevisionID     string    `json:"revision_id"`
	ContentHash    string    `json:"content_hash"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// ReflogEntry records where a ref pointed before and after one move
// (section 21). Refs are not history; the reflog is.
type ReflogEntry struct {
	LogID          string    `json:"log_id"`
	ApplicationID  string    `json:"application_id"`
	ConfigFamilyID string    `json:"config_family_id"`
	InstallationID string    `json:"installation_id"`
	RefName        string    `json:"ref_name"`
	OldRevisionID  string    `json:"old_revision_id,omitempty"`
	NewRevisionID  string    `json:"new_revision_id"`
	Reason         string    `json:"reason,omitempty"`
	IssueIDs       []string  `json:"issue_ids,omitempty"`
	CandidateID    string    `json:"candidate_id,omitempty"`
	ValidationID   string    `json:"validation_id,omitempty"`
	ApprovalID     string    `json:"approval_id,omitempty"`
	DeploymentID   string    `json:"deployment_id,omitempty"`
	LedgerSequence uint64    `json:"ledger_sequence"`
	CreatedAt      time.Time `json:"created_at"`
}

// Candidate is a proposed config that is not yet a production revision
// (section 45).
type Candidate struct {
	CandidateID    string    `json:"candidate_id"`
	ApplicationID  string    `json:"application_id"`
	ConfigFamilyID string    `json:"config_family_id"`
	InstallationID string    `json:"installation_id"`
	BaseRevisionID string    `json:"base_revision_id"`
	RootTreeHash   string    `json:"root_tree_hash"`
	State          string    `json:"state"`
	IssueIDs       []string  `json:"issue_ids,omitempty"`
	WorkItemID     string    `json:"work_item_id,omitempty"`
	WorkflowRunID  string    `json:"workflow_run_id,omitempty"`
	Explanation    string    `json:"explanation,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// WorkItem is the package handed to an improvement workflow (section 39).
type WorkItem struct {
	WorkID         string    `json:"work_id"`
	WorkflowType   string    `json:"workflow_type"`
	ApplicationID  string    `json:"application_id"`
	JunctionID     string    `json:"junction_id,omitempty"`
	ConfigFamilyID string    `json:"config_family_id,omitempty"`
	InstallationID string    `json:"installation_id,omitempty"`
	BaseRevisionID string    `json:"base_revision_id,omitempty"`
	IssueIDs       []string  `json:"issue_ids,omitempty"`
	Package        string    `json:"package,omitempty"`
	State          string    `json:"state"`
	LeaseOwner     string    `json:"lease_owner,omitempty"`
	LeaseExpiresAt time.Time `json:"lease_expires_at,omitempty"`
	Attempts       int       `json:"attempts"`
	Result         string    `json:"result,omitempty"`
	// ImprovementResultID is the typed worker return (mission section 22). The
	// free-form Result field is retained for legacy callers only.
	ImprovementResultID string    `json:"improvement_result_id,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// ImprovementResult is one immutable worker return (mission section 6).
//
// The mutable lifecycle stays on WorkItem; this type only records what the
// worker said, once.
type ImprovementResult struct {
	ResultID      string    `json:"result_id"`
	WorkItemID    string    `json:"work_item_id"`
	WorkflowRunID string    `json:"workflow_run_id,omitempty"`
	ApplicationID string    `json:"application_id"`
	IssueIDs      []string  `json:"issue_ids,omitempty"`
	Worker        string    `json:"worker,omitempty"`
	Attempt       int       `json:"attempt"`
	Summary       string    `json:"summary,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

// ImprovementArtifact is one thing a worker produced (mission section 7).
//
// Payload carries small immutable metadata only. A config bundle is referenced
// through its candidate; a model or a commit is referenced by URI and digest.
// Lymph never copies a large binary in here.
type ImprovementArtifact struct {
	ArtifactID     string    `json:"artifact_id"`
	ResultID       string    `json:"result_id"`
	ApplicationID  string    `json:"application_id"`
	ConfigFamilyID string    `json:"config_family_id,omitempty"`
	InstallationID string    `json:"installation_id,omitempty"`
	Kind           string    `json:"kind"`
	CandidateID    string    `json:"candidate_id,omitempty"`
	ContentHash    string    `json:"content_hash,omitempty"`
	Payload        string    `json:"payload"`
	CreatedAt      time.Time `json:"created_at"`
}

// AuditEvent explains a state change after the fact (section 95).
type AuditEvent struct {
	AuditID        string    `json:"audit_id"`
	Action         string    `json:"action"`
	Actor          string    `json:"actor,omitempty"`
	ApplicationID  string    `json:"application_id,omitempty"`
	SubjectKind    string    `json:"subject_kind,omitempty"`
	SubjectID      string    `json:"subject_id,omitempty"`
	Detail         string    `json:"detail,omitempty"`
	LedgerSequence uint64    `json:"ledger_sequence"`
	CreatedAt      time.Time `json:"created_at"`
}
