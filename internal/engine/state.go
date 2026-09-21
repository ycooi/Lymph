package engine

import "fmt"

// State machines are explicit (section 94). No scattered booleans such as
// tested=true / approved=true / deployed=false: every mutable concept has one
// enumerated state and a declared set of legal transitions. An illegal
// transition is an error, not a silent write.

// IssueStatus is the issue lifecycle from section 37.
type IssueStatus string

// Issue lifecycle states.
const (
	IssueOpen                 IssueStatus = "OPEN"
	IssueTriaged              IssueStatus = "TRIAGED"
	IssueQueuedForImprovement IssueStatus = "QUEUED_FOR_IMPROVEMENT"
	IssueInProgress           IssueStatus = "IN_PROGRESS"
	IssueCandidateProduced    IssueStatus = "CANDIDATE_PRODUCED"
	IssueValidating           IssueStatus = "VALIDATING"
	IssueFixed                IssueStatus = "FIXED"
	IssueDeferred             IssueStatus = "DEFERRED"
	IssueIgnored              IssueStatus = "IGNORED"
	IssueReopened             IssueStatus = "REOPENED"
)

var issueTransitions = map[IssueStatus][]IssueStatus{
	IssueOpen:                 {IssueTriaged, IssueQueuedForImprovement, IssueDeferred, IssueIgnored},
	IssueTriaged:              {IssueQueuedForImprovement, IssueDeferred, IssueIgnored},
	IssueQueuedForImprovement: {IssueInProgress, IssueDeferred, IssueIgnored},
	IssueInProgress:           {IssueCandidateProduced, IssueDeferred, IssueIgnored},
	IssueCandidateProduced:    {IssueValidating, IssueInProgress, IssueDeferred},
	IssueValidating:           {IssueFixed, IssueInProgress, IssueDeferred},
	IssueFixed:                {IssueReopened},
	IssueDeferred:             {IssueReopened, IssueQueuedForImprovement, IssueIgnored},
	IssueIgnored:              {IssueReopened},
	IssueReopened:             {IssueTriaged, IssueQueuedForImprovement, IssueInProgress},
}

// ValidIssueStatus reports whether s is a known issue status.
func ValidIssueStatus(s IssueStatus) bool {
	_, ok := issueTransitions[s]
	return ok
}

// CheckIssueTransition rejects illegal issue movements.
func CheckIssueTransition(from, to IssueStatus) error {
	if !ValidIssueStatus(from) {
		return fmt.Errorf("unknown issue status %q", from)
	}
	if !ValidIssueStatus(to) {
		return fmt.Errorf("unknown issue status %q", to)
	}
	if from == to {
		return nil
	}
	for _, allowed := range issueTransitions[from] {
		if allowed == to {
			return nil
		}
	}
	return fmt.Errorf("illegal issue transition %s -> %s", from, to)
}

// CandidateState is the candidate lifecycle from section 45 and section 94.
type CandidateState string

// Candidate lifecycle states.
const (
	CandidateDraft          CandidateState = "DRAFT"
	CandidateStructurallyOK CandidateState = "STRUCTURALLY_VALID"
	CandidateTesting        CandidateState = "TESTING"
	CandidatePassed         CandidateState = "PASSED"
	CandidateApproved       CandidateState = "APPROVED"
	CandidateDeployable     CandidateState = "DEPLOYABLE"
	CandidateActive         CandidateState = "ACTIVE"
	CandidateRejected       CandidateState = "REJECTED"
	CandidateStale          CandidateState = "STALE"
	CandidateRolledBack     CandidateState = "ROLLED_BACK"
	CandidateSuperseded     CandidateState = "SUPERSEDED"
)

var candidateTransitions = map[CandidateState][]CandidateState{
	CandidateDraft:          {CandidateStructurallyOK, CandidateRejected, CandidateStale},
	CandidateStructurallyOK: {CandidateTesting, CandidateRejected, CandidateStale},
	CandidateTesting:        {CandidatePassed, CandidateRejected, CandidateStale},
	CandidatePassed:         {CandidateApproved, CandidateRejected, CandidateStale},
	CandidateApproved:       {CandidateDeployable, CandidateRejected, CandidateStale},
	CandidateDeployable:     {CandidateActive, CandidateStale},
	CandidateActive:         {CandidateRolledBack, CandidateSuperseded},
	CandidateRejected:       {CandidateSuperseded},
	CandidateStale:          {CandidateSuperseded, CandidateTesting},
	CandidateRolledBack:     {CandidateSuperseded},
	CandidateSuperseded:     {},
}

// ValidCandidateState reports whether s is a known candidate state.
func ValidCandidateState(s CandidateState) bool {
	_, ok := candidateTransitions[s]
	return ok
}

// CheckCandidateTransition rejects illegal candidate movements.
func CheckCandidateTransition(from, to CandidateState) error {
	if !ValidCandidateState(from) {
		return fmt.Errorf("unknown candidate state %q", from)
	}
	if !ValidCandidateState(to) {
		return fmt.Errorf("unknown candidate state %q", to)
	}
	if from == to {
		return nil
	}
	for _, allowed := range candidateTransitions[from] {
		if allowed == to {
			return nil
		}
	}
	return fmt.Errorf("illegal candidate transition %s -> %s", from, to)
}

// WorkState is the work item state from section 43.
type WorkState string

// Work item states.
const (
	WorkQueued    WorkState = "QUEUED"
	WorkLeased    WorkState = "LEASED"
	WorkRunning   WorkState = "RUNNING"
	WorkReturned  WorkState = "RETURNED"
	WorkFailed    WorkState = "FAILED"
	WorkExpired   WorkState = "EXPIRED"
	WorkCancelled WorkState = "CANCELLED"
)

var workTransitions = map[WorkState][]WorkState{
	WorkQueued: {WorkLeased, WorkCancelled},
	// LEASED may go straight to RETURNED: a short job never reports RUNNING,
	// and forcing a progress call would be ceremony, not safety.
	WorkLeased:    {WorkRunning, WorkReturned, WorkExpired, WorkFailed, WorkCancelled},
	WorkRunning:   {WorkReturned, WorkFailed, WorkExpired, WorkCancelled},
	WorkReturned:  {},
	WorkFailed:    {WorkQueued},
	WorkExpired:   {WorkQueued, WorkCancelled},
	WorkCancelled: {},
}

// ValidWorkState reports whether s is a known work state.
func ValidWorkState(s WorkState) bool {
	_, ok := workTransitions[s]
	return ok
}

// CheckWorkTransition rejects illegal work item movements.
func CheckWorkTransition(from, to WorkState) error {
	if !ValidWorkState(from) {
		return fmt.Errorf("unknown work state %q", from)
	}
	if !ValidWorkState(to) {
		return fmt.Errorf("unknown work state %q", to)
	}
	if from == to {
		return nil
	}
	for _, allowed := range workTransitions[from] {
		if allowed == to {
			return nil
		}
	}
	return fmt.Errorf("illegal work transition %s -> %s", from, to)
}

// Deployment references (section 52). Lymph never uses a single pointer.
const (
	RefActive   = "active"
	RefApproved = "approved"
	RefDesired  = "desired"
	RefDeployed = "deployed"
	RefLastGood = "last_good"
)

// CandidateRefName is the per-candidate branch name (section 46).
func CandidateRefName(candidateID string) string { return "candidate/" + candidateID }

// RollbackRefName is the per-rollback pointer name.
func RollbackRefName(revisionID string) string { return "rollback/" + revisionID }
