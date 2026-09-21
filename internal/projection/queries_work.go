package projection

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ycooi/Lymph/internal/ledger"
)

// ---------- work items ----------

func tsOrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return tsFixed(t)
}

// tsFixed renders a timestamp with a fixed-width fractional part.
//
// Lease deadlines are compared as text in SQL, so the format must sort
// chronologically. RFC3339Nano trims trailing zeros, which makes "…:00Z" sort
// after "…:00.5Z"; nine fixed digits remove that trap.
func tsFixed(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

// InsertWorkItem indexes a work item.
func InsertWorkItem(tx *sql.Tx, w WorkItem) error {
	_, err := tx.Exec(`
		INSERT INTO work_items
			(work_id, workflow_type, application_id, junction_id, config_family_id, installation_id,
			 base_revision_id, issue_ids, package, state, lease_owner, lease_expires_at,
			 attempts, result, improvement_result_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(work_id) DO UPDATE SET
			state = excluded.state,
			lease_owner = excluded.lease_owner,
			lease_expires_at = excluded.lease_expires_at,
			attempts = excluded.attempts,
			result = excluded.result,
			improvement_result_id = excluded.improvement_result_id,
			updated_at = excluded.updated_at`,
		w.WorkID, w.WorkflowType, w.ApplicationID, w.JunctionID, w.ConfigFamilyID, w.InstallationID,
		w.BaseRevisionID, encodeStrings(w.IssueIDs), orEmptyJSON(w.Package), w.State,
		w.LeaseOwner, tsOrEmpty(w.LeaseExpiresAt), w.Attempts, orEmptyJSON(w.Result),
		w.ImprovementResultID, ts(w.CreatedAt), ts(w.UpdatedAt))
	return err
}

const workCols = `work_id, workflow_type, application_id, junction_id, config_family_id, installation_id,
	base_revision_id, issue_ids, package, state, lease_owner, lease_expires_at, attempts,
	result, improvement_result_id, created_at, updated_at`

func scanWorkItem(row interface{ Scan(...any) error }) (WorkItem, error) {
	var w WorkItem
	var issues, pkg, leaseExpires, result, created, updated string
	if err := row.Scan(&w.WorkID, &w.WorkflowType, &w.ApplicationID, &w.JunctionID,
		&w.ConfigFamilyID, &w.InstallationID, &w.BaseRevisionID, &issues, &pkg, &w.State,
		&w.LeaseOwner, &leaseExpires, &w.Attempts, &result, &w.ImprovementResultID,
		&created, &updated); err != nil {
		return w, err
	}
	w.IssueIDs = decodeStrings(issues)
	w.Package = pkg
	w.Result = result
	if leaseExpires != "" {
		w.LeaseExpiresAt = parseTS(leaseExpires)
	}
	w.CreatedAt, w.UpdatedAt = parseTS(created), parseTS(updated)
	return w, nil
}

// GetWorkItem loads one work item.
func (db *DB) GetWorkItem(id string) (WorkItem, error) {
	w, err := scanWorkItem(db.sql.QueryRow(`SELECT `+workCols+` FROM work_items WHERE work_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return w, NotFoundf("work item %s", id)
	}
	return w, err
}

// ListWorkItems lists work items, optionally filtered by state.
func (db *DB) ListWorkItems(state string, limit int) ([]WorkItem, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT ` + workCols + ` FROM work_items`
	args := []any{}
	if state != "" {
		q += ` WHERE state = ?`
		args = append(args, state)
	}
	q += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WorkItem
	for rows.Next() {
		w, err := scanWorkItem(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// ClaimableWorkItem finds the oldest queued item (or an expired lease) whose
// workflow type is in the worker's capability set (section 41, section 42).
//
// The caller runs this inside a transaction so the claim is atomic: exactly one
// worker can move the row from QUEUED to LEASED.
func ClaimableWorkItem(tx *sql.Tx, workflowTypes []string, now time.Time) (WorkItem, error) {
	// QUEUED work is claimable; a lease that passed its deadline is claimable
	// again (at-least-once delivery, section 42).
	clauses := []string{"(state = 'QUEUED' OR (state = 'LEASED' AND lease_expires_at <> '' AND lease_expires_at < ?))"}
	args := []any{tsFixed(now)}
	if len(workflowTypes) > 0 {
		placeholders := make([]string, len(workflowTypes))
		for i, wt := range workflowTypes {
			placeholders[i] = "?"
			args = append(args, wt)
		}
		clauses = append(clauses, "workflow_type IN ("+strings.Join(placeholders, ", ")+")")
	}
	q := `SELECT ` + workCols + ` FROM work_items WHERE ` + strings.Join(clauses, " AND ") +
		` ORDER BY created_at ASC LIMIT 1`
	w, err := scanWorkItem(tx.QueryRow(q, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return w, ErrNoWork
	}
	return w, err
}

// CountWorkItemsByState returns counts per work state.
func (db *DB) CountWorkItemsByState() (map[string]int, error) {
	rows, err := db.sql.Query(`SELECT state, COUNT(*) FROM work_items GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		out[state] = n
	}
	return out, rows.Err()
}

// ExpiredLeases lists leases that passed their deadline, for the reconciliation
// pass that returns abandoned work to the queue (section 42, section 87).
func ExpiredLeases(tx *sql.Tx, now time.Time) ([]WorkItem, error) {
	rows, err := tx.Query(`SELECT `+workCols+` FROM work_items
		WHERE state = 'LEASED' AND lease_expires_at <> '' AND lease_expires_at < ?`, tsFixed(now))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WorkItem
	for rows.Next() {
		w, err := scanWorkItem(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// ---------- audit ----------

// InsertAuditTx appends an audit event inside a transaction.
func InsertAuditTx(tx *sql.Tx, e AuditEvent) error {
	_, err := tx.Exec(`
		INSERT INTO audit_events
			(audit_id, action, actor, application_id, subject_kind, subject_id, detail,
			 ledger_sequence, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(audit_id) DO NOTHING`,
		e.AuditID, e.Action, e.Actor, e.ApplicationID, e.SubjectKind, e.SubjectID,
		orEmptyJSON(e.Detail), e.LedgerSequence, ts(e.CreatedAt))
	return err
}

// ListAudit returns recent audit events, newest first.
func (db *DB) ListAudit(limit int) ([]AuditEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := db.sql.Query(`SELECT audit_id, action, actor, application_id, subject_kind,
		subject_id, detail, ledger_sequence, created_at FROM audit_events
		ORDER BY created_at DESC, audit_id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuditEvent
	for rows.Next() {
		var e AuditEvent
		var created string
		if err := rows.Scan(&e.AuditID, &e.Action, &e.Actor, &e.ApplicationID, &e.SubjectKind,
			&e.SubjectID, &e.Detail, &e.LedgerSequence, &created); err != nil {
			return nil, err
		}
		e.CreatedAt = parseTS(created)
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---------- ledger index ----------

// IndexLedgerRecordTx records one ledger record's position in the projection.
func IndexLedgerRecordTx(tx *sql.Tx, rec ledger.Record) error {
	_, err := tx.Exec(`INSERT INTO ledger_index (sequence, kind, object_id, record_hash, created_at)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT(sequence) DO NOTHING`,
		rec.Sequence, string(rec.Kind), rec.ObjectID, rec.RecordHash, rec.Timestamp)
	return err
}

// IndexedLedgerSequence returns the highest ledger sequence present in the
// projection.
func (db *DB) IndexedLedgerSequence() (uint64, error) {
	var seq sql.NullInt64
	if err := db.sql.QueryRow(`SELECT MAX(sequence) FROM ledger_index`).Scan(&seq); err != nil {
		return 0, err
	}
	if !seq.Valid {
		return 0, nil
	}
	return uint64(seq.Int64), nil
}

// ---------- validations and approvals (L2 placeholders) ----------

// Validation records one test workflow result (section 48).
type Validation struct {
	ValidationID   string    `json:"validation_id"`
	CandidateID    string    `json:"candidate_id"`
	ApplicationID  string    `json:"application_id"`
	ConfigFamilyID string    `json:"config_family_id"`
	SuiteID        string    `json:"suite_id,omitempty"`
	SuiteVersion   string    `json:"suite_version,omitempty"`
	Kind           string    `json:"kind"`
	Result         string    `json:"result"`
	Metrics        string    `json:"metrics,omitempty"`
	Artifacts      string    `json:"artifacts,omitempty"`
	InputsHash     string    `json:"inputs_hash,omitempty"`
	Environment    string    `json:"environment,omitempty"`
	Worker         string    `json:"worker,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

// InsertValidation indexes a validation result.
func InsertValidation(tx *sql.Tx, v Validation) error {
	_, err := tx.Exec(`
		INSERT INTO validations
			(validation_id, candidate_id, application_id, config_family_id, suite_id,
			 suite_version, kind, result, metrics, artifacts, inputs_hash, environment,
			 worker, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(validation_id) DO NOTHING`,
		v.ValidationID, v.CandidateID, v.ApplicationID, v.ConfigFamilyID, v.SuiteID,
		v.SuiteVersion, v.Kind, v.Result, orEmptyJSON(v.Metrics), orEmptyJSON(v.Artifacts),
		v.InputsHash, v.Environment, v.Worker, ts(v.CreatedAt))
	return err
}

// ListValidations returns the validations recorded for a candidate.
func (db *DB) ListValidations(candidateID string) ([]Validation, error) {
	rows, err := db.sql.Query(`SELECT validation_id, candidate_id, application_id, config_family_id,
		suite_id, suite_version, kind, result, metrics, artifacts, inputs_hash, environment,
		worker, created_at FROM validations WHERE candidate_id = ? ORDER BY created_at`, candidateID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Validation
	for rows.Next() {
		var v Validation
		var created string
		if err := rows.Scan(&v.ValidationID, &v.CandidateID, &v.ApplicationID, &v.ConfigFamilyID,
			&v.SuiteID, &v.SuiteVersion, &v.Kind, &v.Result, &v.Metrics, &v.Artifacts,
			&v.InputsHash, &v.Environment, &v.Worker, &created); err != nil {
			return nil, err
		}
		v.CreatedAt = parseTS(created)
		out = append(out, v)
	}
	return out, rows.Err()
}

// Approval records a human (or explicitly permitted automatic) promotion
// decision (section 50).
type Approval struct {
	ApprovalID  string `json:"approval_id"`
	CandidateID string `json:"candidate_id"`
	Approver    string `json:"approver"`
	// ActorType separates a human decision from a machine one. Only HUMAN
	// approvals authorise delivery: a worker that could approve its own
	// candidate would make the gate decorative (mission sections 2, 20).
	ActorType string `json:"actor_type"`
	Decision  string `json:"decision"`
	Comment   string `json:"comment,omitempty"`

	// An approval binds to the exact content and target it authorises, never to
	// a candidate id that could later be repointed (mission section 3).
	ApplicationID  string   `json:"application_id,omitempty"`
	InstallationID string   `json:"installation_id,omitempty"`
	ConfigFamilyID string   `json:"config_family_id,omitempty"`
	ContentHash    string   `json:"content_hash,omitempty"`
	BaseRevisionID string   `json:"base_revision_id,omitempty"`
	ValidationIDs  []string `json:"validation_ids,omitempty"`
	PeerUID        int      `json:"peer_uid,omitempty"`

	CreatedAt time.Time `json:"created_at"`
}

// InsertApproval indexes an approval.
func InsertApproval(tx *sql.Tx, a Approval) error {
	_, err := tx.Exec(`
		INSERT INTO approvals
			(approval_id, candidate_id, approver, actor_type, decision, comment,
			 application_id, installation_id, config_family_id, content_hash,
			 base_revision_id, validation_ids, peer_uid, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(approval_id) DO NOTHING`,
		a.ApprovalID, a.CandidateID, a.Approver, orHuman(a.ActorType), a.Decision, a.Comment,
		a.ApplicationID, a.InstallationID, a.ConfigFamilyID, a.ContentHash,
		a.BaseRevisionID, encodeStrings(a.ValidationIDs), a.PeerUID, ts(a.CreatedAt))
	return err
}

// orHuman defaults an unlabelled approval to SYSTEM, not HUMAN.
//
// Any approval record written before actor types existed is of unknown origin,
// and the mission is explicit that unknown origin must not be silently read as
// human (mission section 20). It therefore fails the delivery gate and needs a
// fresh human approval, which is the safe direction.
func orHuman(actorType string) string {
	if actorType == "" {
		return ActorSystem
	}
	return actorType
}

const approvalCols = `approval_id, candidate_id, approver, actor_type, decision, comment,
	application_id, installation_id, config_family_id, content_hash, base_revision_id,
	validation_ids, peer_uid, created_at`

func scanApproval(row interface{ Scan(...any) error }) (Approval, error) {
	var a Approval
	var validations, created string
	if err := row.Scan(&a.ApprovalID, &a.CandidateID, &a.Approver, &a.ActorType, &a.Decision,
		&a.Comment, &a.ApplicationID, &a.InstallationID, &a.ConfigFamilyID, &a.ContentHash,
		&a.BaseRevisionID, &validations, &a.PeerUID, &created); err != nil {
		return a, err
	}
	a.ValidationIDs = decodeStrings(validations)
	a.CreatedAt = parseTS(created)
	return a, nil
}

// GetApproval loads one approval by identity.
func (db *DB) GetApproval(id string) (Approval, error) {
	a, err := scanApproval(db.sql.QueryRow(`SELECT `+approvalCols+` FROM approvals WHERE approval_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return a, NotFoundf("approval %s", id)
	}
	return a, err
}

// GetApprovalTx loads one approval inside a transaction.
func GetApprovalTx(tx *sql.Tx, id string) (Approval, error) {
	a, err := scanApproval(tx.QueryRow(`SELECT `+approvalCols+` FROM approvals WHERE approval_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return a, NotFoundf("approval %s", id)
	}
	return a, err
}

// LatestApproval returns the most recent approval for a candidate.
func (db *DB) LatestApproval(candidateID string) (Approval, error) {
	a, err := scanApproval(db.sql.QueryRow(`SELECT `+approvalCols+` FROM approvals
		WHERE candidate_id = ? ORDER BY created_at DESC LIMIT 1`, candidateID))
	if errors.Is(err, sql.ErrNoRows) {
		return a, fmt.Errorf("%w: approval for candidate %s", ErrNotFound, candidateID)
	}
	if err != nil {
		return a, err
	}
	return a, nil
}

// LatestApprovalTx returns the most recent approval for a candidate inside a
// transaction.
func LatestApprovalTx(tx *sql.Tx, candidateID string) (Approval, error) {
	a, err := scanApproval(tx.QueryRow(`SELECT `+approvalCols+` FROM approvals
		WHERE candidate_id = ? ORDER BY created_at DESC LIMIT 1`, candidateID))
	if errors.Is(err, sql.ErrNoRows) {
		return a, fmt.Errorf("%w: approval for candidate %s", ErrNotFound, candidateID)
	}
	return a, err
}

// ListApprovals returns a candidate's approval history, oldest first. The
// review package and the audit both need the whole history rather than the
// latest decision alone: "approved, then withdrawn" is a different story from
// "never decided" (mission sections 41, 43, 44).
func (db *DB) ListApprovals(candidateID string) ([]Approval, error) {
	rows, err := db.sql.Query(`SELECT `+approvalCols+` FROM approvals
		WHERE candidate_id = ? ORDER BY created_at`, candidateID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Approval
	for rows.Next() {
		a, err := scanApproval(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
