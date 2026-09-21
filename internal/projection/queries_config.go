package projection

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ycooi/Lymph/internal/objectstore"
)

// ---------- revisions ----------

// NextRevisionSequence returns the next per-family revision number.
func NextRevisionSequence(tx *sql.Tx, configFamilyID string) (int, error) {
	var max sql.NullInt64
	if err := tx.QueryRow(`SELECT MAX(sequence) FROM config_revisions WHERE config_family_id = ?`,
		configFamilyID).Scan(&max); err != nil {
		return 0, err
	}
	if !max.Valid {
		return 1, nil
	}
	return int(max.Int64) + 1, nil
}

// InsertRevision indexes a revision plus its blob manifest.
func InsertRevision(tx *sql.Tx, rev ConfigRevision, entries []objectstore.Entry) error {
	_, err := tx.Exec(`
		INSERT INTO config_revisions
			(revision_id, application_id, config_family_id, sequence, content_hash, root_tree_hash,
			 parent_revision_id, schema_revision, label, message, issue_ids, work_item_id,
			 candidate_id, author, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(revision_id) DO NOTHING`,
		rev.RevisionID, rev.ApplicationID, rev.ConfigFamilyID, rev.Sequence, rev.ContentHash,
		rev.RootTreeHash, rev.ParentRevisionID, rev.SchemaRevision, rev.Label, rev.Message,
		encodeStrings(rev.IssueIDs), rev.WorkItemID, rev.CandidateID, rev.Author, ts(rev.CreatedAt))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if _, err := tx.Exec(`
			INSERT INTO config_revision_blobs (revision_id, path, blob_hash, mode, size)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(revision_id, path) DO UPDATE SET
				blob_hash = excluded.blob_hash, mode = excluded.mode, size = excluded.size`,
			rev.RevisionID, e.Path, e.Hash, e.Mode, e.Size); err != nil {
			return err
		}
	}
	return nil
}

const revisionCols = `revision_id, application_id, config_family_id, sequence, content_hash,
	root_tree_hash, parent_revision_id, schema_revision, label, message, issue_ids, work_item_id,
	candidate_id, author, created_at`

func scanRevision(row interface{ Scan(...any) error }) (ConfigRevision, error) {
	var r ConfigRevision
	var issues, created string
	if err := row.Scan(&r.RevisionID, &r.ApplicationID, &r.ConfigFamilyID, &r.Sequence,
		&r.ContentHash, &r.RootTreeHash, &r.ParentRevisionID, &r.SchemaRevision, &r.Label,
		&r.Message, &issues, &r.WorkItemID, &r.CandidateID, &r.Author, &created); err != nil {
		return r, err
	}
	r.IssueIDs = decodeStrings(issues)
	r.CreatedAt = parseTS(created)
	return r, nil
}

// GetRevision loads one revision by identity.
func (db *DB) GetRevision(id string) (ConfigRevision, error) {
	r, err := scanRevision(db.sql.QueryRow(`SELECT `+revisionCols+` FROM config_revisions WHERE revision_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, NotFoundf("revision %s", id)
	}
	return r, err
}

// GetRevisionByContentHash finds the revision stored at a content address.
func (db *DB) GetRevisionByContentHash(hash string) (ConfigRevision, error) {
	r, err := scanRevision(db.sql.QueryRow(`SELECT `+revisionCols+` FROM config_revisions
		WHERE content_hash = ? ORDER BY created_at DESC LIMIT 1`, hash))
	if errors.Is(err, sql.ErrNoRows) {
		return r, NotFoundf("revision with content hash %s", hash)
	}
	return r, err
}

// ListRevisions lists the revisions of a family, newest first.
func (db *DB) ListRevisions(configFamilyID string, limit int) ([]ConfigRevision, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT ` + revisionCols + ` FROM config_revisions`
	args := []any{}
	if configFamilyID != "" {
		q += ` WHERE config_family_id = ?`
		args = append(args, configFamilyID)
	}
	q += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConfigRevision
	for rows.Next() {
		r, err := scanRevision(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RevisionBlobs returns the blob manifest of a revision.
func (db *DB) RevisionBlobs(revisionID string) ([]objectstore.Entry, error) {
	rows, err := db.sql.Query(`SELECT path, blob_hash, mode, size FROM config_revision_blobs
		WHERE revision_id = ? ORDER BY path`, revisionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []objectstore.Entry
	for rows.Next() {
		var e objectstore.Entry
		if err := rows.Scan(&e.Path, &e.Hash, &e.Mode, &e.Size); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---------- refs and reflog ----------

// GetRef resolves a ref to its revision.
func (db *DB) GetRef(applicationID, configFamilyID, installationID, name string) (ConfigRef, error) {
	var r ConfigRef
	var updated string
	err := db.sql.QueryRow(`SELECT application_id, config_family_id, installation_id, name, revision_id,
		content_hash, updated_at FROM config_refs
		WHERE application_id = ? AND config_family_id = ? AND installation_id = ? AND name = ?`,
		applicationID, configFamilyID, installationID, name).
		Scan(&r.ApplicationID, &r.ConfigFamilyID, &r.InstallationID, &r.Name, &r.RevisionID,
			&r.ContentHash, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return r, NotFoundf("ref %s/%s/%s", configFamilyID, installationID, name)
	}
	if err != nil {
		return r, err
	}
	r.UpdatedAt = parseTS(updated)
	return r, nil
}

// CurrentRefRevision returns the revision a ref currently points at, or "".
func CurrentRefRevision(tx *sql.Tx, applicationID, configFamilyID, installationID, name string) (string, error) {
	var current string
	err := tx.QueryRow(`SELECT revision_id FROM config_refs
		WHERE application_id = ? AND config_family_id = ? AND installation_id = ? AND name = ?`,
		applicationID, configFamilyID, installationID, name).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return current, err
}

// SetRefTx moves a ref with compare-and-swap semantics (section 22).
//
// When expectedOldRevision is non-nil the move happens only if the ref
// currently points at exactly that revision. A mismatch returns
// ErrStaleCandidate: the caller's candidate was built on a revision that is no
// longer active and must never silently overwrite the newer one.
func SetRefTx(tx *sql.Tx, ref ConfigRef, expectedOldRevision *string) error {
	current, err := CurrentRefRevision(tx, ref.ApplicationID, ref.ConfigFamilyID, ref.InstallationID, ref.Name)
	if err != nil {
		return err
	}
	if expectedOldRevision != nil && *expectedOldRevision != current {
		return fmt.Errorf("%w: ref %s expected %q, found %q",
			ErrStaleCandidate, ref.Name, *expectedOldRevision, current)
	}

	_, err = tx.Exec(`
		INSERT INTO config_refs (application_id, config_family_id, installation_id, name, revision_id, content_hash, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(application_id, config_family_id, installation_id, name) DO UPDATE SET
			revision_id = excluded.revision_id,
			content_hash = excluded.content_hash,
			updated_at = excluded.updated_at`,
		ref.ApplicationID, ref.ConfigFamilyID, ref.InstallationID, ref.Name, ref.RevisionID,
		ref.ContentHash, ts(ref.UpdatedAt))
	return err
}

// ListRefs lists refs, optionally filtered.
func (db *DB) ListRefs(applicationID, configFamilyID, installationID string) ([]ConfigRef, error) {
	q := `SELECT application_id, config_family_id, installation_id, name, revision_id, content_hash, updated_at
	      FROM config_refs`
	args := []any{}
	where := []string{}
	if applicationID != "" {
		where = append(where, "application_id = ?")
		args = append(args, applicationID)
	}
	if configFamilyID != "" {
		where = append(where, "config_family_id = ?")
		args = append(args, configFamilyID)
	}
	if installationID != "" {
		where = append(where, "installation_id = ?")
		args = append(args, installationID)
	}
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += ` ORDER BY config_family_id, installation_id, name`

	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConfigRef
	for rows.Next() {
		var r ConfigRef
		var updated string
		if err := rows.Scan(&r.ApplicationID, &r.ConfigFamilyID, &r.InstallationID, &r.Name,
			&r.RevisionID, &r.ContentHash, &updated); err != nil {
			return nil, err
		}
		r.UpdatedAt = parseTS(updated)
		out = append(out, r)
	}
	return out, rows.Err()
}

// InsertReflog appends one permanent ref movement record (section 21).
func InsertReflog(tx *sql.Tx, e ReflogEntry) error {
	_, err := tx.Exec(`
		INSERT INTO config_reflog
			(log_id, application_id, config_family_id, installation_id, ref_name, old_revision_id,
			 new_revision_id, reason, issue_ids, candidate_id, validation_id, approval_id,
			 deployment_id, ledger_sequence, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(log_id) DO NOTHING`,
		e.LogID, e.ApplicationID, e.ConfigFamilyID, e.InstallationID, e.RefName, e.OldRevisionID,
		e.NewRevisionID, e.Reason, encodeStrings(e.IssueIDs), e.CandidateID, e.ValidationID,
		e.ApprovalID, e.DeploymentID, e.LedgerSequence, ts(e.CreatedAt))
	return err
}

// ListReflog returns ref movements, newest first.
func (db *DB) ListReflog(applicationID, configFamilyID, installationID string, limit int) ([]ReflogEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT log_id, application_id, config_family_id, installation_id, ref_name, old_revision_id,
	             new_revision_id, reason, issue_ids, candidate_id, validation_id, approval_id,
	             deployment_id, ledger_sequence, created_at
	      FROM config_reflog`
	args := []any{}
	where := []string{}
	if applicationID != "" {
		where = append(where, "application_id = ?")
		args = append(args, applicationID)
	}
	if configFamilyID != "" {
		where = append(where, "config_family_id = ?")
		args = append(args, configFamilyID)
	}
	if installationID != "" {
		where = append(where, "installation_id = ?")
		args = append(args, installationID)
	}
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReflogEntry
	for rows.Next() {
		var e ReflogEntry
		var issues, created string
		if err := rows.Scan(&e.LogID, &e.ApplicationID, &e.ConfigFamilyID, &e.InstallationID,
			&e.RefName, &e.OldRevisionID, &e.NewRevisionID, &e.Reason, &issues, &e.CandidateID,
			&e.ValidationID, &e.ApprovalID, &e.DeploymentID, &e.LedgerSequence, &created); err != nil {
			return nil, err
		}
		e.IssueIDs = decodeStrings(issues)
		e.CreatedAt = parseTS(created)
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---------- candidates ----------

// InsertCandidate indexes a config candidate (section 45).
func InsertCandidate(tx *sql.Tx, c Candidate) error {
	_, err := tx.Exec(`
		INSERT INTO candidates
			(candidate_id, application_id, config_family_id, installation_id, base_revision_id, root_tree_hash,
			 state, issue_ids, work_item_id, workflow_run_id, explanation, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(candidate_id) DO UPDATE SET
			state = excluded.state,
			root_tree_hash = excluded.root_tree_hash,
			issue_ids = excluded.issue_ids,
			explanation = excluded.explanation,
			updated_at = excluded.updated_at`,
		c.CandidateID, c.ApplicationID, c.ConfigFamilyID, c.InstallationID, c.BaseRevisionID, c.RootTreeHash,
		c.State, encodeStrings(c.IssueIDs), c.WorkItemID, c.WorkflowRunID, c.Explanation,
		ts(c.CreatedAt), ts(c.UpdatedAt))
	return err
}

const candidateCols = `candidate_id, application_id, config_family_id, installation_id, base_revision_id,
	root_tree_hash, state, issue_ids, work_item_id, workflow_run_id, explanation, created_at, updated_at`

func scanCandidate(row interface{ Scan(...any) error }) (Candidate, error) {
	var c Candidate
	var issues, created, updated string
	if err := row.Scan(&c.CandidateID, &c.ApplicationID, &c.ConfigFamilyID, &c.InstallationID,
		&c.BaseRevisionID, &c.RootTreeHash, &c.State, &issues, &c.WorkItemID, &c.WorkflowRunID, &c.Explanation,
		&created, &updated); err != nil {
		return c, err
	}
	c.IssueIDs = decodeStrings(issues)
	c.CreatedAt, c.UpdatedAt = parseTS(created), parseTS(updated)
	return c, nil
}

// GetCandidate loads one candidate.
func (db *DB) GetCandidate(id string) (Candidate, error) {
	c, err := scanCandidate(db.sql.QueryRow(`SELECT `+candidateCols+` FROM candidates WHERE candidate_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return c, NotFoundf("candidate %s", id)
	}
	return c, err
}

// SetCandidateState writes an explicit state machine value (section 94).
func SetCandidateState(tx *sql.Tx, candidateID, state string, now time.Time) error {
	res, err := tx.Exec(`UPDATE candidates SET state = ?, updated_at = ? WHERE candidate_id = ?`,
		state, ts(now), candidateID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return NotFoundf("candidate %s", candidateID)
	}
	return nil
}

// ListCandidates lists candidates, optionally filtered by family and state.
func (db *DB) ListCandidates(configFamilyID, state string, limit int) ([]Candidate, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT ` + candidateCols + ` FROM candidates`
	args := []any{}
	where := []string{}
	if configFamilyID != "" {
		where = append(where, "config_family_id = ?")
		args = append(args, configFamilyID)
	}
	if state != "" {
		where = append(where, "state = ?")
		args = append(args, state)
	}
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Candidate
	for rows.Next() {
		c, err := scanCandidate(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
