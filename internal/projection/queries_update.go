package projection

import (
	"database/sql"
	"errors"
	"time"
)

// Update delivery queries. Dispositions are projection state; application
// results are canonical history replayed from the ledger (mission sections 33,
// 40).

// UpsertUpdateDisposition records or advances one installation's relationship
// to one revision.
func UpsertUpdateDisposition(tx *sql.Tx, d UpdateDisposition) error {
	_, err := tx.Exec(`
		INSERT INTO update_dispositions
			(application_id, installation_id, config_family_id, revision_id, content_hash,
			 approval_id, state, reason_code, fetched_at, observed_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(application_id, installation_id, revision_id) DO UPDATE SET
			state = excluded.state,
			reason_code = excluded.reason_code,
			fetched_at = CASE WHEN excluded.fetched_at <> '' THEN excluded.fetched_at ELSE update_dispositions.fetched_at END,
			observed_at = CASE WHEN excluded.observed_at <> '' THEN excluded.observed_at ELSE update_dispositions.observed_at END,
			updated_at = excluded.updated_at`,
		d.ApplicationID, d.InstallationID, d.ConfigFamilyID, d.RevisionID, d.ContentHash,
		d.ApprovalID, d.State, d.ReasonCode, tsOrEmpty(d.FetchedAt), tsOrEmpty(d.ObservedAt), ts(d.UpdatedAt))
	return err
}

// GetUpdateDispositionTx reads one installation's disposition for a revision.
// A missing row means "never offered", which is different from AVAILABLE.
func GetUpdateDispositionTx(tx *sql.Tx, applicationID, installationID, revisionID string) (UpdateDisposition, error) {
	var d UpdateDisposition
	var fetched, observed, updated string
	err := tx.QueryRow(`SELECT application_id, installation_id, config_family_id, revision_id,
		content_hash, approval_id, state, reason_code, fetched_at, observed_at, updated_at
		FROM update_dispositions
		WHERE application_id = ? AND installation_id = ? AND revision_id = ?`,
		applicationID, installationID, revisionID).
		Scan(&d.ApplicationID, &d.InstallationID, &d.ConfigFamilyID, &d.RevisionID,
			&d.ContentHash, &d.ApprovalID, &d.State, &d.ReasonCode, &fetched, &observed, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return d, NotFoundf("update disposition for %s/%s", installationID, revisionID)
	}
	if err != nil {
		return d, err
	}
	if fetched != "" {
		d.FetchedAt = parseTS(fetched)
	}
	if observed != "" {
		d.ObservedAt = parseTS(observed)
	}
	d.UpdatedAt = parseTS(updated)
	return d, nil
}

// ListUpdateDispositions lists dispositions with optional filters.
func (db *DB) ListUpdateDispositions(applicationID, installationID, state string, limit int) ([]UpdateDisposition, error) {
	if limit <= 0 {
		limit = 100
	}
	q := `SELECT application_id, installation_id, config_family_id, revision_id, content_hash,
	             approval_id, state, reason_code, fetched_at, observed_at, updated_at
	      FROM update_dispositions`
	where := []string{}
	args := []any{}
	if applicationID != "" {
		where = append(where, "application_id = ?")
		args = append(args, applicationID)
	}
	if installationID != "" {
		where = append(where, "installation_id = ?")
		args = append(args, installationID)
	}
	if state != "" {
		where = append(where, "state = ?")
		args = append(args, state)
	}
	if len(where) > 0 {
		q += " WHERE " + joinAnd(where)
	}
	q += ` ORDER BY updated_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UpdateDisposition
	for rows.Next() {
		var d UpdateDisposition
		var fetched, observed, updated string
		if err := rows.Scan(&d.ApplicationID, &d.InstallationID, &d.ConfigFamilyID, &d.RevisionID,
			&d.ContentHash, &d.ApprovalID, &d.State, &d.ReasonCode, &fetched, &observed, &updated); err != nil {
			return nil, err
		}
		if fetched != "" {
			d.FetchedAt = parseTS(fetched)
		}
		if observed != "" {
			d.ObservedAt = parseTS(observed)
		}
		d.UpdatedAt = parseTS(updated)
		out = append(out, d)
	}
	return out, rows.Err()
}

func joinAnd(parts []string) string {
	out := ""
	for i, part := range parts {
		if i > 0 {
			out += " AND "
		}
		out += part
	}
	return out
}

// InsertApplicationResult indexes a node's application result.
func InsertApplicationResult(tx *sql.Tx, r ApplicationResult) error {
	_, err := tx.Exec(`
		INSERT INTO application_results
			(result_id, application_id, installation_id, config_family_id, revision_id,
			 content_hash, approval_id, previous_revision_id, observed_hash, result,
			 reason_code, message, details, session_id, process_id, peer_uid, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(result_id) DO NOTHING`,
		r.ResultID, r.ApplicationID, r.InstallationID, r.ConfigFamilyID, r.RevisionID,
		r.ContentHash, r.ApprovalID, r.PreviousRevisionID, r.ObservedHash, r.Result,
		r.ReasonCode, r.Message, orEmptyJSON(r.Details), r.SessionID, r.ProcessID,
		r.PeerUID, ts(r.CreatedAt))
	return err
}

const applicationResultCols = `result_id, application_id, installation_id, config_family_id,
	revision_id, content_hash, approval_id, previous_revision_id, observed_hash, result,
	reason_code, message, details, session_id, process_id, peer_uid, created_at`

func scanApplicationResult(row interface{ Scan(...any) error }) (ApplicationResult, error) {
	var r ApplicationResult
	var created string
	if err := row.Scan(&r.ResultID, &r.ApplicationID, &r.InstallationID, &r.ConfigFamilyID,
		&r.RevisionID, &r.ContentHash, &r.ApprovalID, &r.PreviousRevisionID, &r.ObservedHash,
		&r.Result, &r.ReasonCode, &r.Message, &r.Details, &r.SessionID, &r.ProcessID,
		&r.PeerUID, &created); err != nil {
		return r, err
	}
	r.CreatedAt = parseTS(created)
	return r, nil
}

// GetApplicationResult loads one application result.
func (db *DB) GetApplicationResult(id string) (ApplicationResult, error) {
	r, err := scanApplicationResult(db.sql.QueryRow(
		`SELECT `+applicationResultCols+` FROM application_results WHERE result_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, NotFoundf("application result %s", id)
	}
	return r, err
}

// GetApplicationResultTx loads one application result inside a transaction.
func GetApplicationResultTx(tx *sql.Tx, id string) (ApplicationResult, error) {
	r, err := scanApplicationResult(tx.QueryRow(
		`SELECT `+applicationResultCols+` FROM application_results WHERE result_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, NotFoundf("application result %s", id)
	}
	return r, err
}

// ListApplicationResults lists results, newest first.
func (db *DB) ListApplicationResults(applicationID, installationID string, limit int) ([]ApplicationResult, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT ` + applicationResultCols + ` FROM application_results`
	where := []string{}
	args := []any{}
	if applicationID != "" {
		where = append(where, "application_id = ?")
		args = append(args, applicationID)
	}
	if installationID != "" {
		where = append(where, "installation_id = ?")
		args = append(args, installationID)
	}
	if len(where) > 0 {
		q += " WHERE " + joinAnd(where)
	}
	q += ` ORDER BY created_at DESC, result_id LIMIT ?`
	args = append(args, limit)

	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ApplicationResult
	for rows.Next() {
		r, err := scanApplicationResult(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// LatestApplicationResultForRevision returns the most recent result a node
// reported for one revision, which is how duplicate reports are detected.
func LatestApplicationResultForRevision(tx *sql.Tx, applicationID, installationID, revisionID string) (ApplicationResult, error) {
	r, err := scanApplicationResult(tx.QueryRow(`SELECT `+applicationResultCols+` FROM application_results
		WHERE application_id = ? AND installation_id = ? AND revision_id = ?
		ORDER BY created_at DESC LIMIT 1`, applicationID, installationID, revisionID))
	if errors.Is(err, sql.ErrNoRows) {
		return r, NotFoundf("application result for %s/%s", installationID, revisionID)
	}
	return r, err
}

// LatestAppliedRevision returns the newest revision a node confirmed applied,
// which seeds the `observed` ref interpretation on a rebuild.
func (db *DB) LatestAppliedRevision(applicationID, installationID, configFamilyID string) (string, error) {
	var revisionID string
	err := db.sql.QueryRow(`SELECT revision_id FROM application_results
		WHERE application_id = ? AND installation_id = ? AND config_family_id = ? AND result = ?
		ORDER BY created_at DESC LIMIT 1`,
		applicationID, installationID, configFamilyID, ResultApplied).Scan(&revisionID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return revisionID, err
}

// UpdateFamilyManagementMode reads a family's declared management mode.
func FamilyManagementModeTx(tx *sql.Tx, configFamilyID string) (string, error) {
	var mode string
	err := tx.QueryRow(`SELECT management_mode FROM config_families WHERE config_family_id = ?`,
		configFamilyID).Scan(&mode)
	if errors.Is(err, sql.ErrNoRows) {
		return "", NotFoundf("config family %s", configFamilyID)
	}
	if err != nil {
		return "", err
	}
	return NormalizeManagementMode(mode), nil
}

// SetUpdateDispositionTx is a convenience for marking a state transition with
// one timestamp.
func SetUpdateDispositionTx(tx *sql.Tx, d UpdateDisposition, now time.Time) error {
	d.UpdatedAt = now
	return UpsertUpdateDisposition(tx, d)
}
