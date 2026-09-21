package projection

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ---------- events ----------

// EventExists reports whether an event id is already indexed. The ingress path
// uses this for idempotency (section 68).
func EventExists(tx *sql.Tx, eventID string) (bool, error) {
	var one int
	err := tx.QueryRow(`SELECT 1 FROM events WHERE event_id = ?`, eventID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// InsertEvent indexes an accepted event.
func InsertEvent(tx *sql.Tx, e Event) error {
	_, err := tx.Exec(`
		INSERT INTO events
			(event_id, application_id, junction_id, installation_id, type, source, subject, feedback_type,
			 reason_code, fingerprint, producer_instance, contract_revision, config_revision,
			 config_hash, input_ref, replay_ref, session_id, replayed_by_session_id, durability,
			 data, occurred_at, received_at, ledger_sequence)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_id) DO NOTHING`,
		e.EventID, e.ApplicationID, e.JunctionID, e.InstallationID, e.Type, e.Source, e.Subject, e.FeedbackType,
		e.ReasonCode, e.Fingerprint, e.ProducerInstance, e.ContractRevision, e.ConfigRevision,
		e.ConfigHash, e.InputRef, e.ReplayRef, e.SessionID, e.ReplayedBySessionID,
		e.Durability, orEmptyJSON(e.Data),
		ts(e.OccurredAt), ts(e.ReceivedAt), e.LedgerSequence)
	return err
}

const eventCols = `event_id, application_id, junction_id, installation_id, type, source, subject, feedback_type,
	reason_code, fingerprint, producer_instance, contract_revision, config_revision,
	config_hash, input_ref, replay_ref, session_id, replayed_by_session_id, durability,
	data, occurred_at, received_at, ledger_sequence`

func scanEvent(row interface{ Scan(...any) error }) (Event, error) {
	var e Event
	var data, occurred, received string
	if err := row.Scan(&e.EventID, &e.ApplicationID, &e.JunctionID, &e.InstallationID, &e.Type, &e.Source,
		&e.Subject, &e.FeedbackType, &e.ReasonCode, &e.Fingerprint, &e.ProducerInstance,
		&e.ContractRevision, &e.ConfigRevision, &e.ConfigHash, &e.InputRef, &e.ReplayRef,
		&e.SessionID, &e.ReplayedBySessionID, &e.Durability, &data, &occurred, &received,
		&e.LedgerSequence); err != nil {
		return e, err
	}
	e.Data = data
	e.OccurredAt, e.ReceivedAt = parseTS(occurred), parseTS(received)
	return e, nil
}

// GetEvent loads one event.
func (db *DB) GetEvent(id string) (Event, error) {
	e, err := scanEvent(db.sql.QueryRow(`SELECT `+eventCols+` FROM events WHERE event_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return e, NotFoundf("event %s", id)
	}
	return e, err
}

// ListEvents lists recent events, newest first.
func (db *DB) ListEvents(applicationID string, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT ` + eventCols + ` FROM events`
	args := []any{}
	if applicationID != "" {
		q += ` WHERE application_id = ?`
		args = append(args, applicationID)
	}
	q += ` ORDER BY received_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// CountEvents returns the number of indexed events.
func (db *DB) CountEvents() (int, error) {
	var n int
	err := db.sql.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n)
	return n, err
}

// ---------- issues ----------

// UpsertIssue writes an issue row; replay replaces projection state.
func UpsertIssue(tx *sql.Tx, i Issue) error {
	_, err := tx.Exec(`
		INSERT INTO issues
			(issue_id, application_id, junction_id, feedback_type, reason_code, fingerprint,
			 pattern, status, severity, priority, occurrence_count, unique_sources,
			 controlling_revision, first_seen, last_seen, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(issue_id) DO UPDATE SET
			status = excluded.status,
			severity = excluded.severity,
			priority = excluded.priority,
			occurrence_count = excluded.occurrence_count,
			unique_sources = excluded.unique_sources,
			controlling_revision = excluded.controlling_revision,
			last_seen = excluded.last_seen,
			updated_at = excluded.updated_at`,
		i.IssueID, i.ApplicationID, i.JunctionID, i.FeedbackType, i.ReasonCode, i.Fingerprint,
		i.Pattern, i.Status, i.Severity, i.Priority, i.OccurrenceCount, i.UniqueSources,
		i.ControllingRevision, ts(i.FirstSeen), ts(i.LastSeen), ts(i.CreatedAt), ts(i.UpdatedAt))
	return err
}

const issueCols = `issue_id, application_id, junction_id, feedback_type, reason_code, fingerprint,
	pattern, status, severity, priority, occurrence_count, unique_sources, controlling_revision,
	first_seen, last_seen, created_at, updated_at`

func scanIssue(row interface{ Scan(...any) error }) (Issue, error) {
	var i Issue
	var first, last, created, updated string
	if err := row.Scan(&i.IssueID, &i.ApplicationID, &i.JunctionID, &i.FeedbackType,
		&i.ReasonCode, &i.Fingerprint, &i.Pattern, &i.Status, &i.Severity, &i.Priority,
		&i.OccurrenceCount, &i.UniqueSources, &i.ControllingRevision, &first, &last,
		&created, &updated); err != nil {
		return i, err
	}
	i.FirstSeen, i.LastSeen = parseTS(first), parseTS(last)
	i.CreatedAt, i.UpdatedAt = parseTS(created), parseTS(updated)
	return i, nil
}

// GetIssueByFingerprint finds the issue grouping a fingerprint (section 34).
func (db *DB) GetIssueByFingerprint(fp string) (Issue, error) {
	i, err := scanIssue(db.sql.QueryRow(`SELECT `+issueCols+` FROM issues WHERE fingerprint = ?`, fp))
	if errors.Is(err, sql.ErrNoRows) {
		return i, fmt.Errorf("%w: issue with fingerprint %s", ErrNotFound, fp)
	}
	return i, err
}

// GetIssue loads one issue.
func (db *DB) GetIssue(id string) (Issue, error) {
	i, err := scanIssue(db.sql.QueryRow(`SELECT `+issueCols+` FROM issues WHERE issue_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return i, NotFoundf("issue %s", id)
	}
	return i, err
}

// ListIssues lists issues. statusFilter may be empty for all; order may be
// "count" (default, highest occurrence first) or "recent".
func (db *DB) ListIssues(statusFilter string, limit int, order string) ([]Issue, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT ` + issueCols + ` FROM issues`
	args := []any{}
	if statusFilter != "" {
		q += ` WHERE status = ?`
		args = append(args, statusFilter)
	}
	switch order {
	case "recent":
		q += ` ORDER BY last_seen DESC`
	default:
		q += ` ORDER BY occurrence_count DESC, last_seen DESC`
	}
	q += ` LIMIT ?`
	args = append(args, limit)

	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Issue
	for rows.Next() {
		i, err := scanIssue(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

// SetIssueStatus changes the mutable projection state of an issue. The
// transition itself is journaled by the engine as an immutable record
// (section 37).
func SetIssueStatus(tx *sql.Tx, issueID, status string, now time.Time) error {
	res, err := tx.Exec(`UPDATE issues SET status = ?, updated_at = ? WHERE issue_id = ?`,
		status, ts(now), issueID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return NotFoundf("issue %s", issueID)
	}
	return nil
}

// RecordOccurrence links an event to an issue. It reports whether this event
// was newly counted, so replay stays idempotent.
func RecordOccurrence(tx *sql.Tx, occ Occurrence) (bool, error) {
	res, err := tx.Exec(`INSERT INTO issue_occurrences (issue_id, event_id, seen_at, producer)
		VALUES (?, ?, ?, ?) ON CONFLICT(issue_id, event_id) DO NOTHING`,
		occ.IssueID, occ.EventID, ts(occ.SeenAt), occ.Producer)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// RecomputeIssueCounters refreshes occurrence and distinct-source counters from
// the occurrence table, so counters can never drift on replay.
func RecomputeIssueCounters(tx *sql.Tx, issueID string, now time.Time) error {
	var count, sources int
	if err := tx.QueryRow(`SELECT COUNT(*), COUNT(DISTINCT producer)
		FROM issue_occurrences WHERE issue_id = ?`, issueID).Scan(&count, &sources); err != nil {
		return err
	}
	_, err := tx.Exec(`UPDATE issues SET occurrence_count = ?, unique_sources = ?, updated_at = ?
		WHERE issue_id = ?`, count, sources, ts(now), issueID)
	return err
}

// CountIssuesByStatus returns issue counts per lifecycle status.
func (db *DB) CountIssuesByStatus() (map[string]int, error) {
	rows, err := db.sql.Query(`SELECT status, COUNT(*) FROM issues GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		out[status] = n
	}
	return out, rows.Err()
}
