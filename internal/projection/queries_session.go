package projection

import (
	"database/sql"
	"errors"
	"strings"
	"time"
)

// Session queries. Sessions are operational state: they are written and read by
// the daemon, never replayed from the ledger, and never part of the state digest
// that proves disaster-recovery equivalence (mission sections 8, 44).

const sessionCols = `session_id, application_id, installation_id, process_id,
	protocol_version, client_name, client_version, manifest_hash, pid, hostname,
	peer_uid, peer_gid, state, connected_at, last_seen_at, closed_at`

func scanSession(row interface{ Scan(...any) error }) (Session, error) {
	var s Session
	var connected, lastSeen, closed string
	if err := row.Scan(&s.SessionID, &s.ApplicationID, &s.InstallationID, &s.ProcessID,
		&s.ProtocolVersion, &s.ClientName, &s.ClientVersion, &s.ManifestHash, &s.PID,
		&s.Hostname, &s.PeerUID, &s.PeerGID, &s.State, &connected, &lastSeen, &closed); err != nil {
		return s, err
	}
	s.ConnectedAt, s.LastSeenAt = parseTS(connected), parseTS(lastSeen)
	if closed != "" {
		s.ClosedAt = parseTS(closed)
	}
	return s, nil
}

// InsertSession records a new session.
func InsertSession(tx *sql.Tx, s Session) error {
	_, err := tx.Exec(`
		INSERT INTO sessions
			(session_id, application_id, installation_id, process_id, protocol_version,
			 client_name, client_version, manifest_hash, pid, hostname, peer_uid, peer_gid,
			 state, connected_at, last_seen_at, closed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		s.SessionID, s.ApplicationID, s.InstallationID, s.ProcessID, s.ProtocolVersion,
		s.ClientName, s.ClientVersion, s.ManifestHash, s.PID, s.Hostname, s.PeerUID, s.PeerGID,
		s.State, ts(s.ConnectedAt), ts(s.LastSeenAt), tsOrEmpty(s.ClosedAt))
	return err
}

// GetSession loads one session.
func (db *DB) GetSession(id string) (Session, error) {
	s, err := scanSession(db.sql.QueryRow(`SELECT `+sessionCols+` FROM sessions WHERE session_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return s, NotFoundf("session %s", id)
	}
	return s, err
}

// GetSessionTx loads one session inside a transaction.
func GetSessionTx(tx *sql.Tx, id string) (Session, error) {
	s, err := scanSession(tx.QueryRow(`SELECT `+sessionCols+` FROM sessions WHERE session_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return s, NotFoundf("session %s", id)
	}
	return s, err
}

// ListSessions lists sessions newest first, with optional filters.
func (db *DB) ListSessions(applicationID, installationID, state string, limit int) ([]Session, error) {
	if limit <= 0 {
		limit = 100
	}
	q := `SELECT ` + sessionCols + ` FROM sessions`
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
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += ` ORDER BY connected_at DESC, session_id LIMIT ?`
	args = append(args, limit)

	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Session
	for rows.Next() {
		s, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ActiveSessionsForInstallationTx returns the ACTIVE sessions of one
// installation. The duplicate-process policy is decided from this.
func ActiveSessionsForInstallationTx(tx *sql.Tx, applicationID, installationID string) ([]Session, error) {
	rows, err := tx.Query(`SELECT `+sessionCols+` FROM sessions
		WHERE application_id = ? AND installation_id = ? AND state = ?
		ORDER BY connected_at`, applicationID, installationID, SessionActive)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Session
	for rows.Next() {
		s, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// CountSessionsForProcessTx reports how many sessions this exact process has
// held for this installation, in any state.
//
// It is how the daemon tells a first connection from a reconnection: a process
// that has handshaked before is coming back, whether its previous session was
// closed, expired or lost to a restart (mission section 65).
func CountSessionsForProcessTx(tx *sql.Tx, applicationID, installationID, processID string) (int, error) {
	var n int
	err := tx.QueryRow(`SELECT COUNT(*) FROM sessions
		WHERE application_id = ? AND installation_id = ? AND process_id = ?`,
		applicationID, installationID, processID).Scan(&n)
	return n, err
}

// TouchSessionTx records activity on a session. Every accepted event updates
// LastSeenAt; no heartbeat is required (mission sections 21, 36).
func TouchSessionTx(tx *sql.Tx, sessionID string, at time.Time) error {
	res, err := tx.Exec(`UPDATE sessions SET last_seen_at = ? WHERE session_id = ?`, ts(at), sessionID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return NotFoundf("session %s", sessionID)
	}
	return nil
}

// CloseSessionTx moves a session to CLOSED.
func CloseSessionTx(tx *sql.Tx, sessionID string, at time.Time) error {
	res, err := tx.Exec(`UPDATE sessions SET state = ?, closed_at = ? WHERE session_id = ?`,
		SessionClosed, ts(at), sessionID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return NotFoundf("session %s", sessionID)
	}
	return nil
}

// ExpireIdleSessions marks sessions with no activity since the cutoff as STALE
// and returns them. Process crashes are the normal case, so expiry, not close,
// is what cleans up (mission sections 36, 72).
func ExpireIdleSessions(tx *sql.Tx, cutoff time.Time, now time.Time) ([]Session, error) {
	rows, err := tx.Query(`SELECT `+sessionCols+` FROM sessions
		WHERE state = ? AND last_seen_at < ? ORDER BY last_seen_at`, SessionActive, ts(cutoff))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var expired []Session
	for rows.Next() {
		s, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		expired = append(expired, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, s := range expired {
		if _, err := tx.Exec(`UPDATE sessions SET state = ?, closed_at = ? WHERE session_id = ?`,
			SessionStale, ts(now), s.SessionID); err != nil {
			return nil, err
		}
	}
	return expired, nil
}

// DeleteOldSessions removes finished sessions older than the retention cutoff.
// Deleting operational rows is not history loss (mission section 104).
func DeleteOldSessions(tx *sql.Tx, cutoff time.Time) (int64, error) {
	res, err := tx.Exec(`DELETE FROM sessions
		WHERE state IN (?, ?, ?) AND COALESCE(NULLIF(closed_at, ''), last_seen_at) < ?`,
		SessionClosed, SessionStale, SessionRejected, ts(cutoff))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// CountSessionsByState summarises sessions for the status endpoint.
func (db *DB) CountSessionsByState() (map[string]int, error) {
	rows, err := db.sql.Query(`SELECT state, COUNT(*) FROM sessions GROUP BY state`)
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
