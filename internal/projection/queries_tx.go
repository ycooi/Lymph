package projection

import (
	"database/sql"
	"errors"
)

// The daemon holds a single SQLite connection so that write ordering is
// deterministic. Everything the engine needs to read *inside* a write
// transaction therefore needs a transaction-scoped accessor: reading through
// the pool while a transaction is open would wait on that same connection.

// GetApplicationTx reads an application inside a transaction.
func GetApplicationTx(tx *sql.Tx, id string) (Application, error) {
	a, err := scanApplicationTx(tx.QueryRow(`
		SELECT `+applicationCols+` FROM applications WHERE application_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return a, NotFoundf("application %s", id)
	}
	return a, err
}

// GetApplicationByNameTx reads an application by name inside a transaction.
func GetApplicationByNameTx(tx *sql.Tx, name string) (Application, error) {
	a, err := scanApplicationTx(tx.QueryRow(`
		SELECT `+applicationCols+` FROM applications WHERE name = ?`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return a, NotFoundf("application named %s", name)
	}
	return a, err
}

func scanApplicationTx(row *sql.Row) (Application, error) {
	var a Application
	var created, updated string
	err := row.Scan(&a.ApplicationID, &a.Name, &a.AppType, &a.Owner, &a.Repository,
		&a.InstallationID, &a.DefaultInstallationID, &a.RegistrationRevision, &created, &updated)
	if err != nil {
		return a, err
	}
	a.CreatedAt, a.UpdatedAt = parseTS(created), parseTS(updated)
	return a, nil
}

// GetJunctionTx reads a junction by identity inside a transaction.
func GetJunctionTx(tx *sql.Tx, id string) (Junction, error) {
	j, err := scanJunction(tx.QueryRow(`SELECT `+junctionCols+` FROM junctions WHERE junction_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return j, NotFoundf("junction %s", id)
	}
	return j, err
}

// GetJunctionByNameTx reads a junction by application and name inside a
// transaction.
func GetJunctionByNameTx(tx *sql.Tx, applicationID, name string) (Junction, error) {
	j, err := scanJunction(tx.QueryRow(
		`SELECT `+junctionCols+` FROM junctions WHERE application_id = ? AND name = ?`, applicationID, name))
	if errors.Is(err, sql.ErrNoRows) {
		return j, NotFoundf("junction %s/%s", applicationID, name)
	}
	return j, err
}

// GetConfigFamilyTx reads a config family by identity inside a transaction.
func GetConfigFamilyTx(tx *sql.Tx, id string) (ConfigFamily, error) {
	f, err := scanFamily(tx.QueryRow(`SELECT `+familyCols+` FROM config_families WHERE config_family_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return f, NotFoundf("config family %s", id)
	}
	return f, err
}

// GetConfigFamilyByNameTx reads a family by application and name inside a
// transaction.
func GetConfigFamilyByNameTx(tx *sql.Tx, applicationID, name string) (ConfigFamily, error) {
	f, err := scanFamily(tx.QueryRow(
		`SELECT `+familyCols+` FROM config_families WHERE application_id = ? AND name = ?`, applicationID, name))
	if errors.Is(err, sql.ErrNoRows) {
		return f, NotFoundf("config family %s/%s", applicationID, name)
	}
	return f, err
}

// GetIssueByFingerprintTx finds the issue for a fingerprint inside a
// transaction (section 34).
func GetIssueByFingerprintTx(tx *sql.Tx, fp string) (Issue, error) {
	i, err := scanIssue(tx.QueryRow(`SELECT `+issueCols+` FROM issues WHERE fingerprint = ?`, fp))
	if errors.Is(err, sql.ErrNoRows) {
		return i, NotFoundf("issue with fingerprint %s", fp)
	}
	return i, err
}

// GetIssueTx reads one issue inside a transaction.
func GetIssueTx(tx *sql.Tx, id string) (Issue, error) {
	i, err := scanIssue(tx.QueryRow(`SELECT `+issueCols+` FROM issues WHERE issue_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return i, NotFoundf("issue %s", id)
	}
	return i, err
}

// GetCandidateTx reads one candidate inside a transaction.
func GetCandidateTx(tx *sql.Tx, id string) (Candidate, error) {
	c, err := scanCandidate(tx.QueryRow(`SELECT `+candidateCols+` FROM candidates WHERE candidate_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return c, NotFoundf("candidate %s", id)
	}
	return c, err
}

// GetRevisionTx reads one revision inside a transaction.
func GetRevisionTx(tx *sql.Tx, id string) (ConfigRevision, error) {
	r, err := scanRevision(tx.QueryRow(`SELECT `+revisionCols+` FROM config_revisions WHERE revision_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, NotFoundf("revision %s", id)
	}
	return r, err
}

// GetRefTx resolves a ref inside a transaction.
func GetRefTx(tx *sql.Tx, applicationID, configFamilyID, installationID, name string) (ConfigRef, error) {
	var r ConfigRef
	var updated string
	err := tx.QueryRow(`SELECT application_id, config_family_id, installation_id, name, revision_id, content_hash,
		updated_at FROM config_refs
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

// GetWorkItemTx reads one work item inside a transaction.
func GetWorkItemTx(tx *sql.Tx, id string) (WorkItem, error) {
	w, err := scanWorkItem(tx.QueryRow(`SELECT `+workCols+` FROM work_items WHERE work_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return w, NotFoundf("work item %s", id)
	}
	return w, err
}

// GetEventTx reads one event inside a transaction.
func GetEventTx(tx *sql.Tx, id string) (Event, error) {
	e, err := scanEvent(tx.QueryRow(`SELECT `+eventCols+` FROM events WHERE event_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return e, NotFoundf("event %s", id)
	}
	return e, err
}
