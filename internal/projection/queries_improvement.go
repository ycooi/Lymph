package projection

import (
	"database/sql"
	"errors"
	"strings"
)

// Improvement results and artifacts are immutable worker output (mission
// sections 6 and 7). This file holds their SQL so that engine/improvement.go
// stays about semantics.

// InsertImprovementResult indexes one immutable worker return.
func InsertImprovementResult(tx *sql.Tx, r ImprovementResult) error {
	_, err := tx.Exec(`
		INSERT INTO improvement_results
			(result_id, work_item_id, workflow_run_id, application_id, issue_ids, worker,
			 attempt, summary, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(result_id) DO NOTHING`,
		r.ResultID, r.WorkItemID, r.WorkflowRunID, r.ApplicationID, encodeStrings(r.IssueIDs),
		r.Worker, r.Attempt, r.Summary, ts(r.CreatedAt))
	return err
}

const improvementResultCols = `result_id, work_item_id, workflow_run_id, application_id,
	issue_ids, worker, attempt, summary, created_at`

func scanImprovementResult(row interface{ Scan(...any) error }) (ImprovementResult, error) {
	var r ImprovementResult
	var issues, created string
	if err := row.Scan(&r.ResultID, &r.WorkItemID, &r.WorkflowRunID, &r.ApplicationID,
		&issues, &r.Worker, &r.Attempt, &r.Summary, &created); err != nil {
		return r, err
	}
	r.IssueIDs = decodeStrings(issues)
	r.CreatedAt = parseTS(created)
	return r, nil
}

// GetImprovementResult loads one result.
func (db *DB) GetImprovementResult(id string) (ImprovementResult, error) {
	r, err := scanImprovementResult(db.sql.QueryRow(
		`SELECT `+improvementResultCols+` FROM improvement_results WHERE result_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, NotFoundf("improvement result %s", id)
	}
	return r, err
}

// GetImprovementResultByWork loads the result a work item returned, if any.
func (db *DB) GetImprovementResultByWork(workItemID string) (ImprovementResult, error) {
	r, err := scanImprovementResult(db.sql.QueryRow(
		`SELECT `+improvementResultCols+` FROM improvement_results WHERE work_item_id = ?`, workItemID))
	if errors.Is(err, sql.ErrNoRows) {
		return r, NotFoundf("improvement result for work item %s", workItemID)
	}
	return r, err
}

// GetImprovementResultTx loads one result inside a transaction.
func GetImprovementResultTx(tx *sql.Tx, id string) (ImprovementResult, error) {
	r, err := scanImprovementResult(tx.QueryRow(
		`SELECT `+improvementResultCols+` FROM improvement_results WHERE result_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, NotFoundf("improvement result %s", id)
	}
	return r, err
}

// GetImprovementResultByWorkTx loads a work item's result inside a transaction.
func GetImprovementResultByWorkTx(tx *sql.Tx, workItemID string) (ImprovementResult, error) {
	r, err := scanImprovementResult(tx.QueryRow(
		`SELECT `+improvementResultCols+` FROM improvement_results WHERE work_item_id = ?`, workItemID))
	if errors.Is(err, sql.ErrNoRows) {
		return r, NotFoundf("improvement result for work item %s", workItemID)
	}
	return r, err
}

// ListImprovementArtifactsTx lists artifacts of one result inside a transaction.
func ListImprovementArtifactsTx(tx *sql.Tx, resultID string) ([]ImprovementArtifact, error) {
	rows, err := tx.Query(`SELECT `+improvementArtifactCols+` FROM improvement_artifacts
		WHERE result_id = ? ORDER BY artifact_id`, resultID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ImprovementArtifact
	for rows.Next() {
		a, err := scanImprovementArtifact(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ListInstallationsTx lists installations inside a transaction.
func ListInstallationsTx(tx *sql.Tx, applicationID string) ([]Installation, error) {
	q := `SELECT ` + installationCols + ` FROM installations`
	args := []any{}
	if applicationID != "" {
		q += ` WHERE application_id = ?`
		args = append(args, applicationID)
	}
	q += ` ORDER BY application_id, name, installation_id`

	rows, err := tx.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Installation
	for rows.Next() {
		in, err := scanInstallation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

// ListImprovementResults lists results, newest first, optionally filtered by
// application.
func (db *DB) ListImprovementResults(applicationID string, limit int) ([]ImprovementResult, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT ` + improvementResultCols + ` FROM improvement_results`
	args := []any{}
	if applicationID != "" {
		q += ` WHERE application_id = ?`
		args = append(args, applicationID)
	}
	q += ` ORDER BY created_at DESC, result_id LIMIT ?`
	args = append(args, limit)

	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ImprovementResult
	for rows.Next() {
		r, err := scanImprovementResult(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// InsertImprovementArtifact indexes one artifact of one result.
func InsertImprovementArtifact(tx *sql.Tx, a ImprovementArtifact) error {
	_, err := tx.Exec(`
		INSERT INTO improvement_artifacts
			(artifact_id, result_id, application_id, config_family_id, installation_id, kind,
			 candidate_id, content_hash, payload, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(artifact_id) DO NOTHING`,
		a.ArtifactID, a.ResultID, a.ApplicationID, a.ConfigFamilyID, a.InstallationID, a.Kind,
		a.CandidateID, a.ContentHash, orEmptyJSON(a.Payload), ts(a.CreatedAt))
	return err
}

const improvementArtifactCols = `artifact_id, result_id, application_id, config_family_id,
	installation_id, kind, candidate_id, content_hash, payload, created_at`

func scanImprovementArtifact(row interface{ Scan(...any) error }) (ImprovementArtifact, error) {
	var a ImprovementArtifact
	var created string
	if err := row.Scan(&a.ArtifactID, &a.ResultID, &a.ApplicationID, &a.ConfigFamilyID,
		&a.InstallationID, &a.Kind, &a.CandidateID, &a.ContentHash, &a.Payload, &created); err != nil {
		return a, err
	}
	a.CreatedAt = parseTS(created)
	return a, nil
}

// GetImprovementArtifact loads one artifact.
func (db *DB) GetImprovementArtifact(id string) (ImprovementArtifact, error) {
	a, err := scanImprovementArtifact(db.sql.QueryRow(
		`SELECT `+improvementArtifactCols+` FROM improvement_artifacts WHERE artifact_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return a, NotFoundf("improvement artifact %s", id)
	}
	return a, err
}

// ArtifactFilter narrows an artifact listing.
type ArtifactFilter struct {
	ApplicationID string
	Kind          string
	WorkItemID    string
	ResultID      string
	CandidateID   string
	Limit         int
}

// ListImprovementArtifacts lists artifacts, newest first, with optional filters.
func (db *DB) ListImprovementArtifacts(filter ArtifactFilter) ([]ImprovementArtifact, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}

	q := `SELECT ` + improvementArtifactCols + ` FROM improvement_artifacts`
	where := []string{}
	args := []any{}
	if filter.ApplicationID != "" {
		where = append(where, "application_id = ?")
		args = append(args, filter.ApplicationID)
	}
	if filter.Kind != "" {
		where = append(where, "kind = ?")
		args = append(args, filter.Kind)
	}
	if filter.ResultID != "" {
		where = append(where, "result_id = ?")
		args = append(args, filter.ResultID)
	}
	if filter.CandidateID != "" {
		where = append(where, "candidate_id = ?")
		args = append(args, filter.CandidateID)
	}
	if filter.WorkItemID != "" {
		q += ` JOIN improvement_results r ON r.result_id = improvement_artifacts.result_id`
		where = append(where, "r.work_item_id = ?")
		args = append(args, filter.WorkItemID)
	}
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += ` ORDER BY created_at DESC, artifact_id LIMIT ?`
	args = append(args, limit)

	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ImprovementArtifact
	for rows.Next() {
		a, err := scanImprovementArtifact(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// CountImprovementArtifactsByKind summarises artifacts for status reporting.
func (db *DB) CountImprovementArtifactsByKind() (map[string]int, error) {
	rows, err := db.sql.Query(`SELECT kind, COUNT(*) FROM improvement_artifacts GROUP BY kind`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var kind string
		var n int
		if err := rows.Scan(&kind, &n); err != nil {
			return nil, err
		}
		out[kind] = n
	}
	return out, rows.Err()
}

// DistinctIssueInstallations reports the installations that produced the events
// behind a set of issues (mission section 60).
//
// Installation identity is occurrence context: two deployments hitting the same
// unknown phrase still belong to one issue, but a repair that changes
// configuration must know which deployments it is for.
func DistinctIssueInstallations(tx *sql.Tx, issueIDs []string) ([]string, error) {
	if len(issueIDs) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(issueIDs))
	args := make([]any, len(issueIDs))
	for i, id := range issueIDs {
		placeholders[i] = "?"
		args[i] = id
	}
	q := `SELECT DISTINCT e.installation_id
	      FROM issue_occurrences o
	      JOIN events e ON e.event_id = o.event_id
	      WHERE o.issue_id IN (` + strings.Join(placeholders, ", ") + `)
	        AND e.installation_id <> ''
	      ORDER BY e.installation_id`
	rows, err := tx.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
