package projection

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver, no CGO (section 16)
)

// SchemaVersion is bumped whenever the projection layout changes.
//
// Version 2 changes the identity of a config ref: configuration state is now
// scoped by installation as well as application, family and ref name. That is a
// primary-key change, which no amount of ALTER TABLE handles honestly. The
// projection is disposable by design, so the upgrade path is: detect the
// mismatch, drop the projection, replay the canonical ledger, carry on.
const SchemaVersion = 3

// Meta keys.
const (
	MetaSchemaVersion = "projection.schema_version"
	MetaInstanceUUID  = "lymph.instance_uuid"
	MetaLedgerSeq     = "projection.ledger_sequence"
	MetaLedgerHash    = "projection.ledger_head_hash"
	MetaBuiltAt       = "projection.built_at"
	MetaRebuilds      = "projection.rebuild_count"
)

// DB is the SQLite projection.
type DB struct {
	sql     *sql.DB
	path    string
	upgrade Upgrade
}

// Upgrade records what happened when the projection was reconciled with the
// binary's expected schema.
type Upgrade struct {
	FromVersion string `json:"from_version,omitempty"`
	ToVersion   string `json:"to_version"`
	Rebuilt     bool   `json:"rebuilt"`
}

// Open opens or creates the projection database at <root>/db/lymph.sqlite.
func Open(root string) (*DB, error) {
	dir := filepath.Join(root, "db")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "lymph.sqlite")
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(0)"
	handle, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// A single writer keeps the daemon's write path deterministic. Readers
	// share the same pool; WAL allows them to proceed during writes.
	handle.SetMaxOpenConns(1)
	handle.SetConnMaxLifetime(0)

	db := &DB{sql: handle, path: path}
	// modernc.org/sqlite opens lazily; ping to surface an unusable file early.
	if err := handle.Ping(); err != nil {
		handle.Close()
		return nil, fmt.Errorf("open projection: %w", err)
	}
	upgrade, err := db.migrate()
	if err != nil {
		handle.Close()
		return nil, err
	}
	db.upgrade = upgrade
	return db, nil
}

// Upgrade reports the schema reconciliation performed when this projection was
// opened.
func (db *DB) Upgrade() Upgrade { return db.upgrade }

// Path returns the database file path.
func (db *DB) Path() string { return db.path }

// Close closes the projection.
func (db *DB) Close() error { return db.sql.Close() }

// SQL exposes the handle for read-only diagnostics.
func (db *DB) SQL() *sql.DB { return db.sql }

func (db *DB) migrate() (Upgrade, error) {
	upgrade := Upgrade{ToVersion: fmt.Sprint(SchemaVersion)}

	from, present, err := db.storedSchemaVersion()
	if err != nil {
		return upgrade, err
	}
	upgrade.FromVersion = from
	if present && from != upgrade.ToVersion {
		// The projection is derived. Dropping it loses nothing, and the ledger
		// is never touched here (section 48, 74 of the L2.5 mission).
		if err := db.Reset(); err != nil {
			return upgrade, fmt.Errorf("rebuild projection for schema %s -> %s: %w", from, upgrade.ToVersion, err)
		}
		upgrade.Rebuilt = true
	}

	if _, err := db.sql.Exec(schema); err != nil {
		return upgrade, fmt.Errorf("create projection schema: %w", err)
	}
	// Columns added within a schema version are patched in rather than forcing
	// a rebuild of an already-current projection.
	for _, column := range []struct{ table, name, ddl string }{
		{"events", "installation_id", "TEXT NOT NULL DEFAULT ''"},
		{"candidates", "issue_ids", "TEXT NOT NULL DEFAULT '[]'"},
	} {
		if err := db.ensureColumn(column.table, column.name, column.ddl); err != nil {
			return upgrade, err
		}
	}
	return upgrade, db.SetMeta(MetaSchemaVersion, fmt.Sprint(SchemaVersion))
}

// storedSchemaVersion reads the projection's own schema version without
// assuming the meta table exists yet.
func (db *DB) storedSchemaVersion() (string, bool, error) {
	var name string
	err := db.sql.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'meta'`).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	value, ok, err := db.Meta(MetaSchemaVersion)
	if err != nil {
		return "", false, err
	}
	return value, ok, nil
}

// ensureColumn adds a column when it is missing.
func (db *DB) ensureColumn(table, column, ddl string) error {
	rows, err := db.sql.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return fmt.Errorf("inspect %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid       int
			name      string
			ctype     string
			notNull   int
			dfltValue any
			pk        int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dfltValue, &pk); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if _, err := db.sql.Exec("ALTER TABLE " + table + " ADD COLUMN " + column + " " + ddl); err != nil {
		return fmt.Errorf("add %s.%s: %w", table, column, err)
	}
	return nil
}

// Reset drops every projection table and recreates them empty. Canonical truth
// (ledger plus objects) is untouched; the caller replays it afterwards
// (section 88).
func (db *DB) Reset() error {
	tables := []string{
		"meta", "applications", "app_registrations", "junctions", "config_families",
		"installations", "sessions",
		"managed_targets", "path_ownership", "events", "issues", "issue_occurrences",
		"config_revisions", "config_revision_blobs", "config_refs", "config_reflog",
		"candidates", "work_items", "improvement_results", "improvement_artifacts",
		"validations", "approvals", "deployments",
		"update_dispositions", "application_results",
		"audit_events", "ledger_index",
	}
	tx, err := db.sql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, t := range tables {
		if _, err := tx.Exec("DROP TABLE IF EXISTS " + t); err != nil {
			return fmt.Errorf("drop %s: %w", t, err)
		}
	}
	if _, err := tx.Exec(schema); err != nil {
		return err
	}
	return tx.Commit()
}

// Tx runs fn inside a write transaction.
func (db *DB) Tx(fn func(*sql.Tx) error) error {
	tx, err := db.sql.Begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// InTx runs fn inside a transaction when the caller already holds one, and
// otherwise opens a transaction. Engine code uses this so a single logical
// mutation stays atomic whether it runs alone or inside a rebuild.
func (db *DB) InTx(tx *sql.Tx, fn func(*sql.Tx) error) error {
	if tx != nil {
		return fn(tx)
	}
	return db.Tx(fn)
}

// SetMeta writes a meta key.
func (db *DB) SetMeta(key, value string) error {
	_, err := db.sql.Exec(
		`INSERT INTO meta(key, value) VALUES(?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// SetMetaTx writes a meta key inside a transaction.
func SetMetaTx(tx *sql.Tx, key, value string) error {
	_, err := tx.Exec(
		`INSERT INTO meta(key, value) VALUES(?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// Meta reads a meta key, returning ("", false, nil) when absent.
func (db *DB) Meta(key string) (string, bool, error) {
	var value string
	err := db.sql.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

// MetaTx reads a meta key inside a transaction.
func MetaTx(tx *sql.Tx, key string) (string, bool, error) {
	var value string
	err := tx.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

// LedgerCheckpoint reports how far the projection has consumed the ledger.
func (db *DB) LedgerCheckpoint() (sequence uint64, hash string, builtAt time.Time, err error) {
	seqStr, ok, err := db.Meta(MetaLedgerSeq)
	if err != nil || !ok {
		return 0, "", time.Time{}, err
	}
	if _, err := fmt.Sscanf(seqStr, "%d", &sequence); err != nil {
		return 0, "", time.Time{}, fmt.Errorf("meta %s: %w", MetaLedgerSeq, err)
	}
	hash, _, err = db.Meta(MetaLedgerHash)
	if err != nil {
		return 0, "", time.Time{}, err
	}
	built, ok, err := db.Meta(MetaBuiltAt)
	if err == nil && ok {
		builtAt = parseTS(built)
	}
	return sequence, hash, builtAt, err
}

const schema = `
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS applications (
    application_id        TEXT PRIMARY KEY,
    name                  TEXT NOT NULL,
    app_type              TEXT NOT NULL DEFAULT '',
    owner                 TEXT NOT NULL DEFAULT '',
    repository            TEXT NOT NULL DEFAULT '',
    installation_id       TEXT NOT NULL DEFAULT '',
    default_installation_id TEXT NOT NULL DEFAULT '',
    registration_revision INTEGER NOT NULL DEFAULT 0,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS applications_name_idx ON applications(name);

-- Installations are deployments of one application (mission section 38). The
-- identity is a UUID; a hostname is metadata at best.
CREATE TABLE IF NOT EXISTS installations (
    application_id  TEXT NOT NULL,
    installation_id TEXT NOT NULL,
    name            TEXT NOT NULL DEFAULT '',
    environment     TEXT NOT NULL DEFAULT '',
    session_policy  TEXT NOT NULL DEFAULT 'MULTI_PROCESS_ALLOWED',
    active          INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (application_id, installation_id)
);
CREATE INDEX IF NOT EXISTS installations_id_idx ON installations(installation_id);

-- Sessions are runtime state, not history (mission sections 8, 43). On a
-- projection rebuild this table starts empty, which is the correct answer:
-- applications reconnect.
CREATE TABLE IF NOT EXISTS sessions (
    session_id       TEXT PRIMARY KEY,
    application_id   TEXT NOT NULL,
    installation_id  TEXT NOT NULL,
    process_id       TEXT NOT NULL,
    protocol_version TEXT NOT NULL,
    client_name      TEXT NOT NULL DEFAULT '',
    client_version   TEXT NOT NULL DEFAULT '',
    manifest_hash    TEXT NOT NULL DEFAULT '',
    pid              INTEGER NOT NULL DEFAULT 0,
    hostname         TEXT NOT NULL DEFAULT '',
    peer_uid         INTEGER NOT NULL DEFAULT -1,
    peer_gid         INTEGER NOT NULL DEFAULT -1,
    state            TEXT NOT NULL,
    connected_at     TEXT NOT NULL,
    last_seen_at     TEXT NOT NULL,
    closed_at        TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS sessions_application_idx ON sessions(application_id);
CREATE INDEX IF NOT EXISTS sessions_installation_idx ON sessions(installation_id);
CREATE INDEX IF NOT EXISTS sessions_process_idx ON sessions(process_id);
CREATE INDEX IF NOT EXISTS sessions_state_idx ON sessions(state);
CREATE INDEX IF NOT EXISTS sessions_last_seen_idx ON sessions(last_seen_at);

CREATE TABLE IF NOT EXISTS app_registrations (
    registration_id TEXT PRIMARY KEY,
    application_id  TEXT NOT NULL,
    revision        INTEGER NOT NULL,
    manifest        TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    ledger_sequence INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS app_registrations_app_idx ON app_registrations(application_id, revision);

CREATE TABLE IF NOT EXISTS junctions (
    junction_id      TEXT PRIMARY KEY,
    application_id   TEXT NOT NULL,
    name             TEXT NOT NULL,
    workflow_type    TEXT NOT NULL DEFAULT '',
    config_family_id TEXT NOT NULL DEFAULT '',
    feedback_types   TEXT NOT NULL DEFAULT '[]',
    contract         TEXT NOT NULL DEFAULT '{}',
    replay_adapter   TEXT NOT NULL DEFAULT '',
    active           INTEGER NOT NULL DEFAULT 1,
    created_at       TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS junctions_app_name_idx ON junctions(application_id, name);

CREATE TABLE IF NOT EXISTS config_families (
    config_family_id TEXT PRIMARY KEY,
    application_id   TEXT NOT NULL,
    name             TEXT NOT NULL,
    schema_revision  TEXT NOT NULL DEFAULT '',
    workflow_type    TEXT NOT NULL DEFAULT '',
    requires_holdout INTEGER NOT NULL DEFAULT 0,
    management_mode  TEXT NOT NULL DEFAULT 'EXTERNAL',
    created_at       TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS config_families_app_name_idx ON config_families(application_id, name);

CREATE TABLE IF NOT EXISTS managed_targets (
    target_id        TEXT PRIMARY KEY,
    application_id   TEXT NOT NULL,
    config_family_id TEXT NOT NULL,
    installation_id  TEXT NOT NULL DEFAULT '',
    target_type      TEXT NOT NULL,
    path             TEXT NOT NULL,
    reload_policy    TEXT NOT NULL DEFAULT '',
    atomicity        TEXT NOT NULL DEFAULT '',
    health           TEXT NOT NULL DEFAULT '{}',
    created_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS managed_targets_family_idx ON managed_targets(config_family_id);

CREATE TABLE IF NOT EXISTS path_ownership (
    path             TEXT PRIMARY KEY,
    application_id   TEXT NOT NULL,
    config_family_id TEXT NOT NULL,
    installation_id  TEXT NOT NULL DEFAULT '',
    target_id        TEXT NOT NULL,
    created_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id          TEXT PRIMARY KEY,
    application_id    TEXT NOT NULL,
    junction_id       TEXT NOT NULL,
    installation_id   TEXT NOT NULL DEFAULT '',
    type              TEXT NOT NULL,
    source            TEXT NOT NULL,
    subject           TEXT NOT NULL DEFAULT '',
    feedback_type     TEXT NOT NULL,
    reason_code       TEXT NOT NULL DEFAULT '',
    fingerprint       TEXT NOT NULL,
    producer_instance TEXT NOT NULL DEFAULT '',
    contract_revision TEXT NOT NULL DEFAULT '',
    config_revision   TEXT NOT NULL DEFAULT '',
    config_hash       TEXT NOT NULL DEFAULT '',
    input_ref         TEXT NOT NULL DEFAULT '',
    replay_ref        TEXT NOT NULL DEFAULT '',
    session_id        TEXT NOT NULL DEFAULT '',
    replayed_by_session_id TEXT NOT NULL DEFAULT '',
    durability        TEXT NOT NULL,
    data              TEXT NOT NULL,
    occurred_at       TEXT NOT NULL,
    received_at       TEXT NOT NULL,
    ledger_sequence   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS events_app_idx ON events(application_id, received_at);
CREATE INDEX IF NOT EXISTS events_junction_idx ON events(junction_id, received_at);
CREATE INDEX IF NOT EXISTS events_fingerprint_idx ON events(fingerprint);
CREATE INDEX IF NOT EXISTS events_seq_idx ON events(ledger_sequence);

CREATE TABLE IF NOT EXISTS issues (
    issue_id             TEXT PRIMARY KEY,
    application_id       TEXT NOT NULL,
    junction_id          TEXT NOT NULL,
    feedback_type        TEXT NOT NULL,
    reason_code          TEXT NOT NULL DEFAULT '',
    fingerprint          TEXT NOT NULL,
    pattern              TEXT NOT NULL DEFAULT '',
    status               TEXT NOT NULL,
    severity             TEXT NOT NULL DEFAULT '',
    priority             INTEGER NOT NULL DEFAULT 0,
    occurrence_count     INTEGER NOT NULL DEFAULT 0,
    unique_sources       INTEGER NOT NULL DEFAULT 0,
    controlling_revision TEXT NOT NULL DEFAULT '',
    first_seen           TEXT NOT NULL,
    last_seen            TEXT NOT NULL,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS issues_fingerprint_idx ON issues(fingerprint);
CREATE INDEX IF NOT EXISTS issues_status_idx ON issues(status, occurrence_count DESC);

CREATE TABLE IF NOT EXISTS issue_occurrences (
    issue_id   TEXT NOT NULL,
    event_id   TEXT NOT NULL,
    seen_at    TEXT NOT NULL,
    producer   TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (issue_id, event_id)
);
CREATE INDEX IF NOT EXISTS issue_occurrences_event_idx ON issue_occurrences(event_id);

CREATE TABLE IF NOT EXISTS config_revisions (
    revision_id        TEXT PRIMARY KEY,
    application_id     TEXT NOT NULL,
    config_family_id   TEXT NOT NULL,
    sequence           INTEGER NOT NULL,
    content_hash       TEXT NOT NULL,
    root_tree_hash     TEXT NOT NULL,
    parent_revision_id TEXT NOT NULL DEFAULT '',
    schema_revision    TEXT NOT NULL DEFAULT '',
    label              TEXT NOT NULL DEFAULT '',
    message            TEXT NOT NULL DEFAULT '',
    issue_ids          TEXT NOT NULL DEFAULT '[]',
    work_item_id       TEXT NOT NULL DEFAULT '',
    candidate_id       TEXT NOT NULL DEFAULT '',
    author             TEXT NOT NULL DEFAULT '',
    created_at         TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS config_revisions_family_seq_idx ON config_revisions(config_family_id, sequence);
CREATE INDEX IF NOT EXISTS config_revisions_hash_idx ON config_revisions(content_hash);

CREATE TABLE IF NOT EXISTS config_revision_blobs (
    revision_id TEXT NOT NULL,
    path        TEXT NOT NULL,
    blob_hash   TEXT NOT NULL,
    mode        TEXT NOT NULL DEFAULT '',
    size        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (revision_id, path)
);

CREATE TABLE IF NOT EXISTS config_refs (
    application_id   TEXT NOT NULL,
    config_family_id TEXT NOT NULL,
    installation_id  TEXT NOT NULL DEFAULT '',
    name             TEXT NOT NULL,
    revision_id      TEXT NOT NULL,
    content_hash     TEXT NOT NULL DEFAULT '',
    updated_at       TEXT NOT NULL,
    PRIMARY KEY (application_id, config_family_id, installation_id, name)
);

CREATE TABLE IF NOT EXISTS config_reflog (
    log_id           TEXT PRIMARY KEY,
    application_id   TEXT NOT NULL,
    config_family_id TEXT NOT NULL,
    installation_id  TEXT NOT NULL DEFAULT '',
    ref_name         TEXT NOT NULL,
    old_revision_id  TEXT NOT NULL DEFAULT '',
    new_revision_id  TEXT NOT NULL,
    reason           TEXT NOT NULL DEFAULT '',
    issue_ids        TEXT NOT NULL DEFAULT '[]',
    candidate_id     TEXT NOT NULL DEFAULT '',
    validation_id    TEXT NOT NULL DEFAULT '',
    approval_id      TEXT NOT NULL DEFAULT '',
    deployment_id    TEXT NOT NULL DEFAULT '',
    ledger_sequence  INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS config_reflog_ref_idx ON config_reflog(application_id, config_family_id, installation_id, ref_name, created_at);

CREATE TABLE IF NOT EXISTS candidates (
    candidate_id     TEXT PRIMARY KEY,
    application_id   TEXT NOT NULL,
    config_family_id TEXT NOT NULL,
    installation_id  TEXT NOT NULL DEFAULT '',
    base_revision_id TEXT NOT NULL,
    root_tree_hash   TEXT NOT NULL,
    state            TEXT NOT NULL,
    issue_ids        TEXT NOT NULL DEFAULT '[]',
    work_item_id     TEXT NOT NULL DEFAULT '',
    workflow_run_id  TEXT NOT NULL DEFAULT '',
    explanation      TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS candidates_family_idx ON candidates(config_family_id, state);

CREATE TABLE IF NOT EXISTS work_items (
    work_id          TEXT PRIMARY KEY,
    workflow_type    TEXT NOT NULL,
    application_id   TEXT NOT NULL,
    junction_id      TEXT NOT NULL DEFAULT '',
    config_family_id TEXT NOT NULL DEFAULT '',
    installation_id  TEXT NOT NULL DEFAULT '',
    base_revision_id TEXT NOT NULL DEFAULT '',
    issue_ids        TEXT NOT NULL DEFAULT '[]',
    package          TEXT NOT NULL DEFAULT '{}',
    state            TEXT NOT NULL,
    lease_owner      TEXT NOT NULL DEFAULT '',
    lease_expires_at TEXT NOT NULL DEFAULT '',
    attempts         INTEGER NOT NULL DEFAULT 0,
    result           TEXT NOT NULL DEFAULT '{}',
    improvement_result_id TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS work_items_state_idx ON work_items(state, workflow_type, created_at);

-- A worker's return is an immutable record (mission section 6). One successful
-- RETURNED result per work item; retries produce a new attempt, not a second
-- result.
CREATE TABLE IF NOT EXISTS improvement_results (
    result_id       TEXT PRIMARY KEY,
    work_item_id    TEXT NOT NULL,
    workflow_run_id TEXT NOT NULL DEFAULT '',
    application_id  TEXT NOT NULL,
    issue_ids       TEXT NOT NULL DEFAULT '[]',
    worker          TEXT NOT NULL DEFAULT '',
    attempt         INTEGER NOT NULL DEFAULT 0,
    summary         TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS improvement_results_work_idx ON improvement_results(work_item_id);

-- What the worker actually produced. A configuration candidate is one possible
-- artifact among five (mission section 5); the artifact payload holds small
-- immutable metadata only, never large binaries.
CREATE TABLE IF NOT EXISTS improvement_artifacts (
    artifact_id      TEXT PRIMARY KEY,
    result_id        TEXT NOT NULL,
    application_id   TEXT NOT NULL,
    config_family_id TEXT NOT NULL DEFAULT '',
    installation_id  TEXT NOT NULL DEFAULT '',
    kind             TEXT NOT NULL,
    candidate_id     TEXT NOT NULL DEFAULT '',
    content_hash     TEXT NOT NULL DEFAULT '',
    payload          TEXT NOT NULL DEFAULT '{}',
    created_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS improvement_artifacts_result_idx ON improvement_artifacts(result_id);
CREATE INDEX IF NOT EXISTS improvement_artifacts_kind_idx ON improvement_artifacts(kind);
CREATE INDEX IF NOT EXISTS improvement_artifacts_candidate_idx ON improvement_artifacts(candidate_id);
CREATE INDEX IF NOT EXISTS improvement_artifacts_app_idx ON improvement_artifacts(application_id);

CREATE TABLE IF NOT EXISTS validations (
    validation_id    TEXT PRIMARY KEY,
    candidate_id     TEXT NOT NULL,
    application_id   TEXT NOT NULL,
    config_family_id TEXT NOT NULL,
    suite_id         TEXT NOT NULL DEFAULT '',
    suite_version    TEXT NOT NULL DEFAULT '',
    kind             TEXT NOT NULL DEFAULT '',
    result           TEXT NOT NULL,
    metrics          TEXT NOT NULL DEFAULT '{}',
    artifacts        TEXT NOT NULL DEFAULT '[]',
    inputs_hash      TEXT NOT NULL DEFAULT '',
    environment      TEXT NOT NULL DEFAULT '',
    worker           TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS validations_candidate_idx ON validations(candidate_id, kind);

CREATE TABLE IF NOT EXISTS approvals (
    approval_id   TEXT PRIMARY KEY,
    candidate_id  TEXT NOT NULL,
    approver      TEXT NOT NULL,
    actor_type    TEXT NOT NULL DEFAULT 'SYSTEM',
    decision      TEXT NOT NULL,
    comment       TEXT NOT NULL DEFAULT '',
    application_id   TEXT NOT NULL DEFAULT '',
    installation_id  TEXT NOT NULL DEFAULT '',
    config_family_id TEXT NOT NULL DEFAULT '',
    content_hash     TEXT NOT NULL DEFAULT '',
    base_revision_id TEXT NOT NULL DEFAULT '',
    validation_ids   TEXT NOT NULL DEFAULT '[]',
    peer_uid         INTEGER NOT NULL DEFAULT -1,
    created_at    TEXT NOT NULL
);

-- Delivery is per installation and per revision: one row says how one
-- installation stands in relation to one approved revision (mission section 39).
CREATE TABLE IF NOT EXISTS update_dispositions (
    application_id   TEXT NOT NULL,
    installation_id  TEXT NOT NULL,
    config_family_id TEXT NOT NULL,
    revision_id      TEXT NOT NULL,
    content_hash     TEXT NOT NULL DEFAULT '',
    approval_id      TEXT NOT NULL DEFAULT '',
    state            TEXT NOT NULL,
    reason_code      TEXT NOT NULL DEFAULT '',
    fetched_at       TEXT NOT NULL DEFAULT '',
    observed_at      TEXT NOT NULL DEFAULT '',
    updated_at       TEXT NOT NULL,
    PRIMARY KEY (application_id, installation_id, revision_id)
);
CREATE INDEX IF NOT EXISTS update_dispositions_family_idx
    ON update_dispositions(config_family_id, installation_id, state);

-- A node's application result is history, not runtime state (mission section 33).
CREATE TABLE IF NOT EXISTS application_results (
    result_id        TEXT PRIMARY KEY,
    application_id   TEXT NOT NULL,
    installation_id  TEXT NOT NULL,
    config_family_id TEXT NOT NULL,
    revision_id      TEXT NOT NULL,
    content_hash     TEXT NOT NULL DEFAULT '',
    approval_id      TEXT NOT NULL DEFAULT '',
    previous_revision_id TEXT NOT NULL DEFAULT '',
    observed_hash    TEXT NOT NULL DEFAULT '',
    result           TEXT NOT NULL,
    reason_code      TEXT NOT NULL DEFAULT '',
    message          TEXT NOT NULL DEFAULT '',
    details          TEXT NOT NULL DEFAULT '{}',
    session_id       TEXT NOT NULL DEFAULT '',
    process_id       TEXT NOT NULL DEFAULT '',
    peer_uid         INTEGER NOT NULL DEFAULT -1,
    created_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS application_results_install_idx
    ON application_results(application_id, installation_id, created_at);
CREATE INDEX IF NOT EXISTS application_results_revision_idx
    ON application_results(revision_id);

CREATE TABLE IF NOT EXISTS deployments (
    deployment_id     TEXT PRIMARY KEY,
    application_id    TEXT NOT NULL,
    config_family_id  TEXT NOT NULL,
    revision_id       TEXT NOT NULL,
    target_id         TEXT NOT NULL DEFAULT '',
    state             TEXT NOT NULL,
    atomicity         TEXT NOT NULL DEFAULT '',
    health_result     TEXT NOT NULL DEFAULT '{}',
    previous_revision TEXT NOT NULL DEFAULT '',
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_events (
    audit_id        TEXT PRIMARY KEY,
    action          TEXT NOT NULL,
    actor           TEXT NOT NULL DEFAULT '',
    application_id  TEXT NOT NULL DEFAULT '',
    subject_kind    TEXT NOT NULL DEFAULT '',
    subject_id      TEXT NOT NULL DEFAULT '',
    detail          TEXT NOT NULL DEFAULT '{}',
    ledger_sequence INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS audit_events_time_idx ON audit_events(created_at DESC);

CREATE TABLE IF NOT EXISTS ledger_index (
    sequence   INTEGER PRIMARY KEY,
    kind       TEXT NOT NULL,
    object_id  TEXT NOT NULL,
    record_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);
`
