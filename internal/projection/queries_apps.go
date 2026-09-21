package projection

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func encodeStrings(in []string) string {
	if len(in) == 0 {
		return "[]"
	}
	raw, err := json.Marshal(in)
	if err != nil {
		return "[]"
	}
	return string(raw)
}

func decodeStrings(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil
	}
	return out
}

func orEmptyJSON(s string) string {
	if strings.TrimSpace(s) == "" {
		return "{}"
	}
	return s
}

// ---------- applications ----------

// UpsertApplication indexes an application registration.
func UpsertApplication(tx *sql.Tx, app Application) error {
	_, err := tx.Exec(`
		INSERT INTO applications
			(application_id, name, app_type, owner, repository, installation_id,
			 default_installation_id, registration_revision, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(application_id) DO UPDATE SET
			name = excluded.name,
			app_type = excluded.app_type,
			owner = excluded.owner,
			repository = excluded.repository,
			installation_id = excluded.installation_id,
			default_installation_id = excluded.default_installation_id,
			registration_revision = MAX(applications.registration_revision, excluded.registration_revision),
			updated_at = excluded.updated_at`,
		app.ApplicationID, app.Name, app.AppType, app.Owner, app.Repository,
		app.InstallationID, app.DefaultInstallationID, app.RegistrationRevision,
		ts(app.CreatedAt), ts(app.UpdatedAt))
	return err
}

const applicationCols = `application_id, name, app_type, owner, repository, installation_id,
	default_installation_id, registration_revision, created_at, updated_at`

// GetApplication loads one application.
func (db *DB) GetApplication(id string) (Application, error) {
	return db.scanApplication(db.sql.QueryRow(`
		SELECT `+applicationCols+` FROM applications WHERE application_id = ?`, id), id, "application")
}

// GetApplicationByName loads one application by its human-readable name.
func (db *DB) GetApplicationByName(name string) (Application, error) {
	return db.scanApplication(db.sql.QueryRow(`
		SELECT `+applicationCols+` FROM applications WHERE name = ?`, name), name, "application named")
}

func (db *DB) scanApplication(row interface{ Scan(...any) error }, key, what string) (Application, error) {
	var a Application
	var created, updated string
	err := row.Scan(&a.ApplicationID, &a.Name, &a.AppType, &a.Owner, &a.Repository,
		&a.InstallationID, &a.DefaultInstallationID, &a.RegistrationRevision, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return a, NotFoundf("%s %s", what, key)
	}
	if err != nil {
		return a, err
	}
	a.CreatedAt, a.UpdatedAt = parseTS(created), parseTS(updated)
	return a, nil
}

// ListApplications returns every registered application.
func (db *DB) ListApplications() ([]Application, error) {
	rows, err := db.sql.Query(`
		SELECT ` + applicationCols + ` FROM applications ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Application
	for rows.Next() {
		a, err := db.scanApplication(rows, "", "")
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// CountApplications returns how many applications are registered.
func (db *DB) CountApplications() (int, error) {
	var n int
	err := db.sql.QueryRow(`SELECT COUNT(*) FROM applications`).Scan(&n)
	return n, err
}

// InsertRegistration appends an immutable registration revision (section 7).
func InsertRegistration(tx *sql.Tx, reg Registration) error {
	_, err := tx.Exec(`
		INSERT INTO app_registrations
			(registration_id, application_id, revision, manifest, content_hash, ledger_sequence, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(registration_id) DO NOTHING`,
		reg.RegistrationID, reg.ApplicationID, reg.Revision, reg.Manifest,
		reg.ContentHash, reg.LedgerSequence, ts(reg.CreatedAt))
	return err
}

// ListRegistrations returns the registration history of an application.
func (db *DB) ListRegistrations(applicationID string) ([]Registration, error) {
	rows, err := db.sql.Query(`
		SELECT registration_id, application_id, revision, manifest, content_hash,
		       ledger_sequence, created_at
		FROM app_registrations WHERE application_id = ? ORDER BY revision`, applicationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Registration
	for rows.Next() {
		var r Registration
		var created string
		if err := rows.Scan(&r.RegistrationID, &r.ApplicationID, &r.Revision,
			&r.Manifest, &r.ContentHash, &r.LedgerSequence, &created); err != nil {
			return nil, err
		}
		r.CreatedAt = parseTS(created)
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---------- junctions ----------

// ---------- installations ----------

// UpsertInstallation indexes one deployment of one application.
func UpsertInstallation(tx *sql.Tx, in Installation) error {
	active := 0
	if in.Active {
		active = 1
	}
	_, err := tx.Exec(`
		INSERT INTO installations
			(application_id, installation_id, name, environment, session_policy, active, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(application_id, installation_id) DO UPDATE SET
			name = excluded.name,
			environment = excluded.environment,
			session_policy = excluded.session_policy,
			active = excluded.active,
			updated_at = excluded.updated_at`,
		in.ApplicationID, in.InstallationID, in.Name, in.Environment, sessionPolicyOf(in), active,
		ts(in.CreatedAt), ts(in.UpdatedAt))
	return err
}

// sessionPolicyOf defaults the policy so an installation recorded without one
// is explicitly multi-process, not accidentally undefined.
func sessionPolicyOf(in Installation) string {
	if in.SessionPolicy == "" {
		return SessionPolicyMulti
	}
	return in.SessionPolicy
}

const installationCols = `application_id, installation_id, name, environment, session_policy,
	active, created_at, updated_at`

func scanInstallation(row interface{ Scan(...any) error }) (Installation, error) {
	var in Installation
	var active int
	var created, updated string
	if err := row.Scan(&in.ApplicationID, &in.InstallationID, &in.Name, &in.Environment,
		&in.SessionPolicy, &active, &created, &updated); err != nil {
		return in, err
	}
	in.Active = active == 1
	in.CreatedAt, in.UpdatedAt = parseTS(created), parseTS(updated)
	return in, nil
}

// GetInstallation loads one installation.
func (db *DB) GetInstallation(applicationID, installationID string) (Installation, error) {
	in, err := scanInstallation(db.sql.QueryRow(
		`SELECT `+installationCols+` FROM installations WHERE application_id = ? AND installation_id = ?`,
		applicationID, installationID))
	if errors.Is(err, sql.ErrNoRows) {
		return in, NotFoundf("installation %s of application %s", installationID, applicationID)
	}
	return in, err
}

// GetInstallationTx loads one installation inside a transaction.
func GetInstallationTx(tx *sql.Tx, applicationID, installationID string) (Installation, error) {
	in, err := scanInstallation(tx.QueryRow(
		`SELECT `+installationCols+` FROM installations WHERE application_id = ? AND installation_id = ?`,
		applicationID, installationID))
	if errors.Is(err, sql.ErrNoRows) {
		return in, NotFoundf("installation %s of application %s", installationID, applicationID)
	}
	return in, err
}

// ListInstallations lists installations, optionally filtered by application.
func (db *DB) ListInstallations(applicationID string) ([]Installation, error) {
	q := `SELECT ` + installationCols + ` FROM installations`
	args := []any{}
	if applicationID != "" {
		q += ` WHERE application_id = ?`
		args = append(args, applicationID)
	}
	q += ` ORDER BY application_id, name, installation_id`

	rows, err := db.sql.Query(q, args...)
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

// DefaultInstallationID reports the installation a request without an explicit
// installation belongs to: the application's declared default, or the
// application identity itself for applications registered before installations
// existed (mission section 40).
func DefaultInstallationID(app Application) string {
	if app.DefaultInstallationID != "" {
		return app.DefaultInstallationID
	}
	if app.InstallationID != "" {
		return app.InstallationID
	}
	return app.ApplicationID
}

// UpsertJunction indexes a junction definition.
func UpsertJunction(tx *sql.Tx, j Junction) error {
	active := 0
	if j.Active {
		active = 1
	}
	_, err := tx.Exec(`
		INSERT INTO junctions
			(junction_id, application_id, name, workflow_type, config_family_id,
			 feedback_types, contract, replay_adapter, active, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(junction_id) DO UPDATE SET
			name = excluded.name,
			workflow_type = excluded.workflow_type,
			config_family_id = excluded.config_family_id,
			feedback_types = excluded.feedback_types,
			contract = excluded.contract,
			replay_adapter = excluded.replay_adapter,
			active = excluded.active`,
		j.JunctionID, j.ApplicationID, j.Name, j.WorkflowType, j.ConfigFamilyID,
		encodeStrings(j.FeedbackTypes), orEmptyJSON(j.Contract), j.ReplayAdapter, active, ts(j.CreatedAt))
	return err
}

const junctionCols = `junction_id, application_id, name, workflow_type, config_family_id,
	feedback_types, contract, replay_adapter, active, created_at`

func scanJunction(row interface{ Scan(...any) error }) (Junction, error) {
	var j Junction
	var types, contract, created string
	var active int
	if err := row.Scan(&j.JunctionID, &j.ApplicationID, &j.Name, &j.WorkflowType,
		&j.ConfigFamilyID, &types, &contract, &j.ReplayAdapter, &active, &created); err != nil {
		return j, err
	}
	j.FeedbackTypes = decodeStrings(types)
	j.Contract = contract
	j.Active = active == 1
	j.CreatedAt = parseTS(created)
	return j, nil
}

// GetJunction loads one junction by identity.
func (db *DB) GetJunction(id string) (Junction, error) {
	j, err := scanJunction(db.sql.QueryRow(`SELECT `+junctionCols+` FROM junctions WHERE junction_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return j, NotFoundf("junction %s", id)
	}
	return j, err
}

// GetJunctionByName loads a junction by application and name: the pair an
// application uses when it emits feedback.
func (db *DB) GetJunctionByName(applicationID, name string) (Junction, error) {
	j, err := scanJunction(db.sql.QueryRow(
		`SELECT `+junctionCols+` FROM junctions WHERE application_id = ? AND name = ?`, applicationID, name))
	if errors.Is(err, sql.ErrNoRows) {
		return j, NotFoundf("junction %s/%s", applicationID, name)
	}
	return j, err
}

// ListJunctions lists junctions, optionally filtered by application.
func (db *DB) ListJunctions(applicationID string) ([]Junction, error) {
	q := `SELECT ` + junctionCols + ` FROM junctions`
	args := []any{}
	if applicationID != "" {
		q += ` WHERE application_id = ?`
		args = append(args, applicationID)
	}
	q += ` ORDER BY application_id, name`
	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Junction
	for rows.Next() {
		j, err := scanJunction(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// ---------- config families ----------

// UpsertConfigFamily indexes a config family.
func UpsertConfigFamily(tx *sql.Tx, f ConfigFamily) error {
	holdout := 0
	if f.RequiresHoldout {
		holdout = 1
	}
	_, err := tx.Exec(`
		INSERT INTO config_families
			(config_family_id, application_id, name, schema_revision, workflow_type,
			 requires_holdout, management_mode, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(config_family_id) DO UPDATE SET
			name = excluded.name,
			schema_revision = excluded.schema_revision,
			workflow_type = excluded.workflow_type,
			requires_holdout = excluded.requires_holdout,
			management_mode = excluded.management_mode`,
		f.ConfigFamilyID, f.ApplicationID, f.Name, f.SchemaRevision, f.WorkflowType, holdout,
		NormalizeManagementMode(f.ManagementMode), ts(f.CreatedAt))
	return err
}

const familyCols = `config_family_id, application_id, name, schema_revision, workflow_type,
	requires_holdout, management_mode, created_at`

func scanFamily(row interface{ Scan(...any) error }) (ConfigFamily, error) {
	var f ConfigFamily
	var created string
	var holdout int
	if err := row.Scan(&f.ConfigFamilyID, &f.ApplicationID, &f.Name, &f.SchemaRevision,
		&f.WorkflowType, &holdout, &f.ManagementMode, &created); err != nil {
		return f, err
	}
	f.RequiresHoldout = holdout == 1
	f.ManagementMode = NormalizeManagementMode(f.ManagementMode)
	f.CreatedAt = parseTS(created)
	return f, nil
}

// ListConfigFamiliesTx lists an application's families inside a transaction.
func ListConfigFamiliesTx(tx *sql.Tx, applicationID string) ([]ConfigFamily, error) {
	q := `SELECT ` + familyCols + ` FROM config_families`
	args := []any{}
	if applicationID != "" {
		q += ` WHERE application_id = ?`
		args = append(args, applicationID)
	}
	q += ` ORDER BY name`
	rows, err := tx.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConfigFamily
	for rows.Next() {
		f, err := scanFamily(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// GetConfigFamily loads one config family.
func (db *DB) GetConfigFamily(id string) (ConfigFamily, error) {
	f, err := scanFamily(db.sql.QueryRow(`SELECT `+familyCols+` FROM config_families WHERE config_family_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return f, NotFoundf("config family %s", id)
	}
	return f, err
}

// GetConfigFamilyByName loads a family by application and name.
func (db *DB) GetConfigFamilyByName(applicationID, name string) (ConfigFamily, error) {
	f, err := scanFamily(db.sql.QueryRow(
		`SELECT `+familyCols+` FROM config_families WHERE application_id = ? AND name = ?`, applicationID, name))
	if errors.Is(err, sql.ErrNoRows) {
		return f, NotFoundf("config family %s/%s", applicationID, name)
	}
	return f, err
}

// ListConfigFamilies lists families, optionally filtered by application.
func (db *DB) ListConfigFamilies(applicationID string) ([]ConfigFamily, error) {
	q := `SELECT ` + familyCols + ` FROM config_families`
	args := []any{}
	if applicationID != "" {
		q += ` WHERE application_id = ?`
		args = append(args, applicationID)
	}
	q += ` ORDER BY application_id, name`
	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConfigFamily
	for rows.Next() {
		f, err := scanFamily(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// ---------- managed targets and path ownership ----------

// RegisterTarget records a pre-registered deployment target and claims its
// path (section 25, section 26).
func RegisterTarget(tx *sql.Tx, t ManagedTarget) error {
	var existingApp, existingTarget string
	err := tx.QueryRow(`SELECT application_id, target_id FROM path_ownership WHERE path = ?`, t.Path).
		Scan(&existingApp, &existingTarget)
	switch {
	case err == nil:
		if existingApp != t.ApplicationID {
			return fmt.Errorf("%w: %s belongs to application %s", ErrPathConflict, t.Path, existingApp)
		}
		if existingTarget != t.TargetID {
			return fmt.Errorf("%w: %s is already claimed by target %s", ErrPathConflict, t.Path, existingTarget)
		}
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}

	if _, err := tx.Exec(`
		INSERT INTO managed_targets
			(target_id, application_id, config_family_id, installation_id, target_type, path,
			 reload_policy, atomicity, health, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(target_id) DO UPDATE SET
			installation_id = excluded.installation_id,
			target_type = excluded.target_type,
			path = excluded.path,
			reload_policy = excluded.reload_policy,
			atomicity = excluded.atomicity,
			health = excluded.health`,
		t.TargetID, t.ApplicationID, t.ConfigFamilyID, t.InstallationID, t.TargetType, t.Path,
		t.ReloadPolicy, t.Atomicity, orEmptyJSON(t.Health), ts(t.CreatedAt)); err != nil {
		return err
	}
	_, err = tx.Exec(`
		INSERT INTO path_ownership (path, application_id, config_family_id, installation_id, target_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO NOTHING`,
		t.Path, t.ApplicationID, t.ConfigFamilyID, t.InstallationID, t.TargetID, ts(t.CreatedAt))
	return err
}

// GetPathOwner returns the registered owner of a managed path.
func (db *DB) GetPathOwner(path string) (PathOwner, error) {
	var p PathOwner
	var created string
	err := db.sql.QueryRow(`SELECT path, application_id, config_family_id, installation_id, target_id, created_at
		FROM path_ownership WHERE path = ?`, path).
		Scan(&p.Path, &p.ApplicationID, &p.ConfigFamilyID, &p.InstallationID, &p.TargetID, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return p, NotFoundf("path owner for %s", path)
	}
	if err != nil {
		return p, err
	}
	p.CreatedAt = parseTS(created)
	return p, nil
}

// ListTargets lists managed targets, optionally filtered by family.
func (db *DB) ListTargets(configFamilyID string) ([]ManagedTarget, error) {
	q := `SELECT target_id, application_id, config_family_id, target_type, path,
	             installation_id, reload_policy, atomicity, health, created_at FROM managed_targets`
	args := []any{}
	if configFamilyID != "" {
		q += ` WHERE config_family_id = ?`
		args = append(args, configFamilyID)
	}
	q += ` ORDER BY path`
	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ManagedTarget
	for rows.Next() {
		var t ManagedTarget
		var created string
		if err := rows.Scan(&t.TargetID, &t.ApplicationID, &t.ConfigFamilyID, &t.TargetType,
			&t.Path, &t.InstallationID, &t.ReloadPolicy, &t.Atomicity, &t.Health, &created); err != nil {
			return nil, err
		}
		t.CreatedAt = parseTS(created)
		out = append(out, t)
	}
	return out, rows.Err()
}
