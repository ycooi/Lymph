package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/ledger"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
)

// Manifest is an application registration (section 7). It is stored as an
// immutable revision; a change creates another revision rather than editing
// history in place.
type Manifest struct {
	ApplicationID string `json:"application_id,omitempty"`
	Name          string `json:"name"`
	Type          string `json:"type,omitempty"`
	Owner         string `json:"owner,omitempty"`
	Repository    string `json:"repository,omitempty"`
	// InstallationID is the default installation: the deployment that a request
	// not naming one belongs to.
	InstallationID string `json:"installation_id,omitempty"`
	// Installations declares every deployment of this application.
	Installations  []InstallationSpec `json:"installations,omitempty"`
	Junctions      []JunctionSpec     `json:"junctions,omitempty"`
	ConfigFamilies []ConfigFamilySpec `json:"config_families,omitempty"`
}

// JunctionSpec declares a place where the application may discover that its
// current behaviour is inadequate (section 8).
type JunctionSpec struct {
	JunctionID    string   `json:"junction_id,omitempty"`
	Name          string   `json:"name"`
	Workflow      string   `json:"improvement_workflow,omitempty"`
	ConfigFamily  string   `json:"config_family,omitempty"`
	FeedbackTypes []string `json:"feedback_types,omitempty"`
	ReplayAdapter string   `json:"replay_adapter,omitempty"`
	Contract      string   `json:"contract,omitempty"`
}

// ConfigFamilySpec declares one independently versioned configuration domain
// (section 24) and its managed targets (section 25).
type ConfigFamilySpec struct {
	ConfigFamilyID  string `json:"config_family_id,omitempty"`
	Name            string `json:"name"`
	SchemaRevision  string `json:"schema_revision,omitempty"`
	Workflow        string `json:"workflow_type,omitempty"`
	RequiresHoldout bool   `json:"requires_holdout,omitempty"`
	// ManagementMode is LYMPH_MANAGED or EXTERNAL, and defaults to EXTERNAL: a
	// family must opt in before Lymph may ever deliver it to a node
	// (mission sections 5, 6).
	ManagementMode string       `json:"management_mode,omitempty"`
	Targets        []TargetSpec `json:"targets,omitempty"`
}

// TargetSpec is a pre-registered deployment destination. Events can never
// invent one (section 25, section 71).
type TargetSpec struct {
	TargetID string `json:"target_id,omitempty"`
	// InstallationID defaults to the application's default installation.
	InstallationID string `json:"installation_id,omitempty"`
	Type           string `json:"target_type"`
	Path           string `json:"path"`
	ReloadPolicy   string `json:"reload_policy,omitempty"`
	Atomicity      string `json:"atomicity,omitempty"`
	Health         string `json:"health,omitempty"`
}

// Registration is the result of registering or updating an application.
type Registration struct {
	Application           projection.Application  `json:"application"`
	Registration          projection.Registration `json:"registration"`
	Changed               bool                    `json:"changed"`
	JunctionIDs           map[string]string       `json:"junction_ids"`
	FamilyIDs             map[string]string       `json:"config_family_ids"`
	InstallationIDs       []string                `json:"installation_ids"`
	DefaultInstallationID string                  `json:"default_installation_id"`
}

// RegisterApplication writes one immutable registration revision (section 7).
//
// Re-submitting an identical manifest is a no-op and returns Changed=false.
func (e *Engine) RegisterApplication(ctx context.Context, m Manifest) (Registration, error) {
	if m.Name == "" {
		return Registration{}, errors.New("manifest requires a name")
	}
	if m.ApplicationID == "" {
		m.ApplicationID = identity.NewID()
	} else if !identity.Valid(m.ApplicationID) {
		return Registration{}, fmt.Errorf("application_id %q is not a UUID", m.ApplicationID)
	}

	canonical, err := json.Marshal(m)
	if err != nil {
		return Registration{}, err
	}
	manifestHash := objectstore.HashOf(canonical)
	now := time.Now().UTC()

	// Installation identity is normalized before anything is written, so the
	// rules are one place and the ledger never records an ambiguous manifest
	// (mission section 40).
	installations, defaultInstallation, err := normalizeInstallations(m.ApplicationID, m.InstallationID, m.Installations)
	if err != nil {
		return Registration{}, err
	}

	var out Registration
	err = e.withTx(func(tx *sql.Tx) error {
		existing, err := projection.GetApplicationTx(tx, m.ApplicationID)
		switch {
		case err != nil && !errors.Is(err, projection.ErrNotFound):
			return err
		case err == nil:
			// An identical manifest is not new history (section 7): report the
			// current identity index without writing another revision.
			priorHash, hashErr := latestRegistrationHashTx(tx, m.ApplicationID)
			if hashErr != nil {
				return hashErr
			}
			if priorHash != "" && priorHash == manifestHash {
				junctionIDs, familyIDs, err := registrationIndexTx(tx, m.ApplicationID)
				if err != nil {
					return err
				}
				out = Registration{
					Application:           existing,
					Registration:          projection.Registration{ApplicationID: m.ApplicationID, ContentHash: manifestHash},
					Changed:               false,
					JunctionIDs:           junctionIDs,
					FamilyIDs:             familyIDs,
					DefaultInstallationID: projection.DefaultInstallationID(existing),
				}
				existingInstallations, listErr := projection.ListInstallationsTx(tx, m.ApplicationID)
				if listErr != nil {
					return listErr
				}
				for _, installation := range existingInstallations {
					out.InstallationIDs = append(out.InstallationIDs, installation.InstallationID)
				}
				return nil
			}
			// A name is metadata, but it must stay a usable handle: refuse to
			// let one identity silently take another identity's name.
			byName, nameErr := projection.GetApplicationByNameTx(tx, m.Name)
			if nameErr == nil && byName.ApplicationID != m.ApplicationID {
				return fmt.Errorf("%w: %s is already used by application %s",
					projection.ErrNameConflict, m.Name, byName.ApplicationID)
			}
			if nameErr != nil && !errors.Is(nameErr, projection.ErrNotFound) {
				return nameErr
			}
		}

		revision := existing.RegistrationRevision + 1
		if existing.CreatedAt.IsZero() {
			existing.CreatedAt = now
		}

		app := projection.Application{
			ApplicationID:         m.ApplicationID,
			Name:                  m.Name,
			AppType:               m.Type,
			Owner:                 m.Owner,
			Repository:            m.Repository,
			InstallationID:        m.InstallationID,
			DefaultInstallationID: defaultInstallation,
			RegistrationRevision:  revision,
			CreatedAt:             existing.CreatedAt,
			UpdatedAt:             now,
		}

		installationRows := make([]projection.Installation, 0, len(installations))
		for _, spec := range installations {
			created := now
			if prior, err := projection.GetInstallationTx(tx, m.ApplicationID, spec.InstallationID); err == nil {
				created = prior.CreatedAt
			}
			installationRows = append(installationRows, projection.Installation{
				ApplicationID:  m.ApplicationID,
				InstallationID: spec.InstallationID,
				Name:           spec.Name,
				Environment:    spec.Environment,
				SessionPolicy:  sessionPolicyOfSpec(spec),
				Active:         true,
				CreatedAt:      created,
				UpdatedAt:      now,
			})
		}

		// Installations are written before the rest of the manifest is
		// validated, because targets resolve their installation from this table.
		// The transaction is all-or-nothing, so ordering inside it is free.
		for _, installation := range installationRows {
			if err := projection.UpsertInstallation(tx, installation); err != nil {
				return err
			}
		}

		reg := projection.Registration{
			RegistrationID: identity.NewID(),
			ApplicationID:  m.ApplicationID,
			Revision:       revision,
			Manifest:       string(canonical),
			ContentHash:    manifestHash,
			CreatedAt:      now,
		}

		// The identity of a junction or family is its UUID (section 4). A
		// re-registration reuses the existing UUID rather than minting a new
		// one, so history stays connected.
		junctions := make([]projection.Junction, 0, len(m.Junctions))
		junctionIDs := map[string]string{}
		for _, spec := range m.Junctions {
			if spec.Name == "" {
				return errors.New("junction requires a name")
			}
			id := spec.JunctionID
			if id == "" {
				if prior, err := projection.GetJunctionByNameTx(tx, m.ApplicationID, spec.Name); err == nil {
					id = prior.JunctionID
				} else if errors.Is(err, projection.ErrNotFound) {
					id = identity.NewID()
				} else {
					return err
				}
			}
			j := projection.Junction{
				JunctionID:    id,
				ApplicationID: m.ApplicationID,
				Name:          spec.Name,
				WorkflowType:  spec.Workflow,
				FeedbackTypes: spec.FeedbackTypes,
				Contract:      spec.Contract,
				ReplayAdapter: spec.ReplayAdapter,
				Active:        true,
				CreatedAt:     now,
			}
			junctions = append(junctions, j)
			junctionIDs[spec.Name] = id
		}

		families := make([]projection.ConfigFamily, 0, len(m.ConfigFamilies))
		targets := []projection.ManagedTarget{}
		familyIDs := map[string]string{}
		for _, spec := range m.ConfigFamilies {
			if spec.Name == "" {
				return errors.New("config family requires a name")
			}
			id := spec.ConfigFamilyID
			if id == "" {
				if prior, err := projection.GetConfigFamilyByNameTx(tx, m.ApplicationID, spec.Name); err == nil {
					id = prior.ConfigFamilyID
				} else if errors.Is(err, projection.ErrNotFound) {
					id = identity.NewID()
				} else {
					return err
				}
			}
			families = append(families, projection.ConfigFamily{
				ConfigFamilyID:  id,
				ApplicationID:   m.ApplicationID,
				Name:            spec.Name,
				SchemaRevision:  spec.SchemaRevision,
				WorkflowType:    spec.Workflow,
				RequiresHoldout: spec.RequiresHoldout,
				ManagementMode:  projection.NormalizeManagementMode(spec.ManagementMode),
				CreatedAt:       now,
			})
			familyIDs[spec.Name] = id
			for _, t := range spec.Targets {
				if t.Path == "" {
					return fmt.Errorf("config family %s: managed target requires a path", spec.Name)
				}
				targetID := t.TargetID
				if targetID == "" {
					targetID = identity.NewID()
				}
				// A target belongs to one installation. Omitting it means the
				// application's default, never "every installation".
				installationID, err := e.resolveInstallationTx(tx, app, t.InstallationID)
				if err != nil {
					return fmt.Errorf("config family %s target %s: %w", spec.Name, t.Path, err)
				}
				targets = append(targets, projection.ManagedTarget{
					TargetID:       targetID,
					ApplicationID:  m.ApplicationID,
					ConfigFamilyID: id,
					InstallationID: installationID,
					TargetType:     t.Type,
					Path:           t.Path,
					ReloadPolicy:   t.ReloadPolicy,
					Atomicity:      t.Atomicity,
					Health:         t.Health,
					CreatedAt:      now,
				})
			}
		}

		// Junction → config family link, by name.
		for i := range junctions {
			for _, spec := range m.Junctions {
				if spec.Name != junctions[i].Name || spec.ConfigFamily == "" {
					continue
				}
				id, ok := familyIDs[spec.ConfigFamily]
				if !ok {
					if prior, err := projection.GetConfigFamilyByNameTx(tx, m.ApplicationID, spec.ConfigFamily); err == nil {
						id = prior.ConfigFamilyID
					} else {
						return fmt.Errorf("junction %s references unknown config family %s",
							spec.Name, spec.ConfigFamily)
					}
				}
				junctions[i].ConfigFamilyID = id
			}
		}

		rec, err := e.ledger.Append(ledger.KindRegistration, m.ApplicationID, registrationPayload{
			Application:   app,
			Registration:  reg,
			Installations: installationRows,
			Junctions:     junctions,
			Families:      families,
			Targets:       targets,
		}, ledger.Durable)
		if err != nil {
			return err
		}
		reg.LedgerSequence = rec.Sequence

		if err := projection.UpsertApplication(tx, app); err != nil {
			return err
		}
		if err := projection.InsertRegistration(tx, reg); err != nil {
			return err
		}
		for _, j := range junctions {
			if err := projection.UpsertJunction(tx, j); err != nil {
				return err
			}
		}
		for _, f := range families {
			if err := projection.UpsertConfigFamily(tx, f); err != nil {
				return err
			}
		}
		for _, t := range targets {
			if err := projection.RegisterTarget(tx, t); err != nil {
				return err
			}
		}

		audit := projection.AuditEvent{
			AuditID:       identity.NewID(),
			Action:        "application.registered",
			Actor:         app.Owner,
			ApplicationID: app.ApplicationID,
			SubjectKind:   "application",
			SubjectID:     app.ApplicationID,
			Detail:        fmt.Sprintf(`{"name":%q,"revision":%d,"manifest_hash":%q}`, app.Name, revision, manifestHash),
			CreatedAt:     now,
		}
		if err := appendAuditTx(e, tx, audit, ledger.Async); err != nil {
			return err
		}

		if err := projection.IndexLedgerRecordTx(tx, rec); err != nil {
			return err
		}
		if err := checkpoint(tx, rec); err != nil {
			return err
		}

		out = Registration{
			Application:           app,
			Registration:          reg,
			Changed:               true,
			JunctionIDs:           junctionIDs,
			FamilyIDs:             familyIDs,
			DefaultInstallationID: defaultInstallation,
		}
		for _, installation := range installationRows {
			out.InstallationIDs = append(out.InstallationIDs, installation.InstallationID)
		}
		return nil
	})
	if err != nil {
		return Registration{}, err
	}
	return out, nil
}

// latestRegistrationHashTx returns the newest registration manifest hash.
func latestRegistrationHashTx(tx *sql.Tx, applicationID string) (string, error) {
	var hash string
	err := tx.QueryRow(`SELECT content_hash FROM app_registrations
		WHERE application_id = ? ORDER BY revision DESC LIMIT 1`, applicationID).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return hash, nil
}

// registrationIndexTx returns the current name to UUID mapping.
func registrationIndexTx(tx *sql.Tx, applicationID string) (map[string]string, map[string]string, error) {
	junctionIDs := map[string]string{}
	familyIDs := map[string]string{}

	rows, err := tx.Query(`SELECT name, junction_id FROM junctions WHERE application_id = ?`, applicationID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name, id string
		if err := rows.Scan(&name, &id); err != nil {
			return nil, nil, err
		}
		junctionIDs[name] = id
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	frows, err := tx.Query(`SELECT name, config_family_id FROM config_families WHERE application_id = ?`, applicationID)
	if err != nil {
		return nil, nil, err
	}
	defer frows.Close()
	for frows.Next() {
		var name, id string
		if err := frows.Scan(&name, &id); err != nil {
			return nil, nil, err
		}
		familyIDs[name] = id
	}
	if err := frows.Err(); err != nil {
		return nil, nil, err
	}
	return junctionIDs, familyIDs, nil
}

// appendAuditTx writes one audit record, canonically and in the projection
// (section 95).
func appendAuditTx(e *Engine, tx *sql.Tx, audit projection.AuditEvent, durability ledger.Durability) error {
	rec, err := e.ledger.Append(ledger.KindAudit, audit.AuditID, auditPayload{Audit: audit}, durability)
	if err != nil {
		return err
	}
	audit.LedgerSequence = rec.Sequence
	if err := projection.InsertAuditTx(tx, audit); err != nil {
		return err
	}
	return projection.IndexLedgerRecordTx(tx, rec)
}

// Applications lists registered applications.
func (e *Engine) Applications() ([]projection.Application, error) { return e.db.ListApplications() }

// Junctions lists junctions, optionally filtered by application.
func (e *Engine) Junctions(applicationID string) ([]projection.Junction, error) {
	return e.db.ListJunctions(applicationID)
}

// ConfigFamilies lists config families, optionally filtered by application.
func (e *Engine) ConfigFamilies(applicationID string) ([]projection.ConfigFamily, error) {
	return e.db.ListConfigFamilies(applicationID)
}

// Targets lists managed targets, optionally filtered by config family.
func (e *Engine) Targets(configFamilyID string) ([]projection.ManagedTarget, error) {
	return e.db.ListTargets(configFamilyID)
}
