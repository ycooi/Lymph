package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/ledger"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
)

// A worker does not necessarily produce a configuration. It may produce a
// commit, a model reference, a data fix, or the conclusion that nothing should
// change at all. Lymph records whatever it produced without pretending every
// answer is a YAML file.
//
// This file owns worker output. config.go stays about configuration.

// ArtifactKind is the closed vocabulary of worker output.
type ArtifactKind string

// The five kinds a worker may return.
const (
	ArtifactConfigBundle ArtifactKind = "CONFIG_BUNDLE"
	ArtifactCodeRef      ArtifactKind = "CODE_REF"
	ArtifactModelRef     ArtifactKind = "MODEL_REF"
	ArtifactDataFixRef   ArtifactKind = "DATA_FIX_REF"
	ArtifactNoChange     ArtifactKind = "NO_CHANGE"
)

// ArtifactKinds lists the vocabulary in stable order.
func ArtifactKinds() []ArtifactKind {
	return []ArtifactKind{
		ArtifactConfigBundle, ArtifactCodeRef, ArtifactModelRef,
		ArtifactDataFixRef, ArtifactNoChange,
	}
}

// ValidArtifactKind reports whether a kind is part of the vocabulary. Unknown
// kinds fail hard: "MODEL", "CONFIG", "FILE", "OTHER" and "" are not accepted
// (mission section 5).
func ValidArtifactKind(kind ArtifactKind) bool {
	for _, known := range ArtifactKinds() {
		if kind == known {
			return true
		}
	}
	return false
}

// NoChangeDispositions are the recognised reasons for leaving a config alone.
var NoChangeDispositions = []string{
	"CURRENT_CONFIG_CORRECT",
	"TRANSIENT_UPSTREAM_FAILURE",
	"FALSE_POSITIVE",
	"DUPLICATE",
	"EXTERNAL_CAUSE",
	"OTHER",
}

// ValidNoChangeDisposition reports whether a supplied disposition is known.
// An empty disposition is allowed; an unknown one is not.
func ValidNoChangeDisposition(d string) bool {
	if d == "" {
		return true
	}
	for _, known := range NoChangeDispositions {
		if d == known {
			return true
		}
	}
	return false
}

// ErrInvalidArtifact is returned when worker output is malformed. It is a
// contract violation by the worker, not a Lymph failure.
var ErrInvalidArtifact = errors.New("invalid improvement artifact")

// ErrStaleWorkerAttempt is returned when a worker returns work it no longer
// holds: the lease moved on, or the attempt number is not current.
var ErrStaleWorkerAttempt = errors.New("stale worker attempt: that lease is no longer held by this worker")

// ErrAmbiguousInstallation is returned when a work item would span several
// installations' configuration state and the caller did not choose one.
var ErrAmbiguousInstallation = errors.New("AMBIGUOUS_INSTALLATION")

// ---------- typed payloads ----------

// ConfigBundleArtifactRequest asks Lymph to create a configuration candidate.
// The application, family, installation and deployment target are inherited
// from registered state; a worker cannot invent them here.
type ConfigBundleArtifactRequest struct {
	Bundle         objectstore.Bundle `json:"bundle"`
	Explanation    string             `json:"explanation,omitempty"`
	BaseRevisionID string             `json:"base_revision_id,omitempty"`
}

// CodeRefArtifactRequest records a code change. Lymph never clones or stores
// the repository.
type CodeRefArtifactRequest struct {
	Repository  string `json:"repository"`
	Commit      string `json:"commit"`
	TreeHash    string `json:"tree_hash,omitempty"`
	Branch      string `json:"branch,omitempty"`
	Description string `json:"description,omitempty"`
}

// ModelRefArtifactRequest records a model artifact that lives elsewhere.
type ModelRefArtifactRequest struct {
	URI         string `json:"uri"`
	Digest      string `json:"digest"`
	ModelType   string `json:"model_type,omitempty"`
	Version     string `json:"version,omitempty"`
	Description string `json:"description,omitempty"`
}

// DataFixRefArtifactRequest records a correction to data rather than code or
// configuration. Lymph routes and records it; it never performs the mutation.
type DataFixRefArtifactRequest struct {
	URI         string `json:"uri,omitempty"`
	Digest      string `json:"digest,omitempty"`
	PatchID     string `json:"patch_id,omitempty"`
	Target      string `json:"target,omitempty"`
	Description string `json:"description,omitempty"`
}

// NoChangeArtifactRequest records the conclusion that the current
// configuration is correct. That conclusion is evidence, not a failure.
type NoChangeArtifactRequest struct {
	Reason      string `json:"reason"`
	Disposition string `json:"disposition,omitempty"`
}

// ImprovementArtifactRequest is one typed artifact. Exactly one payload
// matching Kind must be present, and all other payloads must be absent.
type ImprovementArtifactRequest struct {
	Kind ArtifactKind `json:"kind"`

	ConfigBundle *ConfigBundleArtifactRequest `json:"config_bundle,omitempty"`
	CodeRef      *CodeRefArtifactRequest      `json:"code_ref,omitempty"`
	ModelRef     *ModelRefArtifactRequest     `json:"model_ref,omitempty"`
	DataFixRef   *DataFixRefArtifactRequest   `json:"data_fix_ref,omitempty"`
	NoChange     *NoChangeArtifactRequest     `json:"no_change,omitempty"`
}

// ReturnImprovementRequest is one worker return (mission section 14).
//
// Worker and Attempt are the lease token: a worker may only return the attempt
// it actually holds. That closes the stale-worker-result class where a late
// worker finishes an attempt that has already been re-leased.
type ReturnImprovementRequest struct {
	WorkID  string `json:"work_id"`
	Worker  string `json:"worker"`
	Attempt int    `json:"attempt"`

	WorkflowRunID string `json:"workflow_run_id,omitempty"`
	Summary       string `json:"summary,omitempty"`

	Artifacts []ImprovementArtifactRequest `json:"artifacts"`
}

// ReturnImprovementResponse is what the worker gets back.
type ReturnImprovementResponse struct {
	Result     projection.ImprovementResult     `json:"result"`
	Artifacts  []projection.ImprovementArtifact `json:"artifacts"`
	Candidates []projection.Candidate           `json:"candidates"`
	WorkItem   projection.WorkItem              `json:"work_item"`
	Idempotent bool                             `json:"idempotent,omitempty"`
}

// ---------- validation ----------

// validateArtifact checks one typed artifact: known kind, exactly one matching
// payload, and the payload's own minimum facts.
func validateArtifact(req ImprovementArtifactRequest) error {
	if !ValidArtifactKind(req.Kind) {
		return fmt.Errorf("%w: unknown kind %q; known kinds are %s", ErrInvalidArtifact,
			req.Kind, kindList())
	}

	supplied := 0
	for _, present := range []bool{
		req.ConfigBundle != nil, req.CodeRef != nil, req.ModelRef != nil,
		req.DataFixRef != nil, req.NoChange != nil,
	} {
		if present {
			supplied++
		}
	}
	if supplied != 1 {
		return fmt.Errorf("%w: kind %s requires exactly one matching payload, got %d",
			ErrInvalidArtifact, req.Kind, supplied)
	}

	switch req.Kind {
	case ArtifactConfigBundle:
		if req.ConfigBundle == nil {
			return fmt.Errorf("%w: kind CONFIG_BUNDLE requires config_bundle", ErrInvalidArtifact)
		}
		if len(req.ConfigBundle.Bundle) == 0 {
			return fmt.Errorf("%w: CONFIG_BUNDLE carries an empty bundle", ErrInvalidArtifact)
		}
		if err := ValidateBundleStructure(req.ConfigBundle.Bundle); err != nil {
			return err
		}
	case ArtifactCodeRef:
		if req.CodeRef == nil {
			return fmt.Errorf("%w: kind CODE_REF requires code_ref", ErrInvalidArtifact)
		}
		if strings.TrimSpace(req.CodeRef.Repository) == "" || strings.TrimSpace(req.CodeRef.Commit) == "" {
			return fmt.Errorf("%w: CODE_REF requires repository and commit", ErrInvalidArtifact)
		}
	case ArtifactModelRef:
		if req.ModelRef == nil {
			return fmt.Errorf("%w: kind MODEL_REF requires model_ref", ErrInvalidArtifact)
		}
		if strings.TrimSpace(req.ModelRef.URI) == "" {
			return fmt.Errorf("%w: MODEL_REF requires uri", ErrInvalidArtifact)
		}
		if strings.TrimSpace(req.ModelRef.Digest) == "" {
			return fmt.Errorf("%w: MODEL_REF requires digest", ErrInvalidArtifact)
		}
	case ArtifactDataFixRef:
		if req.DataFixRef == nil {
			return fmt.Errorf("%w: kind DATA_FIX_REF requires data_fix_ref", ErrInvalidArtifact)
		}
		fix := req.DataFixRef
		if strings.TrimSpace(fix.URI) == "" && strings.TrimSpace(fix.PatchID) == "" {
			return fmt.Errorf("%w: DATA_FIX_REF requires a uri or a patch_id", ErrInvalidArtifact)
		}
		if strings.TrimSpace(fix.URI) != "" && strings.TrimSpace(fix.PatchID) == "" && strings.TrimSpace(fix.Digest) == "" {
			return fmt.Errorf("%w: DATA_FIX_REF with a uri but no digest is not a stable reference", ErrInvalidArtifact)
		}
	case ArtifactNoChange:
		if req.NoChange == nil {
			return fmt.Errorf("%w: kind NO_CHANGE requires no_change", ErrInvalidArtifact)
		}
		if strings.TrimSpace(req.NoChange.Reason) == "" {
			return fmt.Errorf("%w: NO_CHANGE requires a reason", ErrInvalidArtifact)
		}
		if !ValidNoChangeDisposition(req.NoChange.Disposition) {
			return fmt.Errorf("%w: unknown NO_CHANGE disposition %q", ErrInvalidArtifact, req.NoChange.Disposition)
		}
	}
	return nil
}

func kindList() string {
	kinds := ArtifactKinds()
	out := make([]string, len(kinds))
	for i, kind := range kinds {
		out[i] = string(kind)
	}
	return strings.Join(out, ", ")
}

// artifactPayload renders the small immutable metadata stored for an artifact.
func artifactPayload(req ImprovementArtifactRequest, plan *candidatePlan) (string, string, error) {
	switch req.Kind {
	case ArtifactConfigBundle:
		// The bundle bytes live in the object store and are referenced by the
		// candidate; the artifact carries the reference, not a copy.
		payload := map[string]any{
			"explanation":      req.ConfigBundle.Explanation,
			"base_revision_id": plan.BaseRevisionID,
			"root_tree_hash":   plan.TreeHash,
			"files":            blobManifest(plan.Blobs),
			"file_count":       len(plan.Blobs),
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return "", "", err
		}
		return string(raw), plan.TreeHash, nil

	case ArtifactCodeRef:
		raw, err := json.Marshal(map[string]any{
			"repository":  req.CodeRef.Repository,
			"commit":      req.CodeRef.Commit,
			"tree_hash":   req.CodeRef.TreeHash,
			"branch":      req.CodeRef.Branch,
			"description": req.CodeRef.Description,
		})
		if err != nil {
			return "", "", err
		}
		return string(raw), objectstore.HashOf(raw), nil

	case ArtifactModelRef:
		raw, err := json.Marshal(map[string]any{
			"uri":         req.ModelRef.URI,
			"digest":      req.ModelRef.Digest,
			"model_type":  req.ModelRef.ModelType,
			"version":     req.ModelRef.Version,
			"description": req.ModelRef.Description,
		})
		if err != nil {
			return "", "", err
		}
		// The content hash is over the reference metadata, not the model. Lymph
		// never downloads or copies the artifact itself.
		return string(raw), objectstore.HashOf(raw), nil

	case ArtifactDataFixRef:
		raw, err := json.Marshal(map[string]any{
			"uri":         req.DataFixRef.URI,
			"digest":      req.DataFixRef.Digest,
			"patch_id":    req.DataFixRef.PatchID,
			"target":      req.DataFixRef.Target,
			"description": req.DataFixRef.Description,
		})
		if err != nil {
			return "", "", err
		}
		hash := req.DataFixRef.Digest
		if hash == "" {
			hash = objectstore.HashOf(raw)
		}
		return string(raw), hash, nil

	case ArtifactNoChange:
		raw, err := json.Marshal(map[string]any{
			"reason":      req.NoChange.Reason,
			"disposition": req.NoChange.Disposition,
		})
		if err != nil {
			return "", "", err
		}
		return string(raw), objectstore.HashOf(raw), nil
	}
	return "", "", fmt.Errorf("%w: kind %q", ErrInvalidArtifact, req.Kind)
}

func blobManifest(entries []objectstore.Entry) []map[string]any {
	out := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		out = append(out, map[string]any{
			"path": entry.Path, "hash": entry.Hash, "mode": entry.Mode, "size": entry.Size,
		})
	}
	return out
}

// ---------- the atomic return ----------

// ReturnImprovement records one complete worker return: the result, every
// artifact, any config candidates the artifacts created, and the work item
// moving to RETURNED — as one canonical ledger record (mission sections 16, 17,
// 18, 92).
//
// Order of operations, and why:
//
//  1. validate everything in memory, including structural validation of any
//     config bundle;
//  2. write the immutable objects the bundle needs;
//  3. append the canonical record;
//  4. write the projection rows and commit.
//
// A failure before step 3 leaves nothing canonical; a failure between 3 and 4
// leaves a record that the next start replays. Unreferenced objects written in
// step 2 are harmless: content-addressed and immutable.
func (e *Engine) ReturnImprovement(ctx context.Context, req ReturnImprovementRequest) (ReturnImprovementResponse, error) {
	if strings.TrimSpace(req.WorkID) == "" {
		return ReturnImprovementResponse{}, fmt.Errorf("%w: work_id is required", ErrInvalidArtifact)
	}
	if strings.TrimSpace(req.Worker) == "" {
		return ReturnImprovementResponse{}, fmt.Errorf("%w: worker is required for a leased result", ErrInvalidArtifact)
	}
	if req.Attempt <= 0 {
		return ReturnImprovementResponse{}, fmt.Errorf("%w: attempt must be positive", ErrInvalidArtifact)
	}
	if len(req.Artifacts) == 0 {
		return ReturnImprovementResponse{}, fmt.Errorf("%w: a return needs at least one artifact", ErrInvalidArtifact)
	}
	for i, artifact := range req.Artifacts {
		if err := validateArtifact(artifact); err != nil {
			return ReturnImprovementResponse{}, fmt.Errorf("artifact %d: %w", i, err)
		}
	}

	// Objects for config bundles are written before the canonical append.
	type prepared struct {
		plan  candidatePlan
		index int
	}
	var preparedBundles []prepared

	// A dry-run of the transaction's decisions, so object writes happen before
	// the ledger append but under the same facts.
	now := time.Now().UTC()
	_ = now

	for i, artifact := range req.Artifacts {
		if artifact.Kind != ArtifactConfigBundle {
			continue
		}
		preparedBundles = append(preparedBundles, prepared{index: i, plan: candidatePlan{
			Bundle:         artifact.ConfigBundle.Bundle,
			Explanation:    artifact.ConfigBundle.Explanation,
			BaseRevisionID: artifact.ConfigBundle.BaseRevisionID,
		}})
	}

	// Tree objects are content-addressed, so writing them up front is safe and
	// idempotent even if the transaction later refuses the return.
	plans := map[int]candidatePlan{}
	for _, item := range preparedBundles {
		treeHash, entries, err := e.putBundle(item.plan.Bundle)
		if err != nil {
			return ReturnImprovementResponse{}, err
		}
		plan := item.plan
		plan.TreeHash = treeHash
		plan.Blobs = entries
		plans[item.index] = plan
	}

	var response ReturnImprovementResponse

	err := e.withTx(func(tx *sql.Tx) error {
		work, err := projection.GetWorkItemTx(tx, req.WorkID)
		if err != nil {
			return err
		}

		// Idempotency: a repeated return of the same work item resolves to the
		// stored result rather than creating a second one.
		if existing, err := projection.GetImprovementResultByWorkTx(tx, work.WorkID); err == nil {
			existingArtifacts, err := projection.ListImprovementArtifactsTx(tx, existing.ResultID)
			if err != nil {
				return err
			}
			response = ReturnImprovementResponse{
				Result:     existing,
				Artifacts:  existingArtifacts,
				WorkItem:   work,
				Idempotent: true,
			}
			return nil
		} else if !errors.Is(err, projection.ErrNotFound) {
			return err
		}

		// Lease token: the worker must hold this attempt right now.
		if work.LeaseOwner != req.Worker || work.Attempts != req.Attempt {
			return fmt.Errorf("%w: work %s is held by %q at attempt %d, not by %q at attempt %d",
				ErrStaleWorkerAttempt, work.WorkID, work.LeaseOwner, work.Attempts, req.Worker, req.Attempt)
		}
		if WorkState(work.State) != WorkLeased && WorkState(work.State) != WorkRunning {
			return fmt.Errorf("%w: work %s is %s, not leased", ErrStaleWorkerAttempt, work.WorkID, work.State)
		}

		app, err := projection.GetApplicationTx(tx, work.ApplicationID)
		if err != nil {
			return err
		}
		installationID, err := e.resolveInstallationTx(tx, app, work.InstallationID)
		if err != nil {
			return err
		}

		resultID := identity.NewID()
		result := projection.ImprovementResult{
			ResultID:      resultID,
			WorkItemID:    work.WorkID,
			WorkflowRunID: req.WorkflowRunID,
			ApplicationID: work.ApplicationID,
			IssueIDs:      work.IssueIDs,
			Worker:        req.Worker,
			Attempt:       req.Attempt,
			Summary:       req.Summary,
			CreatedAt:     now,
		}

		artifacts := make([]projection.ImprovementArtifact, 0, len(req.Artifacts))
		candidates := make([]projection.Candidate, 0, len(req.Artifacts))

		for i, artifact := range req.Artifacts {
			record := projection.ImprovementArtifact{
				ArtifactID:     identity.NewID(),
				ResultID:       resultID,
				ApplicationID:  work.ApplicationID,
				ConfigFamilyID: work.ConfigFamilyID,
				InstallationID: installationID,
				Kind:           string(artifact.Kind),
				CreatedAt:      now,
			}

			var plan *candidatePlan
			if artifact.Kind == ArtifactConfigBundle {
				value := plans[i]
				value.ApplicationID = work.ApplicationID
				value.ConfigFamilyID = work.ConfigFamilyID
				value.InstallationID = installationID
				value.IssueIDs = work.IssueIDs
				value.WorkItemID = work.WorkID
				if value.Explanation == "" {
					value.Explanation = req.Summary
				}
				resolved, err := e.resolveCandidatePlanTx(tx, app, value)
				if err != nil {
					return err
				}
				plan = &resolved
			}

			payload, contentHash, err := artifactPayload(artifact, plan)
			if err != nil {
				return err
			}
			record.Payload = payload
			record.ContentHash = contentHash

			if plan != nil {
				candidate := buildCandidate(*plan, resultID, now)
				record.CandidateID = candidate.CandidateID
				record.ConfigFamilyID = candidate.ConfigFamilyID
				record.InstallationID = candidate.InstallationID
				candidates = append(candidates, candidate)
			}
			artifacts = append(artifacts, record)
		}

		returned := work
		if err := CheckWorkTransition(WorkState(work.State), WorkReturned); err != nil {
			return err
		}
		returned.State = string(WorkReturned)
		returned.LeaseOwner = ""
		returned.LeaseExpiresAt = time.Time{}
		returned.ImprovementResultID = resultID
		returned.UpdatedAt = now

		record, err := e.ledger.Append(ledger.KindImprovementResult, resultID, improvementResultPayload{
			Result:     result,
			Artifacts:  artifacts,
			Candidates: candidates,
			WorkItem:   returned,
		}, ledger.Durable)
		if err != nil {
			return err
		}

		if err := projection.InsertImprovementResult(tx, result); err != nil {
			return err
		}
		for _, artifact := range artifacts {
			if err := projection.InsertImprovementArtifact(tx, artifact); err != nil {
				return err
			}
		}
		for _, candidate := range candidates {
			if err := projection.InsertCandidate(tx, candidate); err != nil {
				return err
			}
			if err := e.moveRefTx(tx, refMove{
				ApplicationID:    candidate.ApplicationID,
				ConfigFamilyID:   candidate.ConfigFamilyID,
				InstallationID:   candidate.InstallationID,
				RefName:          CandidateRefName(candidate.CandidateID),
				RevisionRootTree: candidate.RootTreeHash,
				Reason:           "candidate branch created",
				CandidateID:      candidate.CandidateID,
				Actor:            req.Worker,
				At:               now,
			}); err != nil {
				return err
			}
		}
		if err := projection.InsertWorkItem(tx, returned); err != nil {
			return err
		}
		if err := projection.IndexLedgerRecordTx(tx, record); err != nil {
			return err
		}
		if err := checkpoint(tx, record); err != nil {
			return err
		}

		response = ReturnImprovementResponse{
			Result:     result,
			Artifacts:  artifacts,
			Candidates: candidates,
			WorkItem:   returned,
		}
		return nil
	})
	if err != nil {
		return ReturnImprovementResponse{}, err
	}

	e.log.Info("improvement returned",
		"work", req.WorkID, "result", response.Result.ResultID,
		"artifacts", len(response.Artifacts), "candidates", len(response.Candidates),
		"worker", req.Worker, "attempt", req.Attempt, "idempotent", response.Idempotent)
	return response, nil
}

// putBundle writes a bundle to the object store and returns its tree address
// plus the blob manifest.
func (e *Engine) putBundle(bundle objectstore.Bundle) (string, []objectstore.Entry, error) {
	treeHash, err := e.objects.PutBundle(bundle)
	if err != nil {
		return "", nil, err
	}
	tree, err := e.objects.GetTree(treeHash)
	if err != nil {
		return "", nil, err
	}
	entries := append([]objectstore.Entry(nil), tree.Entries...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return treeHash, entries, nil
}

// Improvements lists worker returns, newest first.
func (e *Engine) Improvements(applicationID string, limit int) ([]projection.ImprovementResult, error) {
	return e.db.ListImprovementResults(applicationID, limit)
}

// Artifacts lists artifacts with optional filters.
func (e *Engine) Artifacts(filter projection.ArtifactFilter) ([]projection.ImprovementArtifact, error) {
	return e.db.ListImprovementArtifacts(filter)
}

// Installations lists registered installations, optionally by application.
func (e *Engine) Installations(applicationID string) ([]projection.Installation, error) {
	return e.db.ListInstallations(applicationID)
}
