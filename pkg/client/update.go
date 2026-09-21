package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/ycooi/Lymph/internal/protocol"
)

// Approved update delivery (mission sections 26, 29, 30, 58).
//
// The SDK deliberately exposes no ApplyUpdate. Lymph never writes an
// application's configuration: it publishes that an exact revision is eligible
// for an exact installation, the application fetches the bytes, verifies them
// against the hash the human approved, and then does whatever its own lifecycle
// requires. Only the application knows whether a reload is safe.
//
// So the four calls here are:
//
//	CheckUpdate   — is there something approved for me?
//	FetchUpdate   — give me the exact bytes, verified locally
//	ReportApplied — I am running it
//	ReportRejected— I am not, and here is why

// ApprovedUpdate is Lymph's answer to "may this installation take this
// revision?". It carries no configuration bytes: it is a reference to immutable
// content plus the approval that authorises it (mission section 18).
type ApprovedUpdate struct {
	ApplicationID    string `json:"application_id"`
	InstallationID   string `json:"installation_id"`
	ConfigFamilyID   string `json:"config_family_id"`
	RevisionID       string `json:"revision_id"`
	ContentHash      string `json:"content_hash"`
	ApprovalID       string `json:"approval_id"`
	BaseRevisionID   string `json:"base_revision_id,omitempty"`
	SchemaRevision   string `json:"schema_revision,omitempty"`
	RevisionSequence int    `json:"revision_sequence"`
	BaseCompatible   bool   `json:"base_compatible"`
	BaseReasonCode   string `json:"base_reason_code,omitempty"`
	// SkippedRevisions names revisions between what this node is running and
	// this update's base that the node already refused (or an operator
	// withdrew). They can never be applied, so stepping over them is not a base
	// mismatch (mission sections 39, 55).
	SkippedRevisions []string  `json:"skipped_revisions,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
}

// ManifestEntry is one file of a delivered bundle.
type ManifestEntry struct {
	Path string `json:"path"`
	Hash string `json:"hash"`
	Mode string `json:"mode"`
	Size int64  `json:"size"`
}

// UpdateContent is an approved revision's immutable bytes, verified.
type UpdateContent struct {
	Update       ApprovedUpdate
	RootTreeHash string
	Manifest     []ManifestEntry
	Bundle       map[string][]byte
}

// ApplicationResult is the daemon's canonical record of what a node reported.
type ApplicationResult struct {
	ResultID           string    `json:"result_id"`
	ApplicationID      string    `json:"application_id"`
	InstallationID     string    `json:"installation_id"`
	ConfigFamilyID     string    `json:"config_family_id"`
	RevisionID         string    `json:"revision_id"`
	ContentHash        string    `json:"content_hash"`
	ApprovalID         string    `json:"approval_id,omitempty"`
	PreviousRevisionID string    `json:"previous_revision_id,omitempty"`
	ObservedHash       string    `json:"observed_hash,omitempty"`
	Result             string    `json:"result"`
	ReasonCode         string    `json:"reason_code,omitempty"`
	Message            string    `json:"message,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
}

// Errors the update path can return.
var (
	// ErrIntegrity means the fetched bytes do not hash to what was approved.
	// The node must not apply them (mission section 29).
	ErrIntegrity = errors.New("INTEGRITY_ERROR")
	// ErrBaseRevisionMismatch means the approved update was built on a revision
	// this node is not running (mission section 55).
	ErrBaseRevisionMismatch = errors.New("BASE_REVISION_MISMATCH")
	// ErrNoUpdate is not an error condition: it means "nothing for you yet".
	ErrNoUpdate = errors.New("NO_UPDATE_AVAILABLE")
)

// HashPrefix is the content-address prefix Lymph uses.
const HashPrefix = "sha256:"

// HashOf computes a Lymph content address.
func HashOf(data []byte) string {
	sum := sha256.Sum256(data)
	return HashPrefix + hex.EncodeToString(sum[:])
}

// treeHash recomputes the address of a tree from its manifest. It mirrors the
// daemon's tree encoding exactly, and the update tests assert the two agree —
// a node that verified against a hash it could not reproduce would be trusting
// the daemon rather than checking it.
func treeHash(entries []ManifestEntry) (string, error) {
	ordered := append([]ManifestEntry(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Path < ordered[j].Path })
	raw, err := json.Marshal(struct {
		Kind    string          `json:"kind"`
		Entries []ManifestEntry `json:"entries"`
	}{Kind: "tree", Entries: ordered})
	if err != nil {
		return "", err
	}
	return HashOf(raw), nil
}

// CheckUpdates lists every approved update this installation may take, newest
// first. Identity comes from the client's session (mission section 25).
func (c *Client) CheckUpdates(ctx context.Context) ([]ApprovedUpdate, error) {
	updates, err := c.checkUpdates(ctx, "")
	if err != nil {
		return nil, err
	}
	return updates, nil
}

// CheckUpdate answers the narrower question an application actually asks:
// "is there something approved for this family that I am not running?".
//
// It returns (nil, nil) when there is nothing to do, which is the normal
// answer and must stay quiet. When an update exists but was built on a
// different revision than the one the caller says it is running, the update is
// returned together with ErrBaseRevisionMismatch: the caller needs to see what
// was offered to understand why it is refusing (mission sections 55, 57).
func (c *Client) CheckUpdate(ctx context.Context, configFamily, currentRevision string) (*ApprovedUpdate, error) {
	if configFamily == "" {
		return nil, fmt.Errorf("CheckUpdate needs a config family")
	}
	updates, err := c.checkUpdates(ctx, configFamily)
	if err != nil {
		return nil, err
	}
	if len(updates) == 0 {
		return nil, nil
	}
	// The daemon already sorts newest first; a superseded revision is not
	// offered because its successor is what the operator approved last
	// (mission section 56).
	newest := updates[0]
	if currentRevision != "" && newest.RevisionID == currentRevision {
		return nil, nil
	}
	if !newest.BaseCompatible {
		return &newest, ErrBaseRevisionMismatch
	}
	// The daemon decides base compatibility, because only it knows the
	// disposition history: a revision the node refused, or an operator
	// withdrew, can never be applied, so a later revision built on top of it is
	// still reachable and the daemon lists it under SkippedRevisions. The client
	// checks the one case the daemon cannot see: a caller naming a revision the
	// daemon has no report for at all.
	if currentRevision != "" && newest.BaseRevisionID != "" &&
		newest.BaseRevisionID != currentRevision && len(newest.SkippedRevisions) == 0 {
		newest.BaseCompatible = false
		newest.BaseReasonCode = ErrBaseRevisionMismatch.Error()
		return &newest, ErrBaseRevisionMismatch
	}
	return &newest, nil
}

func (c *Client) checkUpdates(ctx context.Context, configFamily string) ([]ApprovedUpdate, error) {
	if err := c.ensureSession(ctx); err != nil {
		return nil, err
	}
	session, ok := c.Session()
	if !ok {
		return nil, &UnavailableError{Err: errors.New("no session: cannot ask for approved updates")}
	}
	path := "/v1/updates" + Query("config_family", configFamily)
	var out struct {
		Updates []ApprovedUpdate `json:"updates"`
	}
	if err := c.getWithSession(ctx, path, session.SessionID, &out); err != nil {
		return nil, err
	}
	return out.Updates, nil
}

// FetchUpdate downloads an approved revision and verifies it locally against
// the approved content hash before returning it. A mismatch is an
// INTEGRITY_ERROR and the caller must not apply the content
// (mission sections 27 to 29).
func (c *Client) FetchUpdate(ctx context.Context, configFamily, revisionID string) (*UpdateContent, error) {
	if err := c.ensureSession(ctx); err != nil {
		return nil, err
	}
	session, ok := c.Session()
	if !ok {
		return nil, &UnavailableError{Err: errors.New("no session: cannot fetch an approved update")}
	}
	path := "/v1/updates/" + revisionID + "/content" + Query("config_family", configFamily)
	var out struct {
		Update       ApprovedUpdate    `json:"update"`
		RootTreeHash string            `json:"root_tree_hash"`
		Manifest     []ManifestEntry   `json:"manifest"`
		Bundle       map[string][]byte `json:"bundle"`
	}
	if err := c.getWithSession(ctx, path, session.SessionID, &out); err != nil {
		return nil, err
	}

	content := &UpdateContent{
		Update:       out.Update,
		RootTreeHash: out.RootTreeHash,
		Manifest:     out.Manifest,
		Bundle:       out.Bundle,
	}
	if err := verifyContent(content); err != nil {
		return nil, err
	}
	return content, nil
}

// verifyContent is the node's own check. It never asks the daemon whether the
// bytes are right; it recomputes.
func verifyContent(content *UpdateContent) error {
	if len(content.Bundle) == 0 {
		return fmt.Errorf("%w: the delivered bundle is empty", ErrIntegrity)
	}
	if len(content.Manifest) != len(content.Bundle) {
		return fmt.Errorf("%w: manifest lists %d files, bundle carries %d",
			ErrIntegrity, len(content.Manifest), len(content.Bundle))
	}
	for _, entry := range content.Manifest {
		data, ok := content.Bundle[entry.Path]
		if !ok {
			return fmt.Errorf("%w: manifest lists %s but the bundle does not carry it", ErrIntegrity, entry.Path)
		}
		if int64(len(data)) != entry.Size {
			return fmt.Errorf("%w: %s is %d bytes, manifest says %d", ErrIntegrity, entry.Path, len(data), entry.Size)
		}
		if got := HashOf(data); got != entry.Hash {
			return fmt.Errorf("%w: %s hashes to %s, manifest says %s", ErrIntegrity, entry.Path, got, entry.Hash)
		}
	}
	recomputed, err := treeHash(content.Manifest)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrIntegrity, err)
	}
	if recomputed != content.RootTreeHash {
		return fmt.Errorf("%w: bundle hashes to %s, approval covers %s",
			ErrIntegrity, recomputed, content.RootTreeHash)
	}
	if content.Update.ContentHash != "" && recomputed != content.Update.ContentHash {
		return fmt.Errorf("%w: fetched content is %s, the approved update is %s",
			ErrIntegrity, recomputed, content.Update.ContentHash)
	}
	return nil
}

// AppliedReport is what a node sends when it is running a revision.
type AppliedReport struct {
	ConfigFamilyID     string         `json:"config_family_id"`
	ObservedHash       string         `json:"observed_hash,omitempty"`
	PreviousRevisionID string         `json:"previous_revision_id,omitempty"`
	Details            map[string]any `json:"details,omitempty"`
}

// RejectedReport is what a node sends when it will not run a revision. The
// reason code is a closed vocabulary, because "it did not work" is not
// something a learning loop can act on (mission section 32).
type RejectedReport struct {
	ConfigFamilyID string         `json:"config_family_id"`
	ReasonCode     string         `json:"reason_code"`
	Message        string         `json:"message,omitempty"`
	Details        map[string]any `json:"details,omitempty"`
}

// Rejection reason codes a node may use.
var RejectionReasonCodes = []string{
	"LOCAL_VALIDATION_FAILED",
	"SCHEMA_UNSUPPORTED",
	"RELOAD_FAILED",
	"HEALTH_CHECK_FAILED",
	"DEPENDENCY_MISSING",
	"OPERATOR_CANCELLED",
	"OTHER",
}

// ReportApplied tells Lymph this installation is now running the revision. It
// is the only thing that moves the observed and last_good refs
// (mission sections 31, 36, 54).
func (c *Client) ReportApplied(ctx context.Context, revisionID string, report AppliedReport) (ApplicationResult, error) {
	var out ApplicationResult
	if err := c.report(ctx, revisionID, "applied", report, &out); err != nil {
		return ApplicationResult{}, err
	}
	return out, nil
}

// ReportRejected tells Lymph this installation will not run the revision. It
// does not rewrite governance: it is evidence that becomes feedback
// (mission sections 32, 37, 38).
func (c *Client) ReportRejected(ctx context.Context, revisionID string, report RejectedReport) (ApplicationResult, error) {
	valid := false
	for _, code := range RejectionReasonCodes {
		if code == report.ReasonCode {
			valid = true
		}
	}
	if !valid {
		return ApplicationResult{}, fmt.Errorf("reason_code %q is not one of %v", report.ReasonCode, RejectionReasonCodes)
	}
	var out ApplicationResult
	if err := c.report(ctx, revisionID, "rejected", report, &out); err != nil {
		return ApplicationResult{}, err
	}
	return out, nil
}

func (c *Client) report(ctx context.Context, revisionID, outcome string, report any, out *ApplicationResult) error {
	if err := c.ensureSession(ctx); err != nil {
		return err
	}
	session, ok := c.Session()
	if !ok {
		return &UnavailableError{Err: errors.New("no session: cannot report an application result")}
	}
	path := "/v1/updates/" + revisionID + "/" + outcome
	return c.postWithHeaders(ctx, path, report,
		map[string]string{protocol.HeaderSession: session.SessionID}, out)
}

// getWithSession performs a GET carrying a session header.
func (c *Client) getWithSession(ctx context.Context, path, sessionID string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return err
	}
	if sessionID != "" {
		req.Header.Set(protocol.HeaderSession, sessionID)
	}
	return c.do(req, out)
}
