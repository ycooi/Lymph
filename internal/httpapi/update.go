package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
)

// Approved update delivery over HTTP (mission sections 25 to 32, 58 to 60).
//
// The application-facing endpoints take their identity from the session, never
// from the body: an application cannot name another application's installation
// and fetch its update. The operator-facing read endpoints take their privilege
// from the resolved local principal.

// sessionIdentity resolves the session named by X-Lymph-Session and returns the
// application and installation the daemon already believes it is. Missing or
// unknown sessions are refused rather than defaulted (mission section 61).
func (s *Server) sessionIdentity(w http.ResponseWriter, r *http.Request) (sessionApp, sessionInstallation string, ok bool) {
	sessionID := r.Header.Get(protocol.HeaderSession)
	if sessionID == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"error":       "this endpoint requires an application session",
			"reason_code": "SESSION_REQUIRED",
		})
		return "", "", false
	}
	session, err := s.e.SessionByID(r.Context(), sessionID)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"error":       err.Error(),
			"reason_code": "SESSION_UNKNOWN",
		})
		return "", "", false
	}
	return session.ApplicationID, session.InstallationID, true
}

// handleUpdates is GET /v1/updates: what may this installation take?
func (s *Server) handleUpdates(w http.ResponseWriter, r *http.Request) {
	principal, allowed := s.requireRole(w, r, RoleApplication, "reading approved updates")
	if !allowed {
		return
	}
	applicationID, installationID, ok := s.sessionIdentity(w, r)
	if !ok {
		return
	}
	// A policy may pin an application principal to named applications; that is
	// a second gate on top of session identity.
	if !principal.MayActFor(applicationID) {
		writeForbidden(w, &ForbiddenError{Needed: RoleApplication, PeerUID: principal.UID,
			Reason: "peer is not authorised for application " + applicationID})
		return
	}

	family := r.URL.Query().Get("config_family")
	if family != "" {
		// An application naturally has a family name in hand; accept it, and
		// resolve it against the session's own application so one application
		// can never name another's family.
		resolved, err := s.e.ResolveFamilyID(r.Context(), applicationID, family)
		if err != nil {
			writeError(w, err)
			return
		}
		family = resolved
	}
	updates, err := s.e.Updates(r.Context(), applicationID, installationID, family)
	if err != nil {
		writeError(w, err)
		return
	}
	if updates == nil {
		updates = []projection.ApprovedUpdate{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"application_id":  applicationID,
		"installation_id": installationID,
		"updates":         updates,
	})
}

// handleUpdateContent is GET /v1/updates/{revision_id}/content. Eligibility is
// re-checked here: a successful list is not an authorisation for a later fetch
// (mission section 27).
func (s *Server) handleUpdateContent(w http.ResponseWriter, r *http.Request) {
	principal, allowed := s.requireRole(w, r, RoleApplication, "fetching an approved update")
	if !allowed {
		return
	}
	applicationID, installationID, ok := s.sessionIdentity(w, r)
	if !ok {
		return
	}
	if !principal.MayActFor(applicationID) {
		writeForbidden(w, &ForbiddenError{Needed: RoleApplication, PeerUID: principal.UID,
			Reason: "peer is not authorised for application " + applicationID})
		return
	}

	family := r.URL.Query().Get("config_family")
	if family == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "config_family is required"})
		return
	}
	if resolved, err := s.e.ResolveFamilyID(r.Context(), applicationID, family); err == nil {
		family = resolved
	} else {
		writeError(w, err)
		return
	}
	observed := s.observedRevision(r, applicationID, family, installationID)

	fetched, err := s.e.FetchUpdate(r.Context(), applicationID, installationID, family,
		r.PathValue("revision_id"), observed)
	if err != nil {
		writeError(w, err)
		return
	}

	// The bundle is returned as path -> base64 bytes via encoding/json. The
	// manifest is the authoritative list of paths and hashes, and the tree hash
	// is what the node must reproduce to prove it received the approved content
	// (mission sections 28, 29).
	writeJSON(w, http.StatusOK, map[string]any{
		"update":         fetched.Update,
		"root_tree_hash": fetched.TreeHash,
		"manifest":       fetched.Manifest,
		"bundle":         fetched.Bundle,
	})
}

// observedRevision reads the installation's node-confirmed revision, which is
// what base compatibility is judged against.
func (s *Server) observedRevision(r *http.Request, applicationID, configFamilyID, installationID string) string {
	ref, err := s.e.DB().GetRef(applicationID, configFamilyID, installationID, projection.RefObserved)
	if err != nil {
		return ""
	}
	return ref.RevisionID
}

// handleReportApplied is POST /v1/updates/{revision_id}/applied.
func (s *Server) handleReportApplied(w http.ResponseWriter, r *http.Request) {
	principal, allowed := s.requireRole(w, r, RoleApplication, "reporting an applied update")
	if !allowed {
		return
	}
	applicationID, installationID, ok := s.sessionIdentity(w, r)
	if !ok {
		return
	}

	var body struct {
		ConfigFamilyID     string          `json:"config_family_id"`
		ObservedHash       string          `json:"observed_hash,omitempty"`
		PreviousRevisionID string          `json:"previous_revision_id,omitempty"`
		Details            json.RawMessage `json:"details,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, fmt.Errorf("decode request: %w", err))
		return
	}

	session, err := s.e.SessionByID(r.Context(), r.Header.Get(protocol.HeaderSession))
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": err.Error(), "reason_code": "SESSION_UNKNOWN"})
		return
	}

	result, err := s.e.ReportApplied(r.Context(), engine.ReportAppliedRequest{
		ApplicationID:      applicationID,
		InstallationID:     installationID,
		ConfigFamilyID:     body.ConfigFamilyID,
		RevisionID:         r.PathValue("revision_id"),
		ObservedHash:       body.ObservedHash,
		PreviousRevisionID: body.PreviousRevisionID,
		Details:            body.Details,
		SessionID:          session.SessionID,
		ProcessID:          session.ProcessID,
		PeerUID:            principal.UID,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleReportRejected is POST /v1/updates/{revision_id}/rejected.
func (s *Server) handleReportRejected(w http.ResponseWriter, r *http.Request) {
	principal, allowed := s.requireRole(w, r, RoleApplication, "reporting a rejected update")
	if !allowed {
		return
	}
	applicationID, installationID, ok := s.sessionIdentity(w, r)
	if !ok {
		return
	}

	var body struct {
		ConfigFamilyID string          `json:"config_family_id"`
		ReasonCode     string          `json:"reason_code"`
		Message        string          `json:"message,omitempty"`
		Details        json.RawMessage `json:"details,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, fmt.Errorf("decode request: %w", err))
		return
	}

	session, err := s.e.SessionByID(r.Context(), r.Header.Get(protocol.HeaderSession))
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": err.Error(), "reason_code": "SESSION_UNKNOWN"})
		return
	}

	result, err := s.e.ReportRejected(r.Context(), engine.ReportRejectedRequest{
		ApplicationID:  applicationID,
		InstallationID: installationID,
		ConfigFamilyID: body.ConfigFamilyID,
		RevisionID:     r.PathValue("revision_id"),
		ReasonCode:     body.ReasonCode,
		Message:        body.Message,
		Details:        body.Details,
		SessionID:      session.SessionID,
		ProcessID:      session.ProcessID,
		PeerUID:        principal.UID,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleWithdrawUpdate is POST /v1/updates/{revision_id}/withdraw. Withdrawal is
// an operator action: it stops future delivery without erasing the approval
// (mission section 41).
func (s *Server) handleWithdrawUpdate(w http.ResponseWriter, r *http.Request) {
	principal, allowed := s.requireRole(w, r, RoleOperator, "withdrawing an approved update")
	if !allowed {
		return
	}
	applicationID, installationID, ok := s.sessionIdentityAllowingOperator(w, r)
	if !ok {
		return
	}

	var body struct {
		ConfigFamilyID string `json:"config_family_id"`
		Reason         string `json:"reason,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, fmt.Errorf("decode request: %w", err))
		return
	}
	if body.ConfigFamilyID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "config_family_id is required"})
		return
	}

	disposition, err := s.e.WithdrawUpdate(r.Context(), applicationID, installationID,
		body.ConfigFamilyID, r.PathValue("revision_id"), operatorName(principal), body.Reason)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, disposition)
}

// sessionIdentityAllowingOperator resolves the installation a withdrawal
// applies to. An operator may name it explicitly, because an operator is not an
// application and has no session of its own.
func (s *Server) sessionIdentityAllowingOperator(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	if r.Header.Get(protocol.HeaderSession) != "" {
		return s.sessionIdentity(w, r)
	}
	applicationID := r.URL.Query().Get("application_id")
	installationID := r.URL.Query().Get("installation_id")
	if applicationID == "" || installationID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "withdrawal needs an application session or explicit application_id and installation_id",
		})
		return "", "", false
	}
	return applicationID, installationID, true
}

// handleApplicationResults is GET /v1/application-results: the canonical record
// of what nodes reported doing.
func (s *Server) handleApplicationResults(w http.ResponseWriter, r *http.Request) {
	_, allowed := s.requireRole(w, r, RoleOperator, "reading application results")
	if !allowed {
		return
	}
	results, err := s.e.ApplicationResults(r.URL.Query().Get("application"),
		r.URL.Query().Get("installation"), queryInt(r, "limit", 50))
	if err != nil {
		writeError(w, err)
		return
	}
	if results == nil {
		results = []projection.ApplicationResult{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"application_results": results})
}

// handleUpdateDispositions is GET /v1/update-dispositions: per-installation
// delivery state, including withdrawals and rejections.
func (s *Server) handleUpdateDispositions(w http.ResponseWriter, r *http.Request) {
	_, allowed := s.requireRole(w, r, RoleOperator, "reading update dispositions")
	if !allowed {
		return
	}
	dispositions, err := s.e.UpdateDispositions(r.URL.Query().Get("application"),
		r.URL.Query().Get("installation"), r.URL.Query().Get("state"), queryInt(r, "limit", 100))
	if err != nil {
		writeError(w, err)
		return
	}
	if dispositions == nil {
		dispositions = []projection.UpdateDisposition{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"update_dispositions": dispositions})
}

// operatorName is the human-readable actor recorded on an operator action.
func operatorName(principal Principal) string {
	if principal.UID >= 0 {
		return fmt.Sprintf("uid:%d", principal.UID)
	}
	return "operator"
}
