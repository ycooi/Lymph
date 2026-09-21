package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/ledger"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
)

// The handshake.
//
// Registration is persistent identity: this application, this installation.
// A session is transient connectivity: this process, right now. Keeping them
// apart is what lets a process restart without touching configuration history,
// and lets a daemon restart without losing anything that matters (mission
// sections 3, 116).
//
// Sessions are operational state. Successful handshakes are not written to the
// canonical ledger — a heartbeat flood would drown it — but abnormal outcomes
// are, because they explain a deployment that never worked (mission sections 8,
// 105).

// SessionConfig tunes handshake policy.
type SessionConfig struct {
	// IdleTimeout is how long a session may go without activity before a sweep
	// marks it STALE. No heartbeat is required: normal event traffic refreshes a
	// session (mission section 36).
	IdleTimeout time.Duration
	// Retention is how long closed or stale sessions are kept for diagnostics
	// (mission section 104).
	Retention time.Duration
	// StrictDuplicatePolicy turns the duplicate-process warning into a refusal.
	// Off by default: Lymph must not prevent an application from starting
	// (mission section 19).
	StrictDuplicatePolicy bool
}

// DefaultSessionConfig is the policy this build ships with.
func DefaultSessionConfig() SessionConfig {
	return SessionConfig{
		IdleTimeout:           15 * time.Minute,
		Retention:             7 * 24 * time.Hour,
		StrictDuplicatePolicy: false,
	}
}

// HelloRequest is one client handshake (mission section 10).
type HelloRequest struct {
	ProtocolVersion string `json:"protocol_version"`

	ApplicationID  string `json:"application_id"`
	InstallationID string `json:"installation_id"`
	ProcessID      string `json:"process_id"`

	ClientName    string `json:"client_name,omitempty"`
	ClientVersion string `json:"client_version,omitempty"`
	ManifestHash  string `json:"manifest_hash,omitempty"`

	PID      int    `json:"pid,omitempty"`
	Hostname string `json:"hostname,omitempty"`
}

// HelloResponse is the daemon's answer.
type HelloResponse struct {
	Accepted bool `json:"accepted"`

	ProtocolVersion string `json:"protocol_version"`
	ServerVersion   string `json:"server_version"`

	LymphInstanceID string `json:"lymph_instance_id"`
	SessionID       string `json:"session_id,omitempty"`

	ApplicationID  string `json:"application_id"`
	InstallationID string `json:"installation_id"`
	ProcessID      string `json:"process_id"`

	Registered   bool     `json:"registered"`
	Capabilities []string `json:"capabilities"`

	// ReasonCode and Message are set when Accepted is false, and for accepted
	// handshakes that deserve operator attention.
	ReasonCode string `json:"reason_code,omitempty"`
	Message    string `json:"message,omitempty"`

	SupportedVersions []string `json:"supported_versions,omitempty"`

	// Warnings are the "accepted, but look at this" cases: duplicate processes
	// on a single-process installation, manifest drift, a different daemon
	// instance than last time.
	Warnings []string `json:"warnings,omitempty"`

	// Session is the operational record, echoed for diagnostics.
	Session projection.Session `json:"session"`
}

// ErrHandshakeRejected wraps every refusal with its machine-readable reason.
type ErrHandshakeRejected struct {
	ReasonCode string
	Message    string
}

func (e *ErrHandshakeRejected) Error() string {
	return fmt.Sprintf("%s: %s", e.ReasonCode, e.Message)
}

// ReasonCodeOf extracts the machine-readable reason from a handshake error.
func ReasonCodeOf(err error) string {
	var rejected *ErrHandshakeRejected
	if errors.As(err, &rejected) {
		return rejected.ReasonCode
	}
	return ""
}

// PeerInfo is what the transport knows about the caller but the client did not
// have to say (mission section 39). Zero values mean "not available".
type PeerInfo struct {
	UID int
	GID int
	// Supported reports whether the platform could supply credentials at all.
	Supported bool
}

// Hello performs the handshake described in mission section 12, in order.
func (e *Engine) Hello(ctx context.Context, req HelloRequest, peer PeerInfo) (HelloResponse, error) {
	response := HelloResponse{
		ProtocolVersion: protocol.Version,
		ServerVersion:   e.serverVersion,
		LymphInstanceID: e.ident.InstanceUUID,
		ApplicationID:   req.ApplicationID,
		InstallationID:  req.InstallationID,
		ProcessID:       req.ProcessID,
		Capabilities:    protocol.Capabilities(),
	}

	reject := func(code protocol.ReasonCode, detail string) (HelloResponse, error) {
		response.Accepted = false
		response.ReasonCode = string(code)
		response.Message = detail
		if code == protocol.ReasonProtocolIncompatible {
			response.SupportedVersions = protocol.SupportedVersions()
		}
		return response, &ErrHandshakeRejected{ReasonCode: string(code), Message: detail}
	}

	// 1 and 2: fields present, protocol understood.
	if req.ProtocolVersion == "" {
		// These two checks run before the transaction opens, so the audit can
		// take its own.
		response, err := reject(protocol.ReasonMalformed, "protocol_version is required")
		e.auditHandshake(ctx, protocol.ReasonMalformed, req, response.Message)
		return response, err
	}
	if !protocol.Compatible(req.ProtocolVersion) {
		detail := fmt.Sprintf("daemon speaks %s, client asked for %q",
			strings.Join(protocol.SupportedVersions(), ", "), req.ProtocolVersion)
		response, err := reject(protocol.ReasonProtocolIncompatible, detail)
		e.auditHandshake(ctx, protocol.ReasonProtocolIncompatible, req, detail)
		return response, err
	}

	// 3: identities are UUIDs, not names.
	for _, field := range []struct{ name, value string }{
		{"application_id", req.ApplicationID},
		{"installation_id", req.InstallationID},
		{"process_id", req.ProcessID},
	} {
		if field.value == "" {
			response, err := reject(protocol.ReasonInvalidIdentity, field.name+" is required")
			e.auditHandshake(ctx, protocol.ReasonInvalidIdentity, req, response.Message)
			return response, err
		}
		if !identity.Valid(field.value) {
			detail := fmt.Sprintf("%s %q is not a UUID", field.name, field.value)
			response, err := reject(protocol.ReasonInvalidIdentity, detail)
			e.auditHandshake(ctx, protocol.ReasonInvalidIdentity, req, detail)
			return response, err
		}
	}

	now := time.Now().UTC()
	var response_ HelloResponse
	err := e.withTx(func(tx *sql.Tx) error {
		// 4 and 5: the application must be registered.
		app, err := projection.GetApplicationTx(tx, req.ApplicationID)
		if err != nil {
			if errors.Is(err, projection.ErrNotFound) {
				rejected, rejectErr := reject(protocol.ReasonUnknownApplication,
					"application "+req.ApplicationID+" is not registered; registration is an explicit control-plane operation")
				e.auditTx(tx, handshakeAuditEvent(protocol.ReasonUnknownApplication, req, rejected.Message))
				return rejectErr
			}
			return err
		}

		// 6 and 7: the installation must exist and belong to this application.
		installation, err := projection.GetInstallationTx(tx, req.ApplicationID, req.InstallationID)
		if err != nil {
			if errors.Is(err, projection.ErrNotFound) {
				// Distinguish "belongs to someone else" from "does not exist":
				// the first is a configuration mistake worth naming precisely.
				if owner, _ := findInstallationOwnerTx(tx, req.InstallationID); owner != "" && owner != req.ApplicationID {
					rejected, rejectErr := reject(protocol.ReasonInstallationMismatch,
						fmt.Sprintf("installation %s belongs to application %s", req.InstallationID, owner))
					e.auditTx(tx, handshakeAuditEvent(protocol.ReasonInstallationMismatch, req, rejected.Message))
					return rejectErr
				}
				rejected, rejectErr := reject(protocol.ReasonUnknownInstallation,
					fmt.Sprintf("installation %s is not registered for application %s", req.InstallationID, req.ApplicationID))
				e.auditTx(tx, handshakeAuditEvent(protocol.ReasonUnknownInstallation, req, rejected.Message))
				return rejectErr
			}
			return err
		}

		// 9: a manifest hash difference is a warning, never a refusal. An
		// application may legitimately have upgraded before re-registering
		// (mission section 17).
		var warnings []string
		if req.ManifestHash != "" {
			if latest, err := latestRegistrationHashTx(tx, req.ApplicationID); err == nil && latest != "" && latest != req.ManifestHash {
				warnings = append(warnings, "MANIFEST_DRIFT: client manifest hash differs from the registered manifest")
				e.auditTx(tx, projection.AuditEvent{
					Action:        protocol.AuditManifestDrift,
					Actor:         req.ClientName,
					ApplicationID: req.ApplicationID,
					SubjectKind:   "installation",
					SubjectID:     req.InstallationID,
					Detail: fmt.Sprintf(`{"client_hash":%q,"registered_hash":%q,"process_id":%q}`,
						req.ManifestHash, latest, req.ProcessID),
				})
			}
		}

		// 10 and 11: duplicate-process policy.
		active, err := projection.ActiveSessionsForInstallationTx(tx, req.ApplicationID, req.InstallationID)
		if err != nil {
			return err
		}
		var others []projection.Session
		var sameProcess []projection.Session
		for _, session := range active {
			if session.ProcessID == req.ProcessID {
				sameProcess = append(sameProcess, session)
				continue
			}
			others = append(others, session)
		}

		// A reconnecting process replaces its own previous session rather than
		// accumulating one per interruption (mission section 59).
		for _, session := range sameProcess {
			if err := projection.CloseSessionTx(tx, session.SessionID, now); err != nil {
				return err
			}
		}
		if len(sameProcess) > 0 {
			// The same process came back: it lost its session to a restart, an
			// expiry or a connection failure. Worth counting (mission section 65).
			e.sessionReconnected()
		}

		if len(others) > 0 {
			single := installation.SessionPolicy == projection.SessionPolicySingle
			switch {
			case single && e.sessions.StrictDuplicatePolicy:
				rejected, rejectErr := reject(protocol.ReasonInstallationMismatch,
					"installation expects a single process; another is already connected")
				e.auditTx(tx, handshakeAuditEvent(protocol.ReasonInstallationMismatch, req, rejected.Message))
				return rejectErr
			case single:
				warnings = append(warnings, fmt.Sprintf(
					"DUPLICATE_INSTALLATION_SESSION: %d other process(es) connected to an installation that expects one",
					len(others)))
			default:
				// Multi-process is normal: several workers, gunicorn children,
				// MCP replicas. Record it, do not block it.
				warnings = append(warnings, fmt.Sprintf(
					"MULTIPLE_PROCESSES: %d other process(es) are connected to this installation",
					len(others)))
			}
			e.auditTx(tx, projection.AuditEvent{
				Action:        protocol.AuditDuplicateSession,
				Actor:         req.ClientName,
				ApplicationID: req.ApplicationID,
				SubjectKind:   "installation",
				SubjectID:     req.InstallationID,
				Detail: fmt.Sprintf(`{"policy":%q,"other_processes":%d,"process_id":%q}`,
					installation.SessionPolicy, len(others), req.ProcessID),
			})
		}

		// 12 and 13: mint the session and persist it as operational state.
		if prior, err := projection.CountSessionsForProcessTx(tx, req.ApplicationID, req.InstallationID, req.ProcessID); err != nil {
			return err
		} else if prior > 0 {
			// This process has handshaked before: it is reconnecting, which is
			// exactly what happens after a daemon restart (mission sections 60, 65).
			e.sessionReconnected()
		}
		session := projection.Session{
			SessionID:       identity.NewID(),
			ApplicationID:   req.ApplicationID,
			InstallationID:  req.InstallationID,
			ProcessID:       req.ProcessID,
			ProtocolVersion: req.ProtocolVersion,
			ClientName:      req.ClientName,
			ClientVersion:   req.ClientVersion,
			ManifestHash:    req.ManifestHash,
			PID:             req.PID,
			Hostname:        req.Hostname,
			PeerUID:         peerOrUnknown(peer.UID, peer.Supported),
			PeerGID:         peerOrUnknown(peer.GID, peer.Supported),
			State:           projection.SessionActive,
			ConnectedAt:     now,
			LastSeenAt:      now,
		}
		if err := projection.InsertSession(tx, session); err != nil {
			return err
		}
		e.sessionsOpened()

		response_ = response
		response_.Accepted = true
		response_.SessionID = session.SessionID
		response_.Registered = true
		response_.Warnings = warnings
		response_.Session = session
		_ = app
		return nil
	})
	if err != nil {
		return response, err
	}

	e.log.Info("session established",
		"session", response_.SessionID, "application", req.ApplicationID,
		"installation", req.InstallationID, "process", req.ProcessID,
		"client", req.ClientName, "protocol", req.ProtocolVersion,
		"warnings", len(response_.Warnings))
	return response_, nil
}

// peerOrUnknown renders a peer credential, using -1 for "not available", which
// is also what the schema stores.
func peerOrUnknown(value int, supported bool) int {
	if !supported {
		return -1
	}
	return value
}

// findInstallationOwnerTx reports which application owns an installation
// identity, so a mismatch can be named precisely.
func findInstallationOwnerTx(tx *sql.Tx, installationID string) (string, error) {
	var applicationID string
	err := tx.QueryRow(`SELECT application_id FROM installations WHERE installation_id = ? LIMIT 1`,
		installationID).Scan(&applicationID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return applicationID, nil
}

// auditHandshake records a refused handshake canonically.
func (e *Engine) auditHandshake(ctx context.Context, code protocol.ReasonCode, req HelloRequest, detail string) {
	if err := e.audit(ctx, handshakeAuditEvent(code, req, detail)); err != nil {
		e.log.Warn("could not audit rejected handshake", "error", err, "reason", code)
	}
}

// handshakeAuditEvent builds the canonical record for a refused handshake.
func handshakeAuditEvent(code protocol.ReasonCode, req HelloRequest, detail string) projection.AuditEvent {
	action := protocol.AuditHandshakeRejected
	switch code {
	case protocol.ReasonProtocolIncompatible:
		action = protocol.AuditProtocolIncompatible
	case protocol.ReasonInstallationMismatch:
		action = protocol.AuditIdentityMismatch
	}
	return projection.AuditEvent{
		Action:        action,
		Actor:         req.ClientName,
		ApplicationID: req.ApplicationID,
		SubjectKind:   "handshake",
		SubjectID:     req.ProcessID,
		Detail: fmt.Sprintf(`{"reason_code":%q,"installation_id":%q,"protocol_version":%q,"detail":%q}`,
			code, req.InstallationID, req.ProtocolVersion, detail),
	}
}

// auditTx is the transaction-scoped audit helper used inside Hello.
func (e *Engine) auditTx(tx *sql.Tx, event projection.AuditEvent) {
	if event.AuditID == "" {
		event.AuditID = identity.NewID()
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	if err := appendAuditTx(e, tx, event, ledger.Async); err != nil {
		e.log.Warn("could not write audit event", "action", event.Action, "error", err)
	}
}

// SessionLookup resolves a session for event validation (mission sections 20-21,
// 61-63). It returns the session and whether it is usable; an unknown or
// finished session is reported as ErrSessionUnknown so the client can
// re-handshake once.
func (e *Engine) SessionLookup(tx *sql.Tx, sessionID string, at time.Time) (projection.Session, error) {
	session, err := projection.GetSessionTx(tx, sessionID)
	if err != nil {
		return session, err
	}
	if session.State != projection.SessionActive {
		return session, fmt.Errorf("%w: session %s is %s", projection.ErrNotFound, sessionID, session.State)
	}
	if err := projection.TouchSessionTx(tx, sessionID, at); err != nil {
		return session, err
	}
	session.LastSeenAt = at
	return session, nil
}

// Sessions lists sessions for the query API.
func (e *Engine) Sessions(applicationID, installationID, state string, limit int) ([]projection.Session, error) {
	return e.db.ListSessions(applicationID, installationID, state, limit)
}

// SessionByID resolves a live session for a read-only endpoint such as the
// approved-update list. It is the read path's identity: an application asks
// what *it* may take, and the daemon answers from the session rather than from
// anything the caller wrote in the request (mission sections 25, 27, 61).
func (e *Engine) SessionByID(ctx context.Context, sessionID string) (projection.Session, error) {
	var out projection.Session
	if sessionID == "" {
		return out, fmt.Errorf("%w: no session supplied", ErrSessionUnknown)
	}
	err := e.withTx(func(tx *sql.Tx) error {
		session, err := e.SessionLookup(tx, sessionID, time.Now().UTC())
		if err != nil {
			return err
		}
		out = session
		return nil
	})
	return out, err
}

// ErrSessionUnknown is returned when an event names a session this daemon does
// not know: it expired, was closed, or the daemon restarted. Clients catch this
// specifically, handshake once and retry — they do not spool, because the
// daemon is alive and merely forgot (mission section 61).
var ErrSessionUnknown = errors.New("SESSION_UNKNOWN")

// ErrSessionIdentityMismatch is returned when a session is valid but the event
// disagrees with it about who is talking. That is a configuration or
// programming fault, not a race, so it is never retried (mission section 62).
var ErrSessionIdentityMismatch = errors.New("SESSION_IDENTITY_MISMATCH")

// sessionAcceptsEvent applies the identity rules of mission sections 21, 62 and
// 63, and fills in what the session knows so the client does not have to repeat
// it.
//
// It mutates the request: a client that presents a session does not need to
// restate its installation or process, and the session's values win.
func sessionAcceptsEvent(session projection.Session, req *EmitRequest, applicationID string) error {
	if session.ApplicationID != applicationID {
		return fmt.Errorf("%w: session belongs to application %s, event claims %s",
			ErrSessionIdentityMismatch, session.ApplicationID, applicationID)
	}

	switch {
	case req.InstallationID == "":
		// The session knows which deployment this process belongs to.
		req.InstallationID = session.InstallationID
	case req.InstallationID != session.InstallationID:
		return fmt.Errorf("%w: session installation is %s, event claims %s",
			ErrSessionIdentityMismatch, session.InstallationID, req.InstallationID)
	}

	if req.Replay {
		// A replayed event keeps the process identity that produced it. That is
		// historical truth: the spool may be older than the running process.
		return nil
	}
	switch {
	case req.ProducerInstance == "":
		req.ProducerInstance = session.ProcessID
	case req.ProducerInstance != session.ProcessID:
		return fmt.Errorf("%w: session process is %s, event claims %s",
			ErrSessionIdentityMismatch, session.ProcessID, req.ProducerInstance)
	}
	return nil
}

// CloseSession ends a session deliberately. It is best-effort: processes crash,
// and expiry is what actually cleans up (mission section 38).
func (e *Engine) CloseSession(sessionID string) (projection.Session, error) {
	now := time.Now().UTC()
	var out projection.Session
	err := e.withTx(func(tx *sql.Tx) error {
		if err := projection.CloseSessionTx(tx, sessionID, now); err != nil {
			return err
		}
		session, err := projection.GetSessionTx(tx, sessionID)
		if err != nil {
			return err
		}
		out = session
		return nil
	})
	if err != nil {
		return projection.Session{}, err
	}
	return out, nil
}

// SweepSessions marks idle sessions STALE and deletes sessions past retention
// (mission sections 36, 104). It runs at startup and on demand.
func (e *Engine) SweepSessions(ctx context.Context) (stale int, deleted int64, err error) {
	now := time.Now().UTC()
	err = e.withTx(func(tx *sql.Tx) error {
		expired, err := projection.ExpireIdleSessions(tx, now.Add(-e.sessions.IdleTimeout), now)
		if err != nil {
			return err
		}
		stale = len(expired)
		if e.sessions.Retention > 0 {
			removed, err := projection.DeleteOldSessions(tx, now.Add(-e.sessions.Retention))
			if err != nil {
				return err
			}
			deleted = removed
		}
		return nil
	})
	return stale, deleted, err
}
