// Package httpapi exposes lymphd over HTTP+JSON on a Unix domain socket
// (section 60, section 63).
//
// A Unix socket was chosen over a network listener deliberately: it is local
// only, it inherits filesystem permissions, it needs no TLS, and it cannot
// collide with a port or expose a network surface (section 61). The transport
// is ordinary HTTP with ordinary JSON so that curl, Python and Go are all
// first-class clients.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/ledger"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
)

// DefaultSocketPath is the production socket location.
const DefaultSocketPath = "/run/lymph/lymph.sock"

// MaxRequestBodyBytes bounds every API request before JSON decoding. Config
// bundles are small governance artifacts; accepting an unbounded local body
// would let one socket client exhaust the daemon's memory.
const MaxRequestBodyBytes int64 = 16 << 20

// Server is the lymphd HTTP surface.
type Server struct {
	e       *engine.Engine
	log     *slog.Logger
	mux     *http.ServeMux
	handler http.Handler
	socket  string
	mode    os.FileMode
	acl     *ACL

	// mu guards the lifecycle fields: a signal handler may call Shutdown while
	// another goroutine is still starting the listener.
	mu       sync.Mutex
	listener net.Listener
	http     *http.Server
}

// Options configures the server.
type Options struct {
	Engine     *engine.Engine
	SocketPath string
	Logger     *slog.Logger
	// SocketMode is the permission bits applied to the socket. Zero means 0660.
	// Lymph never widens this to world-accessible: the socket is the local trust
	// boundary (mission sections 40, 79).
	SocketMode os.FileMode
	// TCPAddr, when set, additionally listens on a TCP address. It is empty by
	// default and must be requested explicitly (section 61).
	TCPAddr string
	// ACL is the local authorization policy. When nil the daemon runs in
	// single-principal local trust mode: the socket permission bits are the
	// boundary and every local peer holds every role (mission sections 22, 23).
	ACL *ACL
}

// New builds the server and its routes.
func New(opts Options) *Server {
	if opts.SocketPath == "" {
		opts.SocketPath = DefaultSocketPath
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	s := &Server{e: opts.Engine, log: log, socket: opts.SocketPath, mux: http.NewServeMux(), acl: opts.ACL}
	s.mode = opts.SocketMode
	if s.mode == 0 {
		s.mode = 0o660
	}
	s.routes()
	s.handler = http.MaxBytesHandler(s.mux, MaxRequestBodyBytes)
	return s
}

// SocketPath returns the configured socket path.
func (s *Server) SocketPath() string { return s.socket }

// Handler exposes the router (used by tests over httptest).
func (s *Server) Handler() http.Handler { return s.handler }

// Start opens the Unix socket and prepares the HTTP server.
//
// It returns once the socket is accepting connections, so a caller may install
// a signal handler and shut down immediately afterwards without racing the
// listener setup.
func (s *Server) Start() error {
	if err := os.MkdirAll(filepath.Dir(s.socket), 0o755); err != nil {
		return err
	}
	if err := removeStaleSocket(s.socket); err != nil {
		return err
	}
	ln, err := net.Listen("unix", s.socket)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.socket, err)
	}
	// Socket permissions are Lymph's access control on a local socket
	// (section 61): owner and group may talk to the daemon, nobody else.
	if err := os.Chmod(s.socket, s.mode); err != nil {
		s.log.Warn("could not set socket permissions", "socket", s.socket, "error", err)
	}

	s.mu.Lock()
	s.listener = ln
	s.http = &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
		// The connection is kept on the request context so a handler can read
		// SO_PEERCRED. It is metadata for operators, never identity.
		ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
			return context.WithValue(ctx, connContextKey{}, conn)
		},
	}
	s.mu.Unlock()

	s.log.Info("lymphd listening", "socket", s.socket)
	return nil
}

// Serve accepts connections until Shutdown is called.
func (s *Server) Serve() error {
	s.mu.Lock()
	httpServer, listener := s.http, s.listener
	s.mu.Unlock()
	if httpServer == nil || listener == nil {
		return errors.New("Serve called before Start")
	}
	err := httpServer.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Listen is Start followed by Serve.
func (s *Server) Listen() error {
	if err := s.Start(); err != nil {
		return err
	}
	return s.Serve()
}

// Shutdown stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	httpServer, listener := s.http, s.listener
	s.mu.Unlock()

	var err error
	if httpServer != nil {
		err = httpServer.Shutdown(ctx)
	}
	if listener != nil {
		// Closing the listener is what stops a Serve that has not yet begun
		// accepting, and it is harmless once Shutdown has run.
		_ = listener.Close()
	}
	return err
}

// removeStaleSocket clears a socket file left behind by a crashed daemon, but
// refuses to remove a socket that a live daemon is still using.
func removeStaleSocket(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("%s exists and is not a socket; refusing to remove it", path)
	}
	conn, err := net.DialTimeout("unix", path, 2*time.Second)
	if err == nil {
		conn.Close()
		return fmt.Errorf("another lymphd already listens on %s", path)
	}
	slog.Warn("removing stale lymph socket", "socket", path)
	return os.Remove(path)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /v1/health", s.handleHealth)
	s.mux.HandleFunc("GET /v1/version", s.handleVersion)
	s.mux.Handle("GET /v1/status", s.roleHandler(RoleOperator, "reading daemon status", s.handleStatus))
	s.mux.HandleFunc("GET /v1/whoami", s.handleWhoami)
	s.mux.HandleFunc("GET /v1/feedback-types", s.handleFeedbackTypes)

	s.mux.Handle("POST /v1/hello", s.roleHandler(RoleApplication, "opening an application session", s.handleHello))
	s.mux.Handle("GET /v1/sessions", s.roleHandler(RoleOperator, "reading sessions", s.handleListSessions))
	s.mux.Handle("GET /v1/sessions/{id}", s.roleHandler(RoleOperator, "reading a session", s.handleGetSession))
	s.mux.Handle("POST /v1/sessions/{id}/close", s.roleHandler(RoleOperator, "closing a session", s.handleCloseSession))

	s.mux.Handle("POST /v1/events", s.roleHandler(RoleApplication, "emitting feedback", s.handleEmit))
	s.mux.Handle("GET /v1/events", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading feedback", s.handleListEvents))
	s.mux.Handle("GET /v1/events/{id}", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading feedback", s.handleGetEvent))

	s.mux.Handle("POST /v1/applications", s.roleHandler(RoleOperator, "registering an application", s.handleRegister))
	s.mux.Handle("GET /v1/applications", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading applications", s.handleListApplications))
	s.mux.Handle("GET /v1/junctions", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading junctions", s.handleListJunctions))
	s.mux.Handle("GET /v1/config-families", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading config families", s.handleListFamilies))
	s.mux.Handle("GET /v1/targets", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading managed targets", s.handleListTargets))

	s.mux.Handle("GET /v1/issues", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading issues", s.handleListIssues))
	s.mux.Handle("GET /v1/issues/{id}", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading an issue", s.handleGetIssue))
	s.mux.Handle("POST /v1/issues/{id}/status", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "changing issue status", s.handleIssueStatus))

	s.mux.Handle("GET /v1/work-items", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading work items", s.handleListWork))
	s.mux.Handle("POST /v1/work-items", s.roleHandler(RoleOperator, "queueing work", s.handleQueueWork))
	s.mux.Handle("POST /v1/work-items/claim", s.roleHandler(RoleWorker, "claiming work", s.handleClaim))
	s.mux.Handle("POST /v1/work-items/{id}/renew", s.roleHandler(RoleWorker, "renewing work", s.handleRenew))
	s.mux.Handle("POST /v1/work-items/{id}/complete", s.roleHandler(RoleWorker, "completing work", s.handleComplete))
	// The typed worker return. This is the path workers should use: one call,
	// one canonical record, no partial lineage (mission sections 16, 28).
	s.mux.Handle("POST /v1/work-items/{id}/return", s.roleHandler(RoleWorker, "returning improvement work", s.handleReturnImprovement))

	s.mux.Handle("GET /v1/improvement-results", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading improvement results", s.handleListImprovementResults))
	s.mux.Handle("GET /v1/improvement-results/{id}", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading an improvement result", s.handleGetImprovementResult))
	s.mux.Handle("GET /v1/improvement-results/{id}/artifacts", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading improvement artifacts", s.handleListResultArtifacts))
	s.mux.Handle("GET /v1/artifacts", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading artifacts", s.handleListArtifacts))
	s.mux.Handle("GET /v1/artifacts/{id}", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading an artifact", s.handleGetArtifact))
	s.mux.Handle("GET /v1/installations", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading installations", s.handleListInstallations))

	s.mux.Handle("POST /v1/baselines", s.roleHandler(RoleOperator, "importing a baseline", s.handleBaseline))
	s.mux.Handle("POST /v1/revisions", s.roleHandler(RoleOperator, "creating a revision", s.handleCreateRevision))
	s.mux.Handle("GET /v1/revisions", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading revisions", s.handleListRevisions))
	s.mux.Handle("GET /v1/revisions/{id}", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading a revision", s.handleGetRevision))
	s.mux.Handle("GET /v1/revisions/{id}/lineage", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading revision lineage", s.handleLineage))
	s.mux.Handle("GET /v1/diff", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading a revision diff", s.handleDiff))

	s.mux.Handle("POST /v1/candidates", s.roleHandler(RoleWorker, "creating a candidate", s.handleCreateCandidate))
	s.mux.Handle("GET /v1/candidates", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading candidates", s.handleListCandidates))
	s.mux.Handle("GET /v1/candidates/{id}", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading a candidate", s.handleGetCandidate))
	s.mux.Handle("POST /v1/candidates/{id}/validations", s.roleHandler(RoleWorker, "recording candidate validation", s.handleValidation))
	s.mux.HandleFunc("POST /v1/candidates/{id}/approve", s.handleApprove)
	s.mux.Handle("POST /v1/candidates/{id}/promote", s.roleHandler(RoleOperator, "promoting a candidate", s.handlePromote))

	s.mux.Handle("GET /v1/refs", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading refs", s.handleListRefs))
	s.mux.Handle("POST /v1/refs", s.roleHandler(RoleOperator, "moving a ref", s.handleSetRef))
	s.mux.Handle("GET /v1/reflog", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading the reflog", s.handleListReflog))
	s.mux.Handle("GET /v1/audit", s.roleHandler(RoleOperator, "reading the audit log", s.handleListAudit))

	// Approved update delivery (mission sections 25 to 32, 58 to 60). The
	// application-facing routes take identity from the session; the operator
	// routes take privilege from the local principal.
	s.mux.HandleFunc("GET /v1/updates", s.handleUpdates)
	s.mux.HandleFunc("GET /v1/updates/{revision_id}/content", s.handleUpdateContent)
	s.mux.HandleFunc("POST /v1/updates/{revision_id}/applied", s.handleReportApplied)
	s.mux.HandleFunc("POST /v1/updates/{revision_id}/rejected", s.handleReportRejected)
	s.mux.HandleFunc("POST /v1/updates/{revision_id}/withdraw", s.handleWithdrawUpdate)
	s.mux.HandleFunc("GET /v1/application-results", s.handleApplicationResults)
	s.mux.HandleFunc("GET /v1/update-dispositions", s.handleUpdateDispositions)

	s.mux.Handle("GET /v1/objects/{hash}", s.anyRoleHandler([]Role{RoleWorker, RoleOperator}, "reading an object", s.handleGetObject))
	s.mux.Handle("GET /v1/ledger/verify", s.roleHandler(RoleOperator, "verifying the ledger", s.handleVerifyLedger))
	s.mux.Handle("POST /v1/projection/rebuild", s.roleHandler(RoleOperator, "rebuilding the projection", s.handleRebuild))
	s.mux.Handle("POST /v1/projection/ensure", s.roleHandler(RoleOperator, "repairing the projection", s.handleEnsureProjected))
}

// ---------- health and identity ----------

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "ok",
		"instance_uuid": s.e.Identity().InstanceUUID,
		"time":          time.Now().UTC(),
	})
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":           "lymphd",
		"daemon_version":    s.engineVersion(),
		"format":            "lymph-1",
		"store_format":      "lymph-1",
		"protocol_versions": protocol.SupportedVersions(),
		"protocol_version":  protocol.Version,
		"cloudevents":       protocol.SpecVersion,
		"api":               "v1",
		"instance_uuid":     s.e.Identity().InstanceUUID,
		"capabilities":      protocol.Capabilities(),
	})
}

// engineVersion reports the daemon release string, which the engine owns. It is
// separate from the protocol version on purpose (mission section 9).
func (s *Server) engineVersion() string { return s.e.ServerVersion() }

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st, err := s.e.Status(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// handleWhoami reports how the daemon sees this connection: which local uid,
// which roles, and whether an authorization policy is in force. It answers "is
// the human gate actually on?" as a query rather than an assumption (mission
// sections 22, 76). It never reads a role from the request.
func (s *Server) handleWhoami(w http.ResponseWriter, r *http.Request) {
	principal := s.principal(r)
	writeJSON(w, http.StatusOK, map[string]any{
		"principal":            principal,
		"authorization_mode":   principal.Mode,
		"policy_path":          s.acl.Path(),
		"roles":                principal.Roles,
		"may_approve":          principal.Has(RoleOperator),
		"may_fetch_updates":    principal.Has(RoleApplication),
		"may_claim_work":       principal.Has(RoleWorker),
		"client_supplied_role": false,
	})
}

func (s *Server) handleFeedbackTypes(w http.ResponseWriter, r *http.Request) {
	types := protocol.FeedbackTypes()
	out := make([]string, len(types))
	for i, t := range types {
		out[i] = string(t)
	}
	writeJSON(w, http.StatusOK, map[string]any{"feedback_types": out})
}

// ---------- events ----------

// ---------- handshake and sessions ----------

// connContextKey carries the accepted connection through to a handler so peer
// credentials can be read where the platform supports it.
type connContextKey struct{}

// handleHello performs the session handshake (mission sections 10 to 19).
func (s *Server) handleHello(w http.ResponseWriter, r *http.Request) {
	var req engine.HelloRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeHandshakeRejection(w, &engine.ErrHandshakeRejected{
			ReasonCode: string(protocol.ReasonMalformed),
			Message:    "could not decode hello request: " + err.Error(),
		})
		return
	}

	peer := engine.PeerInfo{UID: -1, GID: -1}
	if conn, ok := r.Context().Value(connContextKey{}).(net.Conn); ok {
		peer = peerCredentials(conn)
	}

	response, err := s.e.Hello(r.Context(), req, peer)
	if err != nil {
		var rejected *engine.ErrHandshakeRejected
		if errors.As(err, &rejected) {
			writeHandshakeRejection(w, rejected)
			return
		}
		writeError(w, err)
		return
	}

	// A warning is not a failure: the session exists, and the client should
	// proceed while the operator sees the note (mission sections 18, 19, 74).
	writeJSON(w, http.StatusOK, response)
}

// writeHandshakeRejection maps a reason code onto an HTTP status, and always
// returns a machine-readable body (mission section 11).
func writeHandshakeRejection(w http.ResponseWriter, rejected *engine.ErrHandshakeRejected) {
	status := http.StatusBadRequest
	switch protocol.ReasonCode(rejected.ReasonCode) {
	case protocol.ReasonProtocolIncompatible:
		status = http.StatusUpgradeRequired
	case protocol.ReasonUnknownApplication, protocol.ReasonUnknownInstallation:
		status = http.StatusNotFound
	case protocol.ReasonInstallationMismatch:
		status = http.StatusConflict
	case protocol.ReasonSessionUnknown:
		status = http.StatusUnauthorized
	case protocol.ReasonSessionIdentityMismatch:
		status = http.StatusConflict
	}
	body := map[string]any{
		"accepted":    false,
		"reason_code": rejected.ReasonCode,
		"message":     rejected.Message,
	}
	if protocol.ReasonCode(rejected.ReasonCode) == protocol.ReasonProtocolIncompatible {
		body["supported_versions"] = protocol.SupportedVersions()
		body["protocol_version"] = protocol.Version
	}
	writeJSON(w, status, body)
}

func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request) {
	sessions, err := s.e.Sessions(
		r.URL.Query().Get("application"),
		r.URL.Query().Get("installation"),
		strings.ToUpper(r.URL.Query().Get("state")),
		queryInt(r, "limit", 100))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": sessions})
}

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	session, err := s.e.DB().GetSession(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, session)
}

func (s *Server) handleCloseSession(w http.ResponseWriter, r *http.Request) {
	session, err := s.e.CloseSession(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, session)
}

// EmitRequest is the body of POST /v1/events: a CloudEvents envelope plus the
// client-side metadata that only the producer knows (section 78).
type EmitRequest struct {
	protocol.Event
	ProducerInstance string `json:"producer_instance,omitempty"`
	InstallationID   string `json:"installation_id,omitempty"`
	Durability       string `json:"durability,omitempty"`
	Fingerprint      string `json:"fingerprint,omitempty"`
}

func (s *Server) handleEmit(w http.ResponseWriter, r *http.Request) {
	var req EmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, fmt.Errorf("decode event: %w", err))
		return
	}
	// Session and replay are transport-level facts, carried in headers rather
	// than trusted from the body (mission sections 20, 33).
	sessionID := r.Header.Get(protocol.HeaderSession)
	replay := r.Header.Get(protocol.HeaderReplay) == "1" || strings.EqualFold(r.Header.Get(protocol.HeaderReplay), "true")

	resp, err := s.e.Emit(r.Context(), engine.EmitRequest{
		Event:            req.Event,
		ProducerInstance: req.ProducerInstance,
		InstallationID:   req.InstallationID,
		Durability:       ledger.Durability(req.Durability),
		Fingerprint:      req.Fingerprint,
		SessionID:        sessionID,
		Replay:           replay,
	})
	if err != nil {
		// A known session that the daemon has forgotten is a reconnect signal,
		// not a spool signal: answer 401 with the machine-readable code
		// (mission section 61).
		if errors.Is(err, engine.ErrSessionUnknown) {
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"accepted":    false,
				"reason_code": string(protocol.ReasonSessionUnknown),
				"message":     err.Error(),
			})
			return
		}
		if errors.Is(err, engine.ErrSessionIdentityMismatch) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"accepted":    false,
				"reason_code": string(protocol.ReasonSessionIdentityMismatch),
				"message":     err.Error(),
			})
			return
		}
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	events, err := s.e.Events(r.URL.Query().Get("application"), queryInt(r, "limit", 50))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (s *Server) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	event, err := s.e.DB().GetEvent(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, event)
}

// ---------- registry ----------

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var manifest engine.Manifest
	if err := json.NewDecoder(r.Body).Decode(&manifest); err != nil {
		writeError(w, fmt.Errorf("decode manifest: %w", err))
		return
	}
	resp, err := s.e.RegisterApplication(r.Context(), manifest)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleListApplications(w http.ResponseWriter, r *http.Request) {
	apps, err := s.e.Applications()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"applications": apps})
}

func (s *Server) handleListJunctions(w http.ResponseWriter, r *http.Request) {
	junctions, err := s.e.Junctions(r.URL.Query().Get("application"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"junctions": junctions})
}

func (s *Server) handleListFamilies(w http.ResponseWriter, r *http.Request) {
	families, err := s.e.ConfigFamilies(r.URL.Query().Get("application"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"config_families": families})
}

func (s *Server) handleListTargets(w http.ResponseWriter, r *http.Request) {
	targets, err := s.e.Targets(r.URL.Query().Get("config_family"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"targets": targets})
}

// ---------- issues ----------

func (s *Server) handleListIssues(w http.ResponseWriter, r *http.Request) {
	issues, err := s.e.Issues(r.URL.Query().Get("status"), queryInt(r, "limit", 50), r.URL.Query().Get("order"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"issues": issues})
}

func (s *Server) handleGetIssue(w http.ResponseWriter, r *http.Request) {
	issue, err := s.e.DB().GetIssue(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, issue)
}

func (s *Server) handleIssueStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Status  string `json:"status"`
		Actor   string `json:"actor,omitempty"`
		Comment string `json:"comment,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, fmt.Errorf("decode request: %w", err))
		return
	}
	issue, err := s.e.SetIssueStatus(r.Context(), r.PathValue("id"), engine.IssueStatus(strings.ToUpper(body.Status)), body.Actor, body.Comment)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, issue)
}

// ---------- work items ----------

func (s *Server) handleListWork(w http.ResponseWriter, r *http.Request) {
	items, err := s.e.WorkItems(r.URL.Query().Get("state"), queryInt(r, "limit", 50))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"work_items": items})
}

func (s *Server) handleQueueWork(w http.ResponseWriter, r *http.Request) {
	var req engine.QueueIssueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, fmt.Errorf("decode request: %w", err))
		return
	}
	work, err := s.e.QueueWork(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, work)
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Worker        string   `json:"worker"`
		WorkflowTypes []string `json:"workflow_types,omitempty"`
		TTLSeconds    int      `json:"ttl_seconds,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, fmt.Errorf("decode request: %w", err))
		return
	}
	work, err := s.e.ClaimWork(r.Context(), engine.ClaimRequest{
		Worker:        body.Worker,
		WorkflowTypes: body.WorkflowTypes,
		TTL:           time.Duration(body.TTLSeconds) * time.Second,
	})
	if err != nil {
		if errors.Is(err, projection.ErrNoWork) {
			writeJSON(w, http.StatusNoContent, nil)
			return
		}
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, work)
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Worker     string `json:"worker"`
		TTLSeconds int    `json:"ttl_seconds,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, fmt.Errorf("decode request: %w", err))
		return
	}
	work, err := s.e.RenewLease(r.Context(), r.PathValue("id"), body.Worker, time.Duration(body.TTLSeconds)*time.Second)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, work)
}

func (s *Server) handleComplete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Worker string          `json:"worker"`
		State  string          `json:"state"`
		Result json.RawMessage `json:"result,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, fmt.Errorf("decode request: %w", err))
		return
	}
	work, err := s.e.CompleteWork(r.Context(), engine.CompleteWorkRequest{
		WorkID: r.PathValue("id"),
		Worker: body.Worker,
		State:  engine.WorkState(strings.ToUpper(body.State)),
		Result: string(body.Result),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, work)
}

// handleReturnImprovement accepts one typed worker return.
func (s *Server) handleReturnImprovement(w http.ResponseWriter, r *http.Request) {
	var req engine.ReturnImprovementRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, fmt.Errorf("decode return request: %w", err))
		return
	}
	req.WorkID = r.PathValue("id")
	result, err := s.e.ReturnImprovement(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// ---------- improvement results, artifacts and installations ----------

func (s *Server) handleListImprovementResults(w http.ResponseWriter, r *http.Request) {
	results, err := s.e.Improvements(r.URL.Query().Get("application"), queryInt(r, "limit", 50))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"improvement_results": results})
}

func (s *Server) handleGetImprovementResult(w http.ResponseWriter, r *http.Request) {
	result, err := s.e.DB().GetImprovementResult(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	artifacts, err := s.e.DB().ListImprovementArtifacts(projection.ArtifactFilter{ResultID: result.ResultID, Limit: 1000})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"result": result, "artifacts": artifacts})
}

func (s *Server) handleListResultArtifacts(w http.ResponseWriter, r *http.Request) {
	if _, err := s.e.DB().GetImprovementResult(r.PathValue("id")); err != nil {
		writeError(w, err)
		return
	}
	artifacts, err := s.e.DB().ListImprovementArtifacts(projection.ArtifactFilter{
		ResultID: r.PathValue("id"), Limit: queryInt(r, "limit", 200),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"artifacts": artifacts})
}

func (s *Server) handleListArtifacts(w http.ResponseWriter, r *http.Request) {
	kind := strings.ToUpper(r.URL.Query().Get("kind"))
	if kind != "" && !engine.ValidArtifactKind(engine.ArtifactKind(kind)) {
		writeError(w, fmt.Errorf("unknown artifact kind %q; known kinds: %s", kind, artifactKindList()))
		return
	}
	artifacts, err := s.e.Artifacts(projection.ArtifactFilter{
		ApplicationID: r.URL.Query().Get("application"),
		Kind:          kind,
		WorkItemID:    r.URL.Query().Get("work_item"),
		Limit:         queryInt(r, "limit", 100),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"artifacts": artifacts})
}

func (s *Server) handleGetArtifact(w http.ResponseWriter, r *http.Request) {
	artifact, err := s.e.DB().GetImprovementArtifact(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, artifact)
}

func (s *Server) handleListInstallations(w http.ResponseWriter, r *http.Request) {
	installations, err := s.e.Installations(r.URL.Query().Get("application"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"installations": installations})
}

func artifactKindList() string {
	kinds := engine.ArtifactKinds()
	out := make([]string, len(kinds))
	for i, kind := range kinds {
		out[i] = string(kind)
	}
	return strings.Join(out, ", ")
}

// ---------- configuration ----------

func (s *Server) handleBaseline(w http.ResponseWriter, r *http.Request) {
	var req engine.BaselineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, fmt.Errorf("decode request: %w", err))
		return
	}
	rev, err := s.e.ImportBaseline(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rev)
}

func (s *Server) handleCreateRevision(w http.ResponseWriter, r *http.Request) {
	var req engine.RevisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, fmt.Errorf("decode request: %w", err))
		return
	}
	rev, err := s.e.CreateRevision(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rev)
}

func (s *Server) handleListRevisions(w http.ResponseWriter, r *http.Request) {
	family, err := s.familyID(r.URL.Query().Get("application"), r.URL.Query().Get("config_family"))
	if err != nil {
		writeError(w, err)
		return
	}
	revs, err := s.e.DB().ListRevisions(family, queryInt(r, "limit", 50))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revisions": revs})
}

func (s *Server) handleGetRevision(w http.ResponseWriter, r *http.Request) {
	rev, err := s.e.DB().GetRevision(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	blobs, err := s.e.DB().RevisionBlobs(rev.RevisionID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revision": rev, "blobs": blobs})
}

// handleLineage reconstructs the ancestor chain of a revision: the answer to
// "how did production get here".
func (s *Server) handleLineage(w http.ResponseWriter, r *http.Request) {
	chain, err := s.e.Lineage(r.Context(), r.PathValue("id"), queryInt(r, "limit", 100))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lineage": chain})
}

// handleDiff compares two revisions of the same config family.
func (s *Server) handleDiff(w http.ResponseWriter, r *http.Request) {
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")
	if from == "" || to == "" {
		writeError(w, errors.New("diff requires from and to revision ids"))
		return
	}
	result, err := s.e.Diff(r.Context(), from, to)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleCreateCandidate(w http.ResponseWriter, r *http.Request) {
	var req engine.CandidateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, fmt.Errorf("decode request: %w", err))
		return
	}
	cand, err := s.e.CreateCandidate(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cand)
}

func (s *Server) handleListCandidates(w http.ResponseWriter, r *http.Request) {
	var family string
	var err error
	if q := r.URL.Query().Get("config_family"); q != "" {
		family, err = s.familyID(r.URL.Query().Get("application"), q)
		if err != nil {
			writeError(w, err)
			return
		}
	}
	cands, err := s.e.DB().ListCandidates(family, r.URL.Query().Get("state"), queryInt(r, "limit", 50))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"candidates": cands})
}

// handleGetCandidate returns everything a human needs to decide about one
// candidate: the proposal, its approval history, and its validation evidence.
// It is the data behind `lymphctl review` (mission sections 43, 44).
func (s *Server) handleGetCandidate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	candidate, err := s.e.DB().GetCandidate(id)
	if err != nil {
		writeError(w, err)
		return
	}
	validations, err := s.e.DB().ListValidations(id)
	if err != nil {
		writeError(w, err)
		return
	}
	approvals, err := s.e.DB().ListApprovals(id)
	if err != nil {
		writeError(w, err)
		return
	}
	// The content address the approval would bind to is the candidate's tree,
	// not a revision number that could later be repointed (mission section 3).
	writeJSON(w, http.StatusOK, map[string]any{
		"candidate":    candidate,
		"validations":  validations,
		"approvals":    approvals,
		"content_hash": candidate.RootTreeHash,
	})
}

func (s *Server) handleValidation(w http.ResponseWriter, r *http.Request) {
	var req engine.ValidationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, fmt.Errorf("decode request: %w", err))
		return
	}
	req.CandidateID = r.PathValue("id")
	cand, err := s.e.RecordValidation(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cand)
}

func (s *Server) handleApprove(w http.ResponseWriter, r *http.Request) {
	// Approval is the one action that can put new content on the delivery
	// channel, so it is the one action gated on an operator principal. The actor
	// type is decided here, from the kernel's view of the caller, and never read
	// from the request body: a worker cannot approve by asserting that it is a
	// human (mission sections 20, 22, 42, 66).
	principal, allowed := s.requireRole(w, r, RoleOperator, "approving a candidate")
	if !allowed {
		return
	}
	var req engine.ApprovalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, fmt.Errorf("decode request: %w", err))
		return
	}
	req.CandidateID = r.PathValue("id")
	req.ActorType = projection.ActorHuman
	req.PeerUID = principal.UID
	cand, err := s.e.ApproveCandidate(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cand)
}

func (s *Server) handlePromote(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Actor string `json:"actor,omitempty"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	result, err := s.e.PromoteCandidate(r.Context(), r.PathValue("id"), body.Actor)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleListRefs(w http.ResponseWriter, r *http.Request) {
	family, err := s.resolveOptionalFamily(r.URL.Query().Get("application"), r.URL.Query().Get("config_family"))
	if err != nil {
		writeError(w, err)
		return
	}
	installations, err := s.resolveOptionalInstallation(r.URL.Query().Get("application"), r.URL.Query().Get("installation"))
	if err != nil {
		writeError(w, err)
		return
	}
	refs, err := s.e.DB().ListRefs(r.URL.Query().Get("application"), family, installations)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"refs": refs})
}

func (s *Server) handleSetRef(w http.ResponseWriter, r *http.Request) {
	var req engine.SetRefRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, fmt.Errorf("decode request: %w", err))
		return
	}
	ref, err := s.e.SetRef(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ref)
}

func (s *Server) handleListReflog(w http.ResponseWriter, r *http.Request) {
	family, err := s.resolveOptionalFamily(r.URL.Query().Get("application"), r.URL.Query().Get("config_family"))
	if err != nil {
		writeError(w, err)
		return
	}
	installation, err := s.resolveOptionalInstallation(r.URL.Query().Get("application"), r.URL.Query().Get("installation"))
	if err != nil {
		writeError(w, err)
		return
	}
	entries, err := s.e.DB().ListReflog(r.URL.Query().Get("application"), family, installation, queryInt(r, "limit", 50))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"reflog": entries})
}

func (s *Server) handleListAudit(w http.ResponseWriter, r *http.Request) {
	entries, err := s.e.DB().ListAudit(queryInt(r, "limit", 50))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"audit": entries})
}

// ---------- objects and maintenance ----------

func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request) {
	data, err := s.e.Objects().Get(r.PathValue("hash"))
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *Server) handleVerifyLedger(w http.ResponseWriter, r *http.Request) {
	report, err := s.e.VerifyLedger()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleRebuild(w http.ResponseWriter, r *http.Request) {
	applied, err := s.e.Rebuild()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"replayed_records": applied})
}

func (s *Server) handleEnsureProjected(w http.ResponseWriter, r *http.Request) {
	applied, err := s.e.EnsureProjected()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"replayed_records": applied})
}

// ---------- helpers ----------

// familyID resolves an application and family reference to a family identity.
func (s *Server) familyID(application, family string) (string, error) {
	if family == "" {
		return "", errors.New("config_family is required")
	}
	resolved, err := s.resolveOptionalFamily(application, family)
	if err != nil {
		return "", err
	}
	if resolved == "" {
		return family, nil
	}
	return resolved, nil
}

// resolveOptionalFamily turns a config family reference (identity or name) into
// an identity, returning "" when no family was requested.
//
// Names are metadata (section 4): accepting a name here is convenience for
// operators, and it always resolves to the stable UUID before any query runs.
func (s *Server) resolveOptionalFamily(application, family string) (string, error) {
	if family == "" {
		return "", nil
	}
	if application == "" {
		// A raw UUID can be used on its own; a bare name cannot, because names
		// are only unique inside one application.
		if identity.Valid(family) {
			return family, nil
		}
		return "", fmt.Errorf("resolving config family %q needs an application", family)
	}
	app, err := s.e.DB().GetApplicationByName(application)
	if err != nil {
		if app, err = s.e.DB().GetApplication(application); err != nil {
			return "", err
		}
	}
	if identity.Valid(family) {
		return family, nil
	}
	f, err := s.e.DB().GetConfigFamilyByName(app.ApplicationID, family)
	if err != nil {
		return "", err
	}
	return f.ConfigFamilyID, nil
}

// resolveOptionalInstallation turns an installation reference (identity or
// name) into an installation identity, returning "" when none was requested so
// that the query means "every installation".
func (s *Server) resolveOptionalInstallation(application, installation string) (string, error) {
	if installation == "" {
		return "", nil
	}
	if identity.Valid(installation) {
		return installation, nil
	}
	if application == "" {
		return "", fmt.Errorf("resolving installation %q by name needs an application", installation)
	}
	app, err := s.e.DB().GetApplicationByName(application)
	if err != nil {
		if app, err = s.e.DB().GetApplication(application); err != nil {
			return "", err
		}
	}
	installations, err := s.e.DB().ListInstallations(app.ApplicationID)
	if err != nil {
		return "", err
	}
	for _, candidate := range installations {
		if candidate.Name == installation {
			return candidate.InstallationID, nil
		}
	}
	return "", projection.NotFoundf("installation %q for application %s", installation, app.ApplicationID)
}

func queryInt(r *http.Request, name string, def int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(body)
}

// writeError maps engine errors onto HTTP status codes.
func writeError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, projection.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, projection.ErrStaleCandidate):
		status = http.StatusConflict
	case errors.Is(err, projection.ErrPathConflict), errors.Is(err, projection.ErrNameConflict):
		status = http.StatusConflict
	case errors.Is(err, projection.ErrNoWork):
		status = http.StatusNoContent
	}
	// Delivery refusals carry a reason code, so an application can tell "the
	// daemon is broken" apart from "this revision is not yours to take".
	var notDeliverable *engine.ErrNotDeliverable
	if errors.As(err, &notDeliverable) {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":       err.Error(),
			"reason_code": notDeliverable.ReasonCode,
		})
		return
	}
	var forbidden *ForbiddenError
	if errors.As(err, &forbidden) {
		writeForbidden(w, forbidden)
		return
	}
	if errors.Is(err, engine.ErrSessionUnknown) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"error":       err.Error(),
			"reason_code": "SESSION_UNKNOWN",
		})
		return
	}
	writeJSON(w, status, map[string]any{"error": err.Error()})
}
