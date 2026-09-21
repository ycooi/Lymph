package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Protocol version this client speaks. Kept here rather than imported so a
// client can be built without the daemon's internal packages.
const ProtocolVersion = "1"

// Header names used after a successful handshake.
const (
	HeaderSession = "X-Lymph-Session"
	HeaderReplay  = "X-Lymph-Replay"
)

// Typed client errors. Callers switch on these; nobody parses strings
// (mission section 29).

// UnavailableError means Lymph could not be reached: no socket, connection
// refused, timeout. It is the expected case when the daemon is down, and it is
// the signal to spool and carry on.
type UnavailableError struct{ Err error }

func (e *UnavailableError) Error() string { return "lymph unavailable: " + e.Err.Error() }
func (e *UnavailableError) Unwrap() error { return e.Err }

// HandshakeRejectedError means the daemon answered, and refused the identity.
// The reason code is machine-readable: UNKNOWN_APPLICATION,
// UNKNOWN_INSTALLATION, INSTALLATION_APPLICATION_MISMATCH, INVALID_IDENTITY,
// MALFORMED_REQUEST, SESSION_IDENTITY_MISMATCH.
//
// This is not an outage. Spooling forever would hide a configuration mistake,
// so the SDK surfaces it (mission section 28).
type HandshakeRejectedError struct {
	ReasonCode string
	Message    string
}

func (e *HandshakeRejectedError) Error() string {
	return fmt.Sprintf("lymph refused this identity: %s: %s", e.ReasonCode, e.Message)
}

// ProtocolMismatchError means the daemon does not speak this client's protocol
// version. Events that the daemon could never interpret must not be spooled.
type ProtocolMismatchError struct {
	Requested string
	Supported []string
}

func (e *ProtocolMismatchError) Error() string {
	return fmt.Sprintf("daemon speaks protocol %v, client speaks %s", e.Supported, e.Requested)
}

// IdentityMismatchError means the daemon knows this session but the event
// disagrees with it. A programming fault: never retried, never spooled.
type IdentityMismatchError struct{ Message string }

func (e *IdentityMismatchError) Error() string { return "session identity mismatch: " + e.Message }

// SpoolFullError means the bounded spool refused another event. The application
// decides whether that is worth escalating (mission section 88).
type SpoolFullError struct{ Limit int }

func (e *SpoolFullError) Error() string {
	return fmt.Sprintf("lymph spool is full (%d events)", e.Limit)
}

// SessionInfo is the client's view of its current session.
type SessionInfo struct {
	SessionID       string    `json:"session_id"`
	ApplicationID   string    `json:"application_id"`
	InstallationID  string    `json:"installation_id"`
	ProcessID       string    `json:"process_id"`
	ProtocolVersion string    `json:"protocol_version"`
	LymphInstanceID string    `json:"lymph_instance_id"`
	ServerVersion   string    `json:"server_version"`
	Capabilities    []string  `json:"capabilities"`
	Warnings        []string  `json:"warnings,omitempty"`
	EstablishedAt   time.Time `json:"established_at"`
}

// HelloRequest is the wire handshake (mission section 10).
type HelloRequest struct {
	ProtocolVersion string `json:"protocol_version"`
	ApplicationID   string `json:"application_id"`
	InstallationID  string `json:"installation_id"`
	ProcessID       string `json:"process_id"`
	ClientName      string `json:"client_name,omitempty"`
	ClientVersion   string `json:"client_version,omitempty"`
	ManifestHash    string `json:"manifest_hash,omitempty"`
	PID             int    `json:"pid,omitempty"`
	Hostname        string `json:"hostname,omitempty"`
}

// HelloResponse is the daemon's answer (mission section 11).
type HelloResponse struct {
	Accepted          bool     `json:"accepted"`
	ProtocolVersion   string   `json:"protocol_version"`
	ServerVersion     string   `json:"server_version"`
	LymphInstanceID   string   `json:"lymph_instance_id"`
	SessionID         string   `json:"session_id"`
	ApplicationID     string   `json:"application_id"`
	InstallationID    string   `json:"installation_id"`
	ProcessID         string   `json:"process_id"`
	Registered        bool     `json:"registered"`
	Capabilities      []string `json:"capabilities"`
	ReasonCode        string   `json:"reason_code"`
	Message           string   `json:"message"`
	SupportedVersions []string `json:"supported_versions"`
	Warnings          []string `json:"warnings"`
}

// ApplicationOptions is the recommended constructor for an application that
// knows its own identity (mission section 48).
type ApplicationOptions struct {
	Socket string

	ApplicationID  string
	InstallationID string

	ClientName    string
	ClientVersion string
	ManifestHash  string

	SpoolDir string
	Timeout  time.Duration

	// AutoHandshake defaults to true here: an application client that has its
	// identity should use it.
	AutoHandshake *bool
	Logger        Logger
}

// NewApplicationClient builds a client for a registered application. It
// generates this process's UUIDv7 once and keeps it for the process lifetime
// (mission sections 24, 48).
//
// It performs no network I/O: constructing a client must never block an
// application's start-up.
func NewApplicationClient(opts ApplicationOptions) (*Client, error) {
	if opts.ApplicationID != "" && !validUUID(opts.ApplicationID) {
		return nil, fmt.Errorf("application_id %q is not a UUID", opts.ApplicationID)
	}
	if opts.InstallationID != "" && !validUUID(opts.InstallationID) {
		return nil, fmt.Errorf("installation_id %q is not a UUID", opts.InstallationID)
	}
	auto := true
	if opts.AutoHandshake != nil {
		auto = *opts.AutoHandshake
	}
	return New(Options{
		Socket:           opts.Socket,
		ApplicationID:    opts.ApplicationID,
		InstallationID:   opts.InstallationID,
		ClientName:       opts.ClientName,
		ClientVersion:    opts.ClientVersion,
		ManifestHash:     opts.ManifestHash,
		SpoolDir:         opts.SpoolDir,
		Timeout:          opts.Timeout,
		AutoHandshake:    auto,
		AutoReconnect:    true,
		AutoFlushSpool:   true,
		HandshakeTimeout: opts.Timeout,
		Logger:           opts.Logger,
	}), nil
}

// IdentityFile is the recommended application-side identity document. Lymph
// itself stays authoritative through registration (mission section 76).
type IdentityFile struct {
	ApplicationID  string `json:"application_id"`
	InstallationID string `json:"installation_id"`
	Socket         string `json:"socket,omitempty"`
}

// LoadIdentityFile reads and validates an application identity file. It never
// creates one: identity is delivered by registration, not invented by a client.
func LoadIdentityFile(path string) (IdentityFile, error) {
	var identity IdentityFile
	raw, err := os.ReadFile(path)
	if err != nil {
		return identity, err
	}
	if err := json.Unmarshal(raw, &identity); err != nil {
		return identity, fmt.Errorf("parse %s: %w", path, err)
	}
	if !validUUID(identity.ApplicationID) {
		return identity, fmt.Errorf("%s: application_id %q is not a UUID", path, identity.ApplicationID)
	}
	if identity.InstallationID != "" && !validUUID(identity.InstallationID) {
		return identity, fmt.Errorf("%s: installation_id %q is not a UUID", path, identity.InstallationID)
	}
	return identity, nil
}

// ClientFromIdentityFile is the one-line integration path:
//
//	lymph, err := client.ClientFromIdentityFile("/etc/<application>/lymph.json", spoolDir)
func ClientFromIdentityFile(path, spoolDir string, opts ...func(*ApplicationOptions)) (*Client, error) {
	identity, err := LoadIdentityFile(path)
	if err != nil {
		return nil, err
	}
	options := ApplicationOptions{
		ApplicationID:  identity.ApplicationID,
		InstallationID: identity.InstallationID,
		Socket:         identity.Socket,
		SpoolDir:       spoolDir,
	}
	for _, apply := range opts {
		apply(&options)
	}
	return NewApplicationClient(options)
}

// sessionState is the client's handshake state. It exists so one handshake
// serves every concurrent Emit on the same client (mission section 82).
type sessionState struct {
	mu      sync.RWMutex
	current *SessionInfo
	// handshaking serialises concurrent callers into a single /v1/hello.
	handshaking sync.Mutex
}

// Session returns the current session, if one exists.
func (c *Client) Session() (SessionInfo, bool) {
	c.session.mu.RLock()
	defer c.session.mu.RUnlock()
	if c.session.current == nil {
		return SessionInfo{}, false
	}
	return *c.session.current, true
}

// ProcessID returns this client's process identity. It is generated once, at
// construction, and never regenerated (mission section 24).
func (c *Client) ProcessID() string {
	c.session.mu.RLock()
	defer c.session.mu.RUnlock()
	return c.processID
}

func (c *Client) setSession(info *SessionInfo) {
	c.session.mu.Lock()
	defer c.session.mu.Unlock()
	c.session.current = info
}

// clearSession forgets the current session, which is what a client does when
// the daemon says it does not know it (mission section 35).
func (c *Client) clearSession() {
	c.session.mu.Lock()
	defer c.session.mu.Unlock()
	c.session.current = nil
}

// Hello performs the handshake explicitly. Most applications never call it:
// Emit does it lazily. It exists for start-up patterns that want to know early
// whether Lymph is there (mission sections 26, 56).
func (c *Client) Hello(ctx context.Context) (HelloResponse, error) {
	// One handshake at a time, so a burst of Emits produces one /v1/hello
	// rather than a stampede.
	c.session.handshaking.Lock()
	defer c.session.handshaking.Unlock()

	return c.handshake(ctx)
}

func (c *Client) handshake(ctx context.Context) (HelloResponse, error) {
	req := HelloRequest{
		ProtocolVersion: ProtocolVersion,
		ApplicationID:   c.opts.ApplicationID,
		InstallationID:  c.opts.InstallationID,
		ProcessID:       c.processID,
		ClientName:      c.opts.ClientName,
		ClientVersion:   c.opts.ClientVersion,
		ManifestHash:    c.opts.ManifestHash,
		PID:             os.Getpid(),
		Hostname:        hostnameOrEmpty(),
	}

	var resp HelloResponse
	err := c.postWithHeaders(ctx, "/v1/hello", req, nil, &resp)
	if err != nil {
		var httpErr *httpStatusError
		if errors.As(err, &httpErr) {
			switch httpErr.ReasonCode {
			case "PROTOCOL_INCOMPATIBLE":
				return resp, &ProtocolMismatchError{Requested: ProtocolVersion, Supported: httpErr.SupportedVersions}
			case "SESSION_UNKNOWN", "SESSION_IDENTITY_MISMATCH":
				// Not a handshake problem: the caller was mid-conversation.
				return resp, &HandshakeRejectedError{ReasonCode: httpErr.ReasonCode, Message: httpErr.Message}
			default:
				return resp, &HandshakeRejectedError{ReasonCode: httpErr.ReasonCode, Message: httpErr.Message}
			}
		}
		// Anything else at the transport level is an outage, not a rejection.
		return resp, &UnavailableError{Err: err}
	}
	if !resp.Accepted {
		return resp, &HandshakeRejectedError{ReasonCode: resp.ReasonCode, Message: resp.Message}
	}

	c.setSession(&SessionInfo{
		SessionID:       resp.SessionID,
		ApplicationID:   resp.ApplicationID,
		InstallationID:  resp.InstallationID,
		ProcessID:       resp.ProcessID,
		ProtocolVersion: resp.ProtocolVersion,
		LymphInstanceID: resp.LymphInstanceID,
		ServerVersion:   resp.ServerVersion,
		Capabilities:    resp.Capabilities,
		Warnings:        resp.Warnings,
		EstablishedAt:   time.Now().UTC(),
	})
	c.logf("session established: %s (installation %s, process %s)",
		resp.SessionID, resp.InstallationID, resp.ProcessID)
	for _, warning := range resp.Warnings {
		c.logf("lymph warning: %s", warning)
	}
	return resp, nil
}

// ensureSession returns a usable session, handshaking if needed. It is called on
// the emit path, so it must be cheap when a session already exists.
func (c *Client) ensureSession(ctx context.Context) error {
	if _, ok := c.Session(); ok {
		return nil
	}
	if !c.opts.AutoHandshake {
		return nil
	}
	if c.opts.ApplicationID == "" || c.opts.InstallationID == "" {
		// Without identity there is nothing to handshake about; the client is a
		// legacy sessionless one (mission section 22).
		return nil
	}
	c.session.handshaking.Lock()
	defer c.session.handshaking.Unlock()
	if _, ok := c.Session(); ok {
		return nil
	}
	if _, err := c.handshake(ctx); err != nil {
		return err
	}
	return nil
}

func newProcessID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}

func validUUID(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil
}

func hostnameOrEmpty() string {
	name, err := os.Hostname()
	if err != nil {
		return ""
	}
	return name
}
