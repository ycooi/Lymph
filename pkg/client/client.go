// Package client is the Go SDK for Lymph (section 62).
//
// The whole point of the client is that an application never waits for Lymph
// (section 66). Emit tries the socket, and if the daemon is down the event goes
// to a local bounded spool and the call returns success. The application keeps
// running; Lymph catches up later.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ycooi/Lymph/internal/protocol"
)

// DefaultSocket is the production socket path.
const DefaultSocket = "/run/lymph/lymph.sock"

// Options configures a client.
type Options struct {
	// Socket is the Unix socket path. Ignored when BaseURL is set.
	Socket string
	// BaseURL talks to an explicit HTTP endpoint instead (tests, remote hosts).
	BaseURL string
	// ProducerInstance identifies this runtime instance, not the application
	// (section 78). Two copies of an app may share an application UUID but must
	// not share a producer instance UUID.
	//
	// Deprecated: prefer ProcessID. Both mean "this running process"; the client
	// normalizes them to a single value.
	ProducerInstance string
	// ProcessID is this running process's identity (mission section 5). Empty
	// means one is generated with UUIDv7 at construction and kept for the
	// process lifetime.
	ProcessID string
	// ApplicationID and InstallationID are the persistent identities from
	// registration. Supplying them enables the handshake.
	ApplicationID string
	// InstallationID distinguishes deployments of the same application
	// (section 79).
	InstallationID string
	// ClientName, ClientVersion and ManifestHash are handshake metadata.
	ClientName    string
	ClientVersion string
	ManifestHash  string
	// AutoHandshake performs the handshake lazily on first use.
	AutoHandshake bool
	// AutoReconnect re-handshakes once when the daemon has forgotten a session.
	AutoReconnect bool
	// AutoFlushSpool attempts a spool flush after a successful handshake.
	AutoFlushSpool bool
	// HandshakeTimeout bounds the handshake. Zero means Timeout.
	HandshakeTimeout time.Duration
	// Logger receives SDK diagnostics. Nil means silent: an SDK must not spam an
	// application's logs.
	Logger Logger
	// SpoolDir is where undeliverable events are kept. Empty disables spooling,
	// which makes Emit return an error when the daemon is unreachable.
	SpoolDir string
	// Timeout bounds one HTTP call.
	Timeout time.Duration
	// HTTPClient overrides the transport (used by tests).
	HTTPClient *http.Client
}

// Client is a Lymph client.
type Client struct {
	base      string
	http      *http.Client
	spool     *Spool
	opts      Options
	processID string
	session   sessionState
}

// Logger is the small logging interface the SDK uses. It is one method so an
// application can pass whatever it already has.
type Logger interface {
	Logf(format string, args ...any)
}

func (c *Client) logf(format string, args ...any) {
	if c.opts.Logger != nil {
		c.opts.Logger.Logf(format, args...)
	}
}

// New builds a client.
func New(opts Options) *Client {
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.HandshakeTimeout <= 0 {
		opts.HandshakeTimeout = opts.Timeout
	}
	// ProcessID and ProducerInstance are the same concept. Normalizing here
	// means old calls keep working and new calls read naturally.
	if opts.ProcessID == "" {
		opts.ProcessID = opts.ProducerInstance
	}
	if opts.ProducerInstance == "" {
		opts.ProducerInstance = opts.ProcessID
	}
	c := &Client{opts: opts, processID: opts.ProcessID}
	if c.processID == "" {
		c.processID = newProcessID()
		c.opts.ProcessID = c.processID
		c.opts.ProducerInstance = c.processID
	}
	if opts.HTTPClient != nil {
		c.http = opts.HTTPClient
	} else if opts.BaseURL != "" {
		c.http = &http.Client{Timeout: opts.Timeout}
	} else {
		socket := opts.Socket
		if socket == "" {
			socket = DefaultSocket
		}
		c.http = &http.Client{
			Timeout: opts.Timeout,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", socket)
				},
			},
		}
	}
	if opts.BaseURL != "" {
		c.base = strings.TrimRight(opts.BaseURL, "/")
	} else {
		c.base = "http://lymph"
	}
	if opts.SpoolDir != "" {
		c.spool = NewSpool(opts.SpoolDir, 0)
	}
	return c
}

// Spool returns the client's spool, or nil when spooling is disabled.
func (c *Client) Spool() *Spool { return c.spool }

// EmitRequest is the body of a submission (section 64).
type EmitRequest struct {
	Event            protocol.Event `json:"-"`
	ProducerInstance string         `json:"producer_instance,omitempty"`
	InstallationID   string         `json:"installation_id,omitempty"`
	Durability       string         `json:"durability,omitempty"`
	Fingerprint      string         `json:"fingerprint,omitempty"`
}

// EmitResponse acknowledges an accepted event.
type EmitResponse struct {
	EventID        string `json:"event_id"`
	Accepted       bool   `json:"accepted"`
	Duplicate      bool   `json:"duplicate"`
	Durability     string `json:"durability"`
	Fingerprint    string `json:"fingerprint"`
	IssueID        string `json:"issue_id,omitempty"`
	IssueCreated   bool   `json:"issue_created"`
	OccurrenceNo   int    `json:"occurrence_count"`
	LedgerSequence uint64 `json:"ledger_sequence"`
	// Spooled is set by the SDK, not by the daemon: the event was accepted
	// locally because Lymph was unreachable.
	Spooled bool `json:"spooled,omitempty"`
}

// emitEnvelope is the JSON shape the daemon expects: the CloudEvents members
// inline plus the producer metadata.
type emitEnvelope struct {
	SpecVersion      string          `json:"specversion"`
	ID               string          `json:"id"`
	Source           string          `json:"source"`
	Type             string          `json:"type"`
	Subject          string          `json:"subject,omitempty"`
	Time             time.Time       `json:"time"`
	DataContentType  string          `json:"datacontenttype,omitempty"`
	Data             json.RawMessage `json:"data"`
	ProducerInstance string          `json:"producer_instance,omitempty"`
	InstallationID   string          `json:"installation_id,omitempty"`
	Durability       string          `json:"durability,omitempty"`
	Fingerprint      string          `json:"fingerprint,omitempty"`
	// ProtocolVersionAtCreation records which protocol wrote this spool entry,
	// so a replay can be explained. The session is deliberately absent: session
	// IDs are transient and never replay identity (mission section 31).
	ProtocolVersionAtCreation string `json:"protocol_version_at_creation,omitempty"`
}

func (c *Client) envelope(req EmitRequest) emitEnvelope {
	producer := req.ProducerInstance
	if producer == "" {
		producer = c.opts.ProducerInstance
	}
	installation := req.InstallationID
	if installation == "" {
		installation = c.opts.InstallationID
	}
	ev := req.Event
	if ev.SpecVersion == "" {
		ev.SpecVersion = protocol.SpecVersion
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now().UTC()
	}
	return emitEnvelope{
		SpecVersion:               ev.SpecVersion,
		ID:                        ev.ID,
		Source:                    ev.Source,
		Type:                      ev.Type,
		Subject:                   ev.Subject,
		Time:                      ev.Time,
		DataContentType:           ev.DataContentType,
		Data:                      ev.Data,
		ProducerInstance:          producer,
		InstallationID:            installation,
		Durability:                req.Durability,
		Fingerprint:               req.Fingerprint,
		ProtocolVersionAtCreation: ProtocolVersion,
	}
}

// Feedback is the high-level description of one adaptation hiccup (mission
// section 52). Applications describe *what happened*; the SDK assembles the
// CloudEvent.
type Feedback struct {
	// Junction is the registered junction name (or UUID). The daemon resolves
	// it against this application's registration.
	Junction string

	FeedbackType string
	ReasonCode   string

	Subject string

	ConfigRevision   string
	ConfigHash       string
	ContractRevision string

	InputRef  string
	ReplayRef string

	// Payload is any JSON-marshalable domain detail.
	Payload any

	// Durability defaults to ASYNC. Configuration-shaped feedback that must
	// survive a power cut can ask for DURABLE.
	Durability string

	// EventID is normally left empty: the SDK generates a UUIDv7. It is set
	// only when replaying an event that already has an identity.
	EventID string

	// Fingerprint lets a junction supply its own grouping key.
	Fingerprint string
}

// EmitFeedback is the one call most applications need.
//
// It builds the CloudEvent, performs the handshake if one is not established,
// sends under the session, and falls back to the spool when Lymph is
// unreachable. The application never sees a session ID, an event UUID or a
// reconnect.
func (c *Client) EmitFeedback(ctx context.Context, f Feedback) (EmitResponse, error) {
	if f.Junction == "" {
		return EmitResponse{}, errors.New("lymph: Feedback.Junction is required")
	}
	if c.opts.ApplicationID == "" {
		return EmitResponse{}, errors.New("lymph: this client has no ApplicationID; use NewApplicationClient or set Options.ApplicationID")
	}
	feedbackType := strings.ToUpper(f.FeedbackType)
	if feedbackType == "" {
		feedbackType = "UNKNOWN"
	}

	var payload json.RawMessage
	if f.Payload != nil {
		raw, err := json.Marshal(f.Payload)
		if err != nil {
			return EmitResponse{}, fmt.Errorf("lymph: marshal payload: %w", err)
		}
		payload = raw
	}

	eventID := f.EventID
	if eventID == "" {
		eventID = newProcessID() // UUIDv7, from the same generator as process identity
	}
	data, err := json.Marshal(map[string]any{
		"feedback_type":     feedbackType,
		"reason_code":       f.ReasonCode,
		"config_revision":   f.ConfigRevision,
		"config_hash":       f.ConfigHash,
		"contract_revision": f.ContractRevision,
		"input_ref":         f.InputRef,
		"replay_ref":        f.ReplayRef,
		"payload":           payload,
	})
	if err != nil {
		return EmitResponse{}, err
	}

	subject := f.Subject
	if subject == "" {
		subject = f.Junction
	}
	event := protocol.Event{
		SpecVersion:     protocol.SpecVersion,
		ID:              eventID,
		Source:          "lymph://" + c.opts.ApplicationID + "/" + f.Junction,
		Type:            protocol.FeedbackEventType(protocol.FeedbackType(feedbackType)),
		Subject:         subject,
		Time:            time.Now().UTC(),
		DataContentType: "application/json",
		Data:            data,
	}

	return c.Emit(ctx, EmitRequest{
		Event:       event,
		Durability:  f.Durability,
		Fingerprint: f.Fingerprint,
	})
}

// Emit submits one feedback event without blocking the application.
//
// If Lymph is unreachable and a spool is configured, the event is appended to
// the spool and Emit reports success with Spooled=true. If no spool is
// configured the error is returned to the caller.
//
// If a handshake is configured it happens lazily on this first call, so an
// application never pays for Lymph during start-up unless it asks to.
func (c *Client) Emit(ctx context.Context, req EmitRequest) (EmitResponse, error) {
	env := c.envelope(req)
	resp, err := c.tryEmit(ctx, env, false)
	if err == nil {
		return resp, nil
	}

	// The daemon is alive and has simply forgotten this session: handshake once
	// and retry the same event. The event id is stable, so a retry that raced a
	// successful delivery is deduplicated (mission sections 35, 60, 61).
	var sessionGone *sessionGoneError
	if errors.As(err, &sessionGone) {
		c.clearSession()
		if _, helloErr := c.Hello(ctx); helloErr == nil {
			if retry, retryErr := c.tryEmit(ctx, env, false); retryErr == nil {
				return retry, nil
			} else if !isUnavailable(retryErr) {
				return EmitResponse{}, retryErr
			}
		} else if !isUnavailable(helloErr) {
			// The identity was rejected, not the network: surface it rather than
			// spooling forever (mission section 28).
			return EmitResponse{}, helloErr
		}
		err = &UnavailableError{Err: err}
	}

	// A rejected identity or a session mismatch is a configuration fault: the
	// daemon answered, so spooling would hide the problem.
	var rejected *HandshakeRejectedError
	if errors.As(err, &rejected) {
		return EmitResponse{}, err
	}
	var mismatch *IdentityMismatchError
	if errors.As(err, &mismatch) {
		return EmitResponse{}, err
	}
	var protocolErr *ProtocolMismatchError
	if errors.As(err, &protocolErr) {
		return EmitResponse{}, err
	}

	if c.spool == nil {
		return EmitResponse{}, err
	}
	if spoolErr := c.spool.Append(env); spoolErr != nil {
		return EmitResponse{}, fmt.Errorf("lymph unreachable (%v) and spooling failed: %w", err, spoolErr)
	}
	return EmitResponse{
		EventID:   env.ID,
		Accepted:  true,
		Spooled:   true,
		Duplicate: false,
	}, nil
}

// sessionGoneError marks a session the daemon no longer knows.
type sessionGoneError struct{ sessionID string }

func (e *sessionGoneError) Error() string {
	return "lymph does not know session " + e.sessionID + " (expired, closed, or the daemon restarted)"
}

func isUnavailable(err error) bool {
	var unavailable *UnavailableError
	return errors.As(err, &unavailable)
}

// tryEmit performs one attempt: handshake if needed, then POST the event with
// its session header. It classifies failures into the typed errors above.
func (c *Client) tryEmit(ctx context.Context, env emitEnvelope, replay bool) (EmitResponse, error) {
	if err := c.ensureSession(ctx); err != nil {
		var unavailable *UnavailableError
		if errors.As(err, &unavailable) {
			return EmitResponse{}, err
		}
		return EmitResponse{}, err
	}

	headers := map[string]string{}
	if session, ok := c.Session(); ok {
		headers[HeaderSession] = session.SessionID
	}
	if replay {
		headers[HeaderReplay] = "1"
	}

	var resp EmitResponse
	if err := c.postWithHeaders(ctx, "/v1/events", env, headers, &resp); err != nil {
		var status *httpStatusError
		if errors.As(err, &status) {
			switch status.ReasonCode {
			case "SESSION_UNKNOWN":
				return EmitResponse{}, &sessionGoneError{sessionID: headers[HeaderSession]}
			case "SESSION_IDENTITY_MISMATCH":
				return EmitResponse{}, &IdentityMismatchError{Message: status.Message}
			}
		}
		return EmitResponse{}, &UnavailableError{Err: err}
	}
	return resp, nil
}

// EmitStrict is Emit without the spool fallback: it fails loudly if Lymph is
// unreachable.
func (c *Client) EmitStrict(ctx context.Context, req EmitRequest) (EmitResponse, error) {
	return c.tryEmit(ctx, c.envelope(req), false)
}

// FlushSpool resends every spooled event. The daemon deduplicates by event id,
// so resending is always safe (section 67, section 68).
func (c *Client) FlushSpool(ctx context.Context) (int, error) {
	if c.spool == nil {
		return 0, nil
	}

	// Reconnect first: a spool flush after an outage needs a session, and the
	// new session's process may be a different process than the one that
	// produced these events (mission sections 32, 34).
	if c.opts.ApplicationID != "" && c.opts.InstallationID != "" {
		if _, err := c.Hello(ctx); err != nil {
			var unavailable *UnavailableError
			if errors.As(err, &unavailable) {
				return 0, err
			}
			return 0, err
		}
	}

	pending, err := c.spool.Pending()
	if err != nil {
		return 0, err
	}
	sent := 0
	for _, env := range pending {
		// Replay is the one path where the event's producer may legitimately
		// differ from the current session's process: the spool is older than
		// this process. The event keeps its original producer (mission section
		// 63).
		if _, err := c.tryEmit(ctx, env, true); err != nil {
			// The successfully delivered prefix is dropped so a long outage does
			// not resend everything, and the daemon's dedupe makes any overlap
			// harmless (mission section 34).
			if dropErr := c.spool.DropPrefix(sent); dropErr != nil {
				return sent, dropErr
			}
			return sent, err
		}
		sent++
	}
	if err := c.spool.DropPrefix(sent); err != nil {
		return sent, err
	}
	return sent, nil
}

// StartBackgroundFlush runs an opportunistic flush loop. It is deliberately not
// started automatically: an SDK must not spawn goroutines inside an application
// without being asked (mission section 85).
func (c *Client) StartBackgroundFlush(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, ok := c.Session(); !ok {
					continue
				}
				if sent, err := c.FlushSpool(ctx); err != nil {
					c.logf("lymph spool flush failed after %d events: %v", sent, err)
				}
			}
		}
	}()
}

// Health reports whether the daemon answers.
func (c *Client) Health(ctx context.Context) (map[string]any, error) {
	var out map[string]any
	err := c.get(ctx, "/v1/health", &out)
	return out, err
}

// Status fetches the daemon summary.
func (c *Client) Status(ctx context.Context) (map[string]any, error) {
	var out map[string]any
	err := c.get(ctx, "/v1/status", &out)
	return out, err
}

// Get performs a GET and decodes JSON into out.
func (c *Client) Get(ctx context.Context, path string, out any) error {
	return c.get(ctx, path, out)
}

// Post performs a POST and decodes JSON into out.
func (c *Client) Post(ctx context.Context, path string, body any, out any) error {
	return c.post(ctx, path, body, out)
}

// GetRaw performs a GET and returns the response body unchanged. Objects in
// the content-addressed store are not necessarily JSON, so the SDK cannot
// assume they are.
func (c *Client) GetRaw(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("lymph: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return err
	}
	return c.do(req, out)
}

func (c *Client) post(ctx context.Context, path string, body any, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req, out)
}

func (c *Client) do(req *http.Request, out any) error {
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if resp.StatusCode >= 400 {
		return statusError(resp.StatusCode, body)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(body, out)
}

// httpStatusError is a daemon answer that carries a machine-readable reason.
// Callers switch on ReasonCode; the message is for humans (mission section 29).
type httpStatusError struct {
	Status            int
	ReasonCode        string
	Message           string
	SupportedVersions []string
	Body              string
}

func (e *httpStatusError) Error() string {
	if e.ReasonCode != "" {
		return fmt.Sprintf("lymph: %s: %s (status %d)", e.ReasonCode, e.Message, e.Status)
	}
	if e.Message != "" {
		return fmt.Sprintf("lymph: %s (status %d)", e.Message, e.Status)
	}
	return fmt.Sprintf("lymph: status %d: %s", e.Status, strings.TrimSpace(e.Body))
}

// statusError turns an error response body into a structured error, falling back
// to the raw body when the daemon did not send a reason code.
func statusError(status int, body []byte) error {
	var payload struct {
		Error             string   `json:"error"`
		ReasonCode        string   `json:"reason_code"`
		Message           string   `json:"message"`
		SupportedVersions []string `json:"supported_versions"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return &httpStatusError{Status: status, Body: string(body)}
	}
	message := payload.Message
	if message == "" {
		message = payload.Error
	}
	return &httpStatusError{
		Status:            status,
		ReasonCode:        payload.ReasonCode,
		Message:           message,
		SupportedVersions: payload.SupportedVersions,
		Body:              string(body),
	}
}

// postWithHeaders is post plus explicit headers. The handshake and the session
// header travel here rather than in the body (mission sections 20, 33).
func (c *Client) postWithHeaders(ctx context.Context, path string, body any, headers map[string]string, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		if value != "" {
			req.Header.Set(name, value)
		}
	}
	return c.do(req, out)
}

// Query builds a query string from key/value pairs, skipping empty values.
func Query(pairs ...string) string {
	values := url.Values{}
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			values.Set(pairs[i], pairs[i+1])
		}
	}
	if len(values) == 0 {
		return ""
	}
	return "?" + values.Encode()
}

// ---------- typed worker returns (mission section 30) ----------

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

// ImprovementArtifact is one typed thing a worker produced.
type ImprovementArtifact struct {
	Kind         ArtifactKind      `json:"kind"`
	ConfigBundle map[string][]byte `json:"config_bundle,omitempty"`
	Explanation  string            `json:"explanation,omitempty"`
	BaseRevision string            `json:"base_revision_id,omitempty"`
	CodeRef      *CodeRef          `json:"code_ref,omitempty"`
	ModelRef     *ModelRef         `json:"model_ref,omitempty"`
	DataFixRef   *DataFixRef       `json:"data_fix_ref,omitempty"`
	NoChange     *NoChange         `json:"no_change,omitempty"`
}

// CodeRef records a code change that lives in a repository Lymph never clones.
type CodeRef struct {
	Repository  string `json:"repository"`
	Commit      string `json:"commit"`
	TreeHash    string `json:"tree_hash,omitempty"`
	Branch      string `json:"branch,omitempty"`
	Description string `json:"description,omitempty"`
}

// ModelRef records a model artifact that lives outside Lymph.
type ModelRef struct {
	URI         string `json:"uri"`
	Digest      string `json:"digest"`
	ModelType   string `json:"model_type,omitempty"`
	Version     string `json:"version,omitempty"`
	Description string `json:"description,omitempty"`
}

// DataFixRef records a correction to data rather than code or configuration.
type DataFixRef struct {
	URI         string `json:"uri,omitempty"`
	Digest      string `json:"digest,omitempty"`
	PatchID     string `json:"patch_id,omitempty"`
	Target      string `json:"target,omitempty"`
	Description string `json:"description,omitempty"`
}

// NoChange records the conclusion that the current configuration is correct.
type NoChange struct {
	Reason      string `json:"reason"`
	Disposition string `json:"disposition,omitempty"`
}

// ImprovementReturn is one worker return.
type ImprovementReturn struct {
	Worker        string                `json:"worker"`
	Attempt       int                   `json:"attempt"`
	WorkflowRunID string                `json:"workflow_run_id,omitempty"`
	Summary       string                `json:"summary,omitempty"`
	Artifacts     []ImprovementArtifact `json:"artifacts"`
}

// ReturnImprovement records a worker's typed output in one atomic call.
//
// A worker that returns here does not need to know how Lymph stores anything:
// it says what it produced, and Lymph canonicalises it.
func (c *Client) ReturnImprovement(ctx context.Context, workID string, ret ImprovementReturn) (map[string]any, error) {
	var out map[string]any
	err := c.post(ctx, "/v1/work-items/"+workID+"/return", ret, &out)
	return out, err
}

// ImprovementResults lists worker returns.
func (c *Client) ImprovementResults(ctx context.Context, applicationID string, limit int) (map[string]any, error) {
	var out map[string]any
	err := c.get(ctx, "/v1/improvement-results"+Query("application", applicationID, "limit", itoa(limit)), &out)
	return out, err
}

// Artifacts lists typed worker output.
func (c *Client) Artifacts(ctx context.Context, kind ArtifactKind, applicationID string) (map[string]any, error) {
	var out map[string]any
	err := c.get(ctx, "/v1/artifacts"+Query("kind", string(kind), "application", applicationID), &out)
	return out, err
}

// Installations lists registered deployments.
func (c *Client) Installations(ctx context.Context, applicationID string) (map[string]any, error) {
	var out map[string]any
	err := c.get(ctx, "/v1/installations"+Query("application", applicationID), &out)
	return out, err
}

func itoa(n int) string {
	if n <= 0 {
		return ""
	}
	return strconv.Itoa(n)
}
