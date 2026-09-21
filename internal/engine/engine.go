// Package engine wires Lymph's canonical stores together and exposes the
// operations the daemon, the SDK and lymphctl all share.
//
// Ordering rule used everywhere in this package:
//
//  1. decide inside a SQLite transaction
//  2. append one self-contained record to the canonical ledger
//  3. write the projection rows carrying that record's sequence number
//  4. commit
//
// Because the ledger record is self-contained, a crash between the append and
// the commit only costs a projection catch-up on the next start (section 58),
// never correctness.
package engine

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ycooi/Lymph/internal/buildinfo"
	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/ledger"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
)

// Options configures the engine.
type Options struct {
	// Root is the lymph store directory (default /var/lib/lymph).
	Root string
	// DefaultDurability is used when a caller does not ask for one.
	DefaultDurability ledger.Durability
	// Logger receives operational log lines. Nil means silent.
	Logger *slog.Logger
	// ServerVersion is reported to clients at handshake time. It is deliberately
	// independent of the protocol version (mission section 9).
	ServerVersion string
	// Sessions tunes handshake and session-expiry policy.
	Sessions SessionConfig
}

// Engine is the daemon's core.
type Engine struct {
	opts    Options
	ident   identity.File
	ledger  *ledger.Ledger
	objects *objectstore.Store
	db      *projection.DB
	log     *slog.Logger
	started time.Time

	serverVersion string
	sessions      SessionConfig

	// Operational counters. They are process-local and reset on restart, which
	// is what a counter is for; anything that must survive lives in the ledger.
	metrics engineMetrics

	// Feedback produced inside a transaction that cannot be emitted until that
	// transaction commits (the emit path opens its own).
	deferredMu       sync.Mutex
	deferredFeedback []EmitRequest
}

// engineMetrics are the operational counters surfaced by Status (mission
// section 65). They are deliberately not canonical state.
type engineMetrics struct {
	mu                  sync.Mutex
	handshakesTotal     int64
	handshakeRejections int64
	sessionReconnects   int64
	sessionlessEvents   int64
	spoolReplays        int64
}

func (m *engineMetrics) snapshot() (handshakes, rejections, reconnects, sessionless, replays int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.handshakesTotal, m.handshakeRejections, m.sessionReconnects,
		m.sessionlessEvents, m.spoolReplays
}

func (e *Engine) sessionsOpened() {
	e.metrics.mu.Lock()
	e.metrics.handshakesTotal++
	e.metrics.mu.Unlock()
}

func (e *Engine) handshakeRejected() {
	e.metrics.mu.Lock()
	e.metrics.handshakeRejections++
	e.metrics.mu.Unlock()
}

func (e *Engine) sessionReconnected() {
	e.metrics.mu.Lock()
	e.metrics.sessionReconnects++
	e.metrics.mu.Unlock()
}

func (e *Engine) sessionlessEvent() {
	e.metrics.mu.Lock()
	e.metrics.sessionlessEvents++
	e.metrics.mu.Unlock()
}

func (e *Engine) spoolReplay() {
	e.metrics.mu.Lock()
	e.metrics.spoolReplays++
	e.metrics.mu.Unlock()
}

// Paths lists the directories and files the engine owns. Only lymphd writes
// here; applications never touch these directories (section 13).
type Paths struct {
	Root    string `json:"root"`
	Ledger  string `json:"ledger"`
	Objects string `json:"objects"`
	DB      string `json:"db"`
	Spool   string `json:"spool"`
}

// DefaultRoot is the production store location.
const DefaultRoot = "/var/lib/lymph"

// Open initialises the store, verifies canonical integrity and brings the
// projection up to date (section 58).
func Open(opts Options) (*Engine, error) {
	if opts.Root == "" {
		opts.Root = DefaultRoot
	}
	if opts.DefaultDurability == "" {
		opts.DefaultDurability = ledger.Async
	}
	if opts.Sessions.IdleTimeout <= 0 {
		opts.Sessions = DefaultSessionConfig()
	}
	if opts.ServerVersion == "" {
		opts.ServerVersion = buildinfo.Version
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}

	root, err := filepath.Abs(opts.Root)
	if err != nil {
		return nil, err
	}
	for _, dir := range []string{"", "spool/incoming", "staging/deployments", "snapshots", "locks"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o750); err != nil {
			return nil, err
		}
	}

	// 1. instance identity: never silently regenerated (section 5).
	ident, _, err := identity.Ensure(root)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		opts:          opts,
		ident:         ident,
		log:           log,
		started:       time.Now().UTC(),
		serverVersion: opts.ServerVersion,
		sessions:      opts.Sessions,
	}

	// 2. canonical ledger.
	e.ledger, err = ledger.Open(root, ledger.Options{})
	if err != nil {
		return nil, fmt.Errorf("open ledger: %w", err)
	}
	report, err := e.ledger.Verify()
	if err != nil {
		e.ledger.Close()
		return nil, fmt.Errorf("ledger integrity: %w", err)
	}
	log.Info("ledger verified", "records", report.Records, "segments", report.Segments, "head", report.HeadHash)

	// 3. object store.
	e.objects, err = objectstore.Open(root)
	if err != nil {
		e.ledger.Close()
		return nil, fmt.Errorf("open object store: %w", err)
	}

	// 4. projection.
	e.db, err = projection.Open(root)
	if err != nil {
		e.ledger.Close()
		return nil, fmt.Errorf("open projection: %w", err)
	}
	if err := e.db.SetMeta(projection.MetaInstanceUUID, e.ident.InstanceUUID); err != nil {
		e.close()
		return nil, err
	}

	// 5. catch the projection up with the ledger, from wherever the projection
	// last committed. A fresh projection replays everything; an existing one
	// only replays what a crash left behind (section 58).
	checkpoint, _, _, err := e.db.LedgerCheckpoint()
	if err != nil {
		e.close()
		return nil, err
	}
	caught, err := e.catchUp(checkpoint)
	if err != nil {
		e.close()
		return nil, err
	}
	if caught > 0 {
		log.Info("projection caught up from ledger", "records", caught)
	}

	// 6. expired leases return to the queue (section 42, section 87).
	expired, err := e.SweepLeases(context.Background())
	if err != nil {
		e.close()
		return nil, err
	}
	if expired > 0 {
		log.Warn("released expired work leases", "count", expired)
	}

	// 7. sessions do not survive a restart, by design: the daemon forgets every
	// session it had, clients reconnect, and events resume (mission section 116).
	// Any session rows carried over from a previous run of this store are closed
	// out first so the table cannot accumulate phantom ACTIVE rows.
	stale, deleted, err := e.SweepSessions(context.Background())
	if err != nil {
		e.close()
		return nil, err
	}
	if stale > 0 || deleted > 0 {
		log.Info("session table reconciled at startup", "stale", stale, "deleted", deleted)
	}
	if closed, err := e.closeAllActiveSessions(); err != nil {
		e.close()
		return nil, err
	} else if closed > 0 {
		log.Info("closed sessions left by a previous daemon run", "count", closed)
	}

	return e, nil
}

// closeAllActiveSessions marks every ACTIVE session as STALE. Called at startup:
// the processes that held them are talking to a daemon that no longer exists,
// so they will re-handshake, and until they do the sessions are not valid.
func (e *Engine) closeAllActiveSessions() (int64, error) {
	var closed int64
	now := time.Now().UTC()
	err := e.withTx(func(tx *sql.Tx) error {
		res, err := tx.Exec(`UPDATE sessions SET state = ?, closed_at = ? WHERE state = ?`,
			projection.SessionStale, now.Format(time.RFC3339Nano), projection.SessionActive)
		if err != nil {
			return err
		}
		closed, err = res.RowsAffected()
		return err
	})
	return closed, err
}

// ServerVersion reports the daemon release string.
func (e *Engine) ServerVersion() string { return e.serverVersion }

// SessionConfigForTests exposes the active session policy.
func (e *Engine) SessionConfigForTests() SessionConfig { return e.sessions }

func (e *Engine) close() {
	if e.db != nil {
		e.db.Close()
	}
	if e.ledger != nil {
		e.ledger.Close()
	}
}

// Close shuts the engine down cleanly.
func (e *Engine) Close() error {
	if e.db != nil {
		e.db.Close()
		e.db = nil
	}
	if e.ledger != nil {
		err := e.ledger.Close()
		e.ledger = nil
		return err
	}
	return nil
}

// Root returns the store root.
func (e *Engine) Root() string { return e.opts.Root }

// Paths returns the store layout.
func (e *Engine) Paths() Paths {
	return Paths{
		Root:    e.opts.Root,
		Ledger:  e.ledger.Root(),
		Objects: filepath.Join(e.opts.Root, "objects"),
		DB:      e.db.Path(),
		Spool:   filepath.Join(e.opts.Root, "spool"),
	}
}

// Identity returns the lymph instance identity.
func (e *Engine) Identity() identity.File { return e.ident }

// DB exposes the projection for read paths (the HTTP query handlers and
// lymphctl). It is not a licence to write SQL from outside this package.
func (e *Engine) DB() *projection.DB { return e.db }

// Objects exposes the object store.
func (e *Engine) Objects() *objectstore.Store { return e.objects }

// Ledger exposes the ledger for verification and replay tooling.
func (e *Engine) Ledger() *ledger.Ledger { return e.ledger }

// Status is the daemon summary shown by `lymphctl status` (section 96).
type Status struct {
	InstanceUUID        string         `json:"instance_uuid"`
	Root                string         `json:"root"`
	StartedAt           time.Time      `json:"started_at"`
	LedgerRecords       uint64         `json:"ledger_records"`
	LedgerSequence      uint64         `json:"ledger_sequence"`
	LedgerHead          string         `json:"ledger_head_hash"`
	ProjectedRecords    uint64         `json:"projected_records"`
	Applications        int            `json:"applications"`
	Junctions           int            `json:"junctions"`
	Events              int            `json:"events"`
	Issues              map[string]int `json:"issues"`
	WorkItems           map[string]int `json:"work_items"`
	Candidates          int            `json:"candidates"`
	ObjectCount         int            `json:"object_count"`
	ObjectBytes         int64          `json:"object_bytes"`
	DriftRecords        uint64         `json:"records_pending_projection"`
	ServerVersion       string         `json:"server_version"`
	ProtocolVersions    []string       `json:"protocol_versions"`
	Sessions            map[string]int `json:"sessions"`
	HandshakesTotal     int64          `json:"handshakes_total"`
	HandshakeRejections int64          `json:"handshake_rejections"`
	SessionReconnects   int64          `json:"session_reconnects"`
	SessionlessEvents   int64          `json:"sessionless_events"`
	SpoolReplays        int64          `json:"spool_replays"`
}

// Status collects the summary counters.
func (e *Engine) Status(ctx context.Context) (Status, error) {
	s := Status{
		InstanceUUID:   e.ident.InstanceUUID,
		Root:           e.opts.Root,
		StartedAt:      e.started,
		LedgerRecords:  e.ledger.Count(),
		LedgerSequence: e.ledger.LastSequence(),
	}

	seq, hash, _, err := e.db.LedgerCheckpoint()
	if err != nil {
		return s, err
	}
	s.LedgerHead = hash
	s.ProjectedRecords = seq
	if e.ledger.LastSequence() > seq {
		s.DriftRecords = e.ledger.LastSequence() - seq
	}

	if s.Applications, err = e.db.CountApplications(); err != nil {
		return s, err
	}
	junctions, err := e.db.ListJunctions("")
	if err != nil {
		return s, err
	}
	s.Junctions = len(junctions)
	if s.Events, err = e.db.CountEvents(); err != nil {
		return s, err
	}
	if s.Issues, err = e.db.CountIssuesByStatus(); err != nil {
		return s, err
	}
	if s.WorkItems, err = e.db.CountWorkItemsByState(); err != nil {
		return s, err
	}
	candidates, err := e.db.ListCandidates("", "", 1000)
	if err != nil {
		return s, err
	}
	s.Candidates = len(candidates)
	s.ObjectCount, s.ObjectBytes, err = e.objects.Usage()
	if err != nil {
		return s, err
	}
	// Sessions are runtime state; the counts are operational, not canonical
	// (mission sections 8, 65).
	if s.Sessions, err = e.db.CountSessionsByState(); err != nil {
		return s, err
	}
	s.ServerVersion = e.serverVersion
	s.ProtocolVersions = protocol.Versions()
	s.HandshakesTotal, s.HandshakeRejections, s.SessionReconnects,
		s.SessionlessEvents, s.SpoolReplays = e.metrics.snapshot()
	return s, nil
}

// VerifyLedger re-checks the hash chain on demand.
func (e *Engine) VerifyLedger() (ledger.VerifyReport, error) { return e.ledger.Verify() }

// withTx runs fn in a projection transaction.
func (e *Engine) withTx(fn func(tx *sql.Tx) error) error { return e.db.Tx(fn) }
