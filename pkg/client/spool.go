package client

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DefaultSpoolLimit is the maximum number of events kept in the spool.
//
// The spool is bounded on purpose (section 66): an application must never grow
// an unbounded queue because a daemon stayed down for a month.
const DefaultSpoolLimit = 10000

// Spool is a local, bounded, append-only queue of undelivered events
// (section 67).
type Spool struct {
	dir   string
	limit int
	mu    sync.Mutex
}

// NewSpool opens (creating if needed) a spool directory.
func NewSpool(dir string, limit int) *Spool {
	if limit <= 0 {
		limit = DefaultSpoolLimit
	}
	return &Spool{dir: dir, limit: limit}
}

// Dir returns the spool directory.
func (s *Spool) Dir() string { return s.dir }

func (s *Spool) file() string { return filepath.Join(s.dir, "incoming.jsonl") }

// Append writes an event to the spool. Events carry their id before they are
// sent, so a resend is always idempotent on the daemon side.
func (s *Spool) Append(env emitEnvelope) error {
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return s.appendRaw(raw)
}

// AppendRaw stores an already-serialized event envelope. It exists for tooling
// and tests that hold the wire form rather than the SDK's type.
func (s *Spool) AppendRaw(env map[string]any) error {
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return s.appendRaw(raw)
}

func (s *Spool) appendRaw(raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	count, err := s.count()
	if err != nil {
		return err
	}
	if count >= s.limit {
		return &SpoolFullError{Limit: s.limit}
	}

	f, err := os.OpenFile(s.file(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	raw = append(raw, '\n')
	if _, err := f.Write(raw); err != nil {
		return err
	}
	return f.Sync()
}

// Pending reads every spooled event in order.
func (s *Spool) Pending() ([]emitEnvelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.Open(s.file())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var out []emitEnvelope
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 8<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var env emitEnvelope
		if err := json.Unmarshal(line, &env); err != nil {
			return nil, fmt.Errorf("corrupt spool entry: %w", err)
		}
		out = append(out, env)
	}
	return out, sc.Err()
}

// Clear removes the spool file after a successful flush.
func (s *Spool) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(s.file()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// DropPrefix removes the first n entries, keeping the rest. A flush that fails
// halfway keeps what was not delivered, so a long outage does not resend
// everything and nothing is lost (mission section 34).
func (s *Spool) DropPrefix(n int) error {
	if n <= 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	pending, err := s.pendingUnlocked()
	if err != nil {
		return err
	}
	if n >= len(pending) {
		if err := os.Remove(s.file()); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	remaining := pending[n:]
	var buffer bytes.Buffer
	for _, env := range remaining {
		raw, err := json.Marshal(env)
		if err != nil {
			return err
		}
		buffer.Write(raw)
		buffer.WriteByte('\n')
	}

	dir := filepath.Dir(s.file())
	tmp, err := os.CreateTemp(dir, ".incoming.tmp*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(buffer.Bytes()); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		return err
	}
	return os.Rename(tmpName, s.file())
}

// Count reports how many events are waiting.
func (s *Spool) Count() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count()
}

func (s *Spool) count() (int, error) {
	pending, err := s.pendingUnlocked()
	if err != nil {
		return 0, err
	}
	return len(pending), nil
}

func (s *Spool) pendingUnlocked() ([]emitEnvelope, error) {
	f, err := os.Open(s.file())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []emitEnvelope
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 8<<20)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var env emitEnvelope
		if err := json.Unmarshal(sc.Bytes(), &env); err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	return out, sc.Err()
}

// SpoolInfo describes a spool for diagnostics.
type SpoolInfo struct {
	Dir      string    `json:"dir"`
	Pending  int       `json:"pending"`
	Modified time.Time `json:"modified,omitempty"`
}

// Info summarises the spool.
func (s *Spool) Info() (SpoolInfo, error) {
	count, err := s.Count()
	if err != nil {
		return SpoolInfo{}, err
	}
	info := SpoolInfo{Dir: s.dir, Pending: count}
	if st, err := os.Stat(s.file()); err == nil {
		info.Modified = st.ModTime()
	}
	return info, nil
}
