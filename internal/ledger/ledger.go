// Package ledger implements the append-only event ledger: Lymph's canonical
// truth (section 12, section 14).
//
// Layout (section 13):
//
//	<root>/ledger/2026/09/segment-000001.log
//
// Each line is one self-describing JSON record. Every record carries the hash
// of its predecessor, so the chain proves that no accepted record was edited,
// reordered or dropped. This is integrity, not security (section 14).
package ledger

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ycooi/Lymph/internal/fsutil"
)

// Kind classifies a canonical record. The projection is built by replaying
// records and dispatching on kind.
type Kind string

// Record kinds currently emitted by Lymph.
const (
	KindEvent                   Kind = "EVENT"
	KindIssue                   Kind = "ISSUE"
	KindWorkItem                Kind = "WORK_ITEM"
	KindImprovementResult       Kind = "IMPROVEMENT_RESULT"
	KindUpdateApplicationResult Kind = "UPDATE_APPLICATION_RESULT"
	KindUpdateWithdrawn         Kind = "UPDATE_WITHDRAWN"
	KindRef                     Kind = "REF"
	KindCandidate               Kind = "CANDIDATE"
	KindRevision                Kind = "REVISION"
	KindValidation              Kind = "VALIDATION"
	KindApproval                Kind = "APPROVAL"
	KindRegistration            Kind = "REGISTRATION"
	KindAudit                   Kind = "AUDIT"
)

// Durability selects the fsync policy for one append (section 15).
//
// Feedback may be ASYNC: the record reaches the file without waiting for the
// platter. Configuration promotion must be DURABLE: ledger append, fsync,
// projection transaction, then acknowledgement.
type Durability string

// Durability modes.
const (
	Async   Durability = "ASYNC"
	Durable Durability = "DURABLE"
)

// Record is one ledger entry (section 14).
type Record struct {
	Sequence   uint64          `json:"sequence"`
	Kind       Kind            `json:"kind"`
	ObjectID   string          `json:"object_id"`
	Timestamp  string          `json:"timestamp"`
	Payload    json.RawMessage `json:"payload"`
	PrevHash   string          `json:"previous_record_hash"`
	RecordHash string          `json:"record_hash"`
}

// Options configures a ledger.
type Options struct {
	// MaxSegmentBytes rotates the active segment once it exceeds this size.
	// Zero means DefaultMaxSegmentBytes.
	MaxSegmentBytes int64
	// OnAppend is called after a record is committed to the file. The
	// projection indexer uses it to keep the SQLite projection in step with
	// the ledger without the ledger knowing anything about SQL.
	OnAppend func(Record) error
}

// DefaultMaxSegmentBytes is the segment rotation threshold.
const DefaultMaxSegmentBytes = 64 << 20

// Ledger is a single-writer append-only log.
type Ledger struct {
	root string
	opts Options

	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	segPath  string
	segYear  int
	segMonth int
	segNum   int
	segBytes int64

	sequence uint64
	lastHash string
	count    uint64

	// tornTailBytes counts bytes discarded from the last segment during Open:
	// a record that was being written when the process or the filesystem
	// failed. It was never acknowledged, so it is not part of history.
	tornTailBytes     int64
	tornTailRecovered bool
}

// Open opens (or creates) the ledger under root and recovers the tail of the
// chain so the next append continues it (section 58).
func Open(root string, opts Options) (*Ledger, error) {
	if opts.MaxSegmentBytes <= 0 {
		opts.MaxSegmentBytes = DefaultMaxSegmentBytes
	}
	l := &Ledger{root: filepath.Join(root, "ledger"), opts: opts}
	if err := os.MkdirAll(l.root, 0o750); err != nil {
		return nil, err
	}
	segments, err := l.segments()
	if err != nil {
		return nil, err
	}
	// Rotation creates and directory-syncs the next segment before the first
	// record is written. A crash in that small window leaves a zero-byte newest
	// segment. It contains no acknowledged history, so remove it and resume from
	// the preceding segment. Empty segments anywhere else remain corruption.
	for len(segments) > 0 {
		last := segments[len(segments)-1]
		info, statErr := os.Stat(last)
		if statErr != nil {
			return nil, statErr
		}
		if info.Size() != 0 {
			break
		}
		if err := os.Remove(last); err != nil {
			return nil, fmt.Errorf("remove empty ledger tail %s: %w", last, err)
		}
		if err := fsutil.SyncDir(filepath.Dir(last)); err != nil {
			return nil, fmt.Errorf("sync ledger directory after removing empty tail %s: %w", last, err)
		}
		segments = segments[:len(segments)-1]
	}
	if len(segments) > 0 {
		for _, seg := range segments[:len(segments)-1] {
			c, torn, err := countRecords(seg, false)
			if err != nil {
				return nil, err
			}
			if c == 0 {
				return nil, fmt.Errorf("ledger segment %s is empty in the middle of the log", seg)
			}
			if torn > 0 {
				return nil, fmt.Errorf("ledger segment %s ends mid-record (%d bytes): refusing to continue past a corrupted middle of the log", seg, torn)
			}
			l.count += c
		}
		last := segments[len(segments)-1]
		rec, size, n, torn, err := readTail(last)
		if err != nil {
			return nil, err
		}
		if torn > 0 {
			// A torn tail is a crash artefact, not corruption: the record was
			// never acknowledged to a caller. Drop it and carry on.
			if err := truncateTail(last, torn); err != nil {
				return nil, fmt.Errorf("recover torn ledger tail in %s: %w", last, err)
			}
			size -= torn
			l.tornTailBytes = torn
		}
		l.sequence = rec.Sequence
		l.lastHash = rec.RecordHash
		l.segBytes = size
		l.count += n
	}
	return l, nil
}

// Root returns the ledger directory.
func (l *Ledger) Root() string { return l.root }

// LastSequence returns the highest committed sequence number.
func (l *Ledger) LastSequence() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sequence
}

// Count returns the number of committed records.
func (l *Ledger) Count() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count
}

// HeadHash returns the hash of the newest committed record, or "" when the
// ledger is empty. This is the cheap form of the value Verify() recomputes.
func (l *Ledger) HeadHash() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastHash
}

// Append commits one record and returns it, hash chain included.
//
// The returned record is what downstream projections index.
func (l *Ledger) Append(kind Kind, objectID string, value any, durability Durability) (Record, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return Record{}, fmt.Errorf("marshal %s payload: %w", kind, err)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.ensureSegment(time.Now().UTC()); err != nil {
		return Record{}, err
	}

	rec := Record{
		Sequence:  l.sequence + 1,
		Kind:      kind,
		ObjectID:  objectID,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Payload:   payload,
		PrevHash:  l.lastHash,
	}
	rec.RecordHash = Hash(rec.PrevHash, rec.Sequence, rec.Kind, rec.ObjectID, rec.Timestamp, rec.Payload)

	line, err := json.Marshal(rec)
	if err != nil {
		return Record{}, fmt.Errorf("marshal ledger record: %w", err)
	}
	line = append(line, '\n')

	if _, err := l.writer.Write(line); err != nil {
		l.discardFailedAppend()
		return Record{}, fmt.Errorf("append ledger record: %w", err)
	}
	if err := l.writer.Flush(); err != nil {
		// The kernel may have taken part of the record. A half-written record
		// is neither accepted nor canonical, so it is removed rather than left
		// to corrupt the next append: "complete or absent" (test plan section
		// 23). If the truncate itself fails, Open() recovers the torn tail on
		// the next start.
		l.discardFailedAppend()
		return Record{}, fmt.Errorf("flush ledger record: %w", err)
	}
	if durability == Durable {
		if err := l.file.Sync(); err != nil {
			return Record{}, fmt.Errorf("fsync ledger record: %w", err)
		}
	}

	l.sequence = rec.Sequence
	l.lastHash = rec.RecordHash
	l.segBytes += int64(len(line))
	l.count++

	if l.opts.OnAppend != nil {
		if err := l.opts.OnAppend(rec); err != nil {
			return rec, fmt.Errorf("index ledger record %d: %w", rec.Sequence, err)
		}
	}
	return rec, nil
}

// discardFailedAppend rewinds the active segment to the last accepted record.
func (l *Ledger) discardFailedAppend() {
	offset := l.segBytes
	l.writer.Reset(l.file)
	if err := l.file.Truncate(offset); err != nil {
		return // Open() will recover the torn tail instead.
	}
	l.segBytes = offset
	l.tornTailRecovered = true
}

// ForEach replays every record in commit order.
func (l *Ledger) ForEach(fn func(Record) error) error {
	segments, err := l.segments()
	if err != nil {
		return err
	}
	for _, seg := range segments {
		if _, err := scanFile(seg, false, fn); err != nil {
			return err
		}
	}
	return nil
}

// TornTailBytes reports how many bytes of an unfinished record were discarded
// when this ledger was opened. Zero means the log ended cleanly.
func (l *Ledger) TornTailBytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.tornTailBytes
}

// VerifyReport summarises a chain verification pass.
type VerifyReport struct {
	Records   uint64 `json:"records"`
	Segments  int    `json:"segments"`
	FirstSeq  uint64 `json:"first_sequence"`
	LastSeq   uint64 `json:"last_sequence"`
	HeadHash  string `json:"head_hash"`
	BytesRead int64  `json:"bytes_read"`
	// TornTailBytes is the size of an unfinished record discarded from the end
	// of the log. It was never acknowledged, so discarding it loses nothing.
	TornTailBytes int64 `json:"torn_tail_bytes,omitempty"`
}

// Verify walks the whole ledger and checks sequence continuity plus the hash
// chain. A broken chain is an integrity failure, not a warning.
func (l *Ledger) Verify() (VerifyReport, error) {
	var report VerifyReport
	var prev string
	var expectSeq uint64 = 1

	segments, err := l.segments()
	if err != nil {
		return report, err
	}
	report.Segments = len(segments)

	for i, seg := range segments {
		last := i == len(segments)-1
		torn, err := scanFile(seg, last, func(rec Record) error {
			if rec.Sequence != expectSeq {
				return fmt.Errorf("ledger sequence break: expected %d, found %d", expectSeq, rec.Sequence)
			}
			if rec.PrevHash != prev {
				return fmt.Errorf("ledger chain break at sequence %d: previous hash %q, expected %q", rec.Sequence, rec.PrevHash, prev)
			}
			want := Hash(rec.PrevHash, rec.Sequence, rec.Kind, rec.ObjectID, rec.Timestamp, rec.Payload)
			if rec.RecordHash != want {
				return fmt.Errorf("ledger record %d is corrupt: hash %q, recomputed %q", rec.Sequence, rec.RecordHash, want)
			}
			prev = rec.RecordHash
			report.Records++
			report.LastSeq = rec.Sequence
			if report.FirstSeq == 0 {
				report.FirstSeq = rec.Sequence
			}
			expectSeq++
			return nil
		})
		if err != nil {
			return report, err
		}
		if torn > 0 {
			if !last {
				return report, fmt.Errorf("ledger segment %s ends mid-record (%d bytes): history is damaged before the end of the log", seg, torn)
			}
			// Reported, not fatal: an unfinished record at the very end was
			// never acknowledged to a caller.
			report.TornTailBytes = torn
		}
	}
	report.HeadHash = prev
	for _, seg := range segments {
		if info, statErr := os.Stat(seg); statErr == nil {
			report.BytesRead += info.Size()
		}
	}
	return report, nil
}

// Close flushes and closes the active segment.
func (l *Ledger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closeLocked()
}

func (l *Ledger) closeLocked() error {
	if l.file == nil {
		return nil
	}
	err := l.writer.Flush()
	if syncErr := l.file.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := l.file.Close(); err == nil {
		err = closeErr
	}
	l.file = nil
	l.writer = nil
	return err
}

// ensureSegment makes sure the active segment is open and belongs to the
// current UTC month, rotating first when the size threshold is crossed.
func (l *Ledger) ensureSegment(now time.Time) error {
	if l.file != nil {
		sameMonth := l.segYear == now.Year() && l.segMonth == int(now.Month())
		tooBig := l.segBytes >= l.opts.MaxSegmentBytes
		if sameMonth && !tooBig {
			return nil
		}
		if err := l.closeLocked(); err != nil {
			return err
		}
	}

	dir := filepath.Join(l.root, fmt.Sprintf("%04d", now.Year()), fmt.Sprintf("%02d", int(now.Month())))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}

	next := 1
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if n, ok := segmentNumber(e.Name()); ok && n >= next {
			next = n + 1
		}
	}

	path := filepath.Join(dir, segmentName(next))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	if err := fsutil.SyncDir(dir); err != nil {
		f.Close()
		return err
	}

	l.file = f
	l.writer = bufio.NewWriterSize(f, 64<<10)
	l.segPath = path
	l.segYear = now.Year()
	l.segMonth = int(now.Month())
	l.segNum = next
	l.segBytes = info.Size()
	return nil
}

// segments lists every segment file in commit order.
func (l *Ledger) segments() ([]string, error) {
	var out []string
	err := filepath.WalkDir(l.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if _, ok := segmentNumber(d.Name()); ok {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

func segmentName(n int) string { return fmt.Sprintf("segment-%06d.log", n) }

func segmentNumber(name string) (int, bool) {
	rest, ok := strings.CutPrefix(name, "segment-")
	if !ok {
		return 0, false
	}
	digits, ok := strings.CutSuffix(rest, ".log")
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(digits)
	if err != nil {
		return 0, false
	}
	return n, true
}

// scanFile reads one segment.
//
// A trailing chunk that does not end in a newline is a torn write. With
// tolerateTornTail it is reported and skipped; without, it is an error, because
// a torn record in the middle of the log means history is damaged.
func scanFile(path string, tolerateTornTail bool, fn func(Record) error) (tornBytes int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 256<<10)
	line := 0
	for {
		raw, readErr := reader.ReadBytes('\n')
		if len(raw) == 0 && readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return 0, nil
			}
			return 0, readErr
		}
		line++
		complete := raw[len(raw)-1] == '\n'
		if !complete {
			if tolerateTornTail {
				return int64(len(raw)), nil
			}
			return int64(len(raw)), fmt.Errorf("%s line %d: record ends mid-line (%d bytes without a terminator)", path, line, len(raw))
		}
		if len(raw) > 16<<20 {
			return 0, fmt.Errorf("%s line %d: record is %d bytes, over the 16 MiB limit", path, line, len(raw))
		}
		trimmed := strings.TrimSpace(string(raw))
		if trimmed == "" {
			continue
		}
		rec, err := decodeRecordLine([]byte(trimmed))
		if err != nil {
			return 0, fmt.Errorf("%s line %d: %w", path, line, err)
		}
		if err := fn(rec); err != nil {
			// Name the place. When verification fails, the operator needs the
			// segment and the record, not just "the ledger is broken".
			return 0, fmt.Errorf("%s line %d: %w", path, line, err)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return 0, nil
			}
			return 0, readErr
		}
	}
}

// decodeRecordLine parses one ledger line. It is separate from the file reader
// so that it can be fuzzed directly.
func decodeRecordLine(raw []byte) (Record, error) {
	var rec Record
	if err := json.Unmarshal(raw, &rec); err != nil {
		return rec, err
	}
	return rec, nil
}

// truncateTail removes a torn tail from a segment.
func truncateTail(path string, tornBytes int64) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(info.Size() - tornBytes); err != nil {
		return err
	}
	return f.Sync()
}

func readTail(path string) (Record, int64, uint64, int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Record{}, 0, 0, 0, err
	}
	var last Record
	var n uint64
	torn, err := scanFile(path, true, func(rec Record) error {
		last = rec
		n++
		return nil
	})
	if err != nil {
		return Record{}, 0, 0, 0, err
	}
	if n == 0 {
		if torn > 0 {
			// The segment never held a complete record: it is entirely torn.
			return Record{}, info.Size(), 0, torn, nil
		}
		return Record{}, info.Size(), 0, 0, fmt.Errorf("ledger segment %s is empty", path)
	}
	return last, info.Size(), n, torn, nil
}

func countRecords(path string, tolerateTornTail bool) (uint64, int64, error) {
	var n uint64
	torn, err := scanFile(path, tolerateTornTail, func(Record) error {
		n++
		return nil
	})
	return n, torn, err
}
