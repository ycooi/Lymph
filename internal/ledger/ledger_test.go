package ledger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAppendVerifyAndReopen(t *testing.T) {
	root := t.TempDir()
	l, err := Open(root, Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := l.Append(KindEvent, "object", map[string]any{"i": i}, Durable); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	report, err := l.Verify()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if report.Records != 5 || report.LastSeq != 5 {
		t.Fatalf("unexpected report: %+v", report)
	}
	head := report.HeadHash
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopening must continue the existing chain, not start a new one.
	reopened, err := Open(root, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if reopened.LastSequence() != 5 {
		t.Fatalf("expected to resume at sequence 5, got %d", reopened.LastSequence())
	}
	rec, err := reopened.Append(KindAudit, "object", map[string]any{"i": 5}, Durable)
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if rec.Sequence != 6 {
		t.Fatalf("expected sequence 6, got %d", rec.Sequence)
	}
	if rec.PrevHash != head {
		t.Fatalf("chain not continued: previous hash %q, expected %q", rec.PrevHash, head)
	}
	if _, err := reopened.Verify(); err != nil {
		t.Fatalf("verify after reopen: %v", err)
	}
}

func TestVerifyDetectsTampering(t *testing.T) {
	root := t.TempDir()
	l, err := Open(root, Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := l.Append(KindEvent, "object", map[string]any{"i": i}, Durable); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Rewrite one record's payload in place: the chain must notice.
	segments, err := filepath.Glob(filepath.Join(root, "ledger", "*", "*", "segment-*.log"))
	if err != nil || len(segments) != 1 {
		t.Fatalf("expected one segment, got %v (%v)", segments, err)
	}
	raw, err := os.ReadFile(segments[0])
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	tampered := strings.Replace(string(raw), `"i":1`, `"i":9`, 1)
	if tampered == string(raw) {
		t.Fatalf("tamper injection did not change the file")
	}
	if err := os.WriteFile(segments[0], []byte(tampered), 0o640); err != nil {
		t.Fatalf("write tampered segment: %v", err)
	}

	reopened, err := Open(root, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.Verify(); err == nil {
		t.Fatal("expected verification to fail after tampering, but it passed")
	}
}

func TestSegmentRotationKeepsOrder(t *testing.T) {
	root := t.TempDir()
	l, err := Open(root, Options{MaxSegmentBytes: 200})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 50; i++ {
		if _, err := l.Append(KindEvent, "object", map[string]any{"payload": strings.Repeat("x", 20), "i": i}, Durable); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	segments, err := filepath.Glob(filepath.Join(root, "ledger", "*", "*", "segment-*.log"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(segments) < 2 {
		t.Fatalf("expected rotation into multiple segments, got %d", len(segments))
	}
	reopened, err := Open(root, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	report, err := reopened.Verify()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if report.Records != 50 {
		t.Fatalf("expected 50 records across segments, got %d", report.Records)
	}
	if reopened.LastSequence() != 50 {
		t.Fatalf("expected to resume at 50, got %d", reopened.LastSequence())
	}
}

// TestTornTailIsRecovered covers the crash-mid-append case: a record that was
// being written when the process died is not history, and the next append must
// continue the chain as if it had never started.
func TestTornTailIsRecovered(t *testing.T) {
	root := t.TempDir()
	l, err := Open(root, Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := l.Append(KindEvent, "object", map[string]any{"i": i}, Durable); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	segments, err := filepath.Glob(filepath.Join(root, "ledger", "*", "*", "segment-*.log"))
	if err != nil || len(segments) != 1 {
		t.Fatalf("expected one segment, got %v (%v)", segments, err)
	}
	torn := []byte(`{"sequence":4,"kind":"EVENT","object_id":"x","timestamp":"2026-`)
	f, err := os.OpenFile(segments[0], os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err := f.Write(torn); err != nil {
		t.Fatalf("write torn record: %v", err)
	}
	f.Close()

	reopened, err := Open(root, Options{})
	if err != nil {
		t.Fatalf("reopen after torn tail: %v", err)
	}
	defer reopened.Close()
	if got := reopened.TornTailBytes(); got != int64(len(torn)) {
		t.Fatalf("recovered %d torn bytes, expected %d", got, len(torn))
	}
	if reopened.LastSequence() != 3 {
		t.Fatalf("expected to resume at 3, got %d", reopened.LastSequence())
	}
	rec, err := reopened.Append(KindEvent, "object", map[string]any{"i": 3}, Durable)
	if err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if rec.Sequence != 4 {
		t.Fatalf("expected sequence 4 after recovery, got %d", rec.Sequence)
	}
	report, err := reopened.Verify()
	if err != nil {
		t.Fatalf("verify after recovery: %v", err)
	}
	if report.Records != 4 || report.TornTailBytes != 0 {
		t.Fatalf("unexpected report after recovery: %+v", report)
	}
}

// TestEmptyNewestSegmentIsRecovered covers a crash after segment creation and
// directory sync but before the first record reaches the new segment.
func TestEmptyNewestSegmentIsRecovered(t *testing.T) {
	root := t.TempDir()
	l, err := Open(root, Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	first, err := l.Append(KindEvent, "object", map[string]any{"i": 1}, Durable)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	segments, err := filepath.Glob(filepath.Join(root, "ledger", "*", "*", "segment-*.log"))
	if err != nil || len(segments) != 1 {
		t.Fatalf("expected one segment, got %v (%v)", segments, err)
	}
	empty := filepath.Join(filepath.Dir(segments[0]), segmentName(2))
	f, err := os.OpenFile(empty, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		t.Fatalf("create empty tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close empty tail: %v", err)
	}

	reopened, err := Open(root, Options{})
	if err != nil {
		t.Fatalf("reopen with empty tail: %v", err)
	}
	defer reopened.Close()
	if reopened.LastSequence() != first.Sequence {
		t.Fatalf("resumed at %d, expected %d", reopened.LastSequence(), first.Sequence)
	}
	if _, err := os.Stat(empty); !os.IsNotExist(err) {
		t.Fatalf("empty tail was not removed: %v", err)
	}
	next, err := reopened.Append(KindEvent, "object", map[string]any{"i": 2}, Durable)
	if err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if next.Sequence != first.Sequence+1 {
		t.Fatalf("continued at %d, expected %d", next.Sequence, first.Sequence+1)
	}
	if _, err := reopened.Verify(); err != nil {
		t.Fatalf("verify after recovery: %v", err)
	}
}

func TestEmptyMiddleSegmentIsRejected(t *testing.T) {
	root := t.TempDir()
	l, err := Open(root, Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := l.Append(KindEvent, "object", map[string]any{"i": 1}, Durable); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	segments, err := filepath.Glob(filepath.Join(root, "ledger", "*", "*", "segment-*.log"))
	if err != nil || len(segments) != 1 {
		t.Fatalf("expected one segment, got %v (%v)", segments, err)
	}
	dir := filepath.Dir(segments[0])
	if err := os.WriteFile(filepath.Join(dir, segmentName(2)), nil, 0o640); err != nil {
		t.Fatalf("create empty middle segment: %v", err)
	}
	raw, err := os.ReadFile(segments[0])
	if err != nil {
		t.Fatalf("read source segment: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, segmentName(3)), raw, 0o640); err != nil {
		t.Fatalf("create later segment: %v", err)
	}

	if _, err := Open(root, Options{}); err == nil || !strings.Contains(err.Error(), "empty in the middle") {
		t.Fatalf("expected empty-middle rejection, got %v", err)
	}
}

// TestFailedAppendLeavesNoPartialRecord covers the write-error case: a mutation
// that could not be written must leave nothing behind, so the next mutation
// does not inherit a corrupt chain.
func TestFailedAppendLeavesNoPartialRecord(t *testing.T) {
	root := t.TempDir()
	l, err := Open(root, Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := l.Append(KindEvent, "object", map[string]any{"i": i}, Durable); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Simulate the filesystem disappearing under the daemon.
	if err := l.file.Close(); err != nil {
		t.Fatalf("close underlying file: %v", err)
	}
	if _, err := l.Append(KindEvent, "object", map[string]any{"i": 2}, Durable); err == nil {
		t.Fatal("append to a closed segment must fail")
	}
	if l.LastSequence() != 2 {
		t.Fatalf("failed append advanced the sequence to %d", l.LastSequence())
	}

	// What is on disk is still exactly the two accepted records.
	reopened, err := Open(root, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	report, err := reopened.Verify()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if report.Records != 2 {
		t.Fatalf("expected 2 records on disk, got %d", report.Records)
	}
	rec, err := reopened.Append(KindEvent, "object", map[string]any{"i": 2}, Durable)
	if err != nil {
		t.Fatalf("append after failure: %v", err)
	}
	if rec.Sequence != 3 {
		t.Fatalf("expected the chain to continue at 3, got %d", rec.Sequence)
	}
}
