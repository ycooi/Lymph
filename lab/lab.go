// Package lab is the Lymph Lab: five deliberately unrelated fake applications
// driven through one unchanged daemon, plus the chaos, fuzz and soak tests that
// try to break it.
//
// Every test records a graded item. TestZZVerdict writes lab/REPORT.md and
// lab/report.json and computes the four independent verdicts from the
// acceptance gate in the test plan. A test that finds a defect fails the run; a
// test that finds an unimplemented capability records BLOCKED, which does not
// fail the run but does stop its verdict group from passing.
package lab

import (
	"fmt"
	"sync"
	"testing"
)

// Status grades one item.
type Status string

// Grades.
const (
	StatusPass    Status = "PASS"
	StatusFail    Status = "FAIL"
	StatusPartial Status = "PARTIAL"
	StatusBlocked Status = "BLOCKED"
	StatusSkipped Status = "SKIPPED"
)

// Item is one graded test-plan item.
type Item struct {
	ID       string `json:"id"`
	Spec     string `json:"spec"`
	Title    string `json:"title"`
	Status   Status `json:"status"`
	Evidence string `json:"evidence"`
	Detail   string `json:"detail,omitempty"`
	// Supporting holds evidence from the detailed tests behind this grade.
	Supporting []string `json:"supporting,omitempty"`
}

// Report accumulates items across the test binary's run.
type Report struct {
	mu    sync.Mutex
	items map[string]Item
	order []string
	// support holds evidence from detailed tests that belongs to an existing
	// graded item rather than a row of its own (mission section 79).
	support map[string][]string
}

// TheReport is the package-level report every lab test writes into.
var TheReport = &Report{items: map[string]Item{}, support: map[string][]string{}}

// Note attaches supporting evidence to an existing graded item. The detailed
// test still fails loudly if the property breaks; it just does not add a row to
// the summary.
//
// It mirrors the Passf signature so a detailed test reads the same way, with
// the parent item named first.
func Note(t *testing.T, parentID, label, spec, title, evidence string, args ...any) {
	t.Helper()
	_ = spec
	text := fmt.Sprintf("%s: %s — %s", label, title, fmt.Sprintf(evidence, args...))
	TheReport.mu.Lock()
	TheReport.support[parentID] = append(TheReport.support[parentID], text)
	TheReport.mu.Unlock()
	t.Logf("[NOTE → %s] %s", parentID, text)
}

// Supporting returns the evidence notes attached to one item.
func (r *Report) Supporting(id string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.support[id]...)
}

func (r *Report) record(item Item) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, seen := r.items[item.ID]; !seen {
		r.order = append(r.order, item.ID)
	}
	r.items[item.ID] = item
}

// Items returns the graded items in recording order.
func (r *Report) Items() []Item {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Item, 0, len(r.order))
	for _, id := range r.order {
		item := r.items[id]
		item.Supporting = append([]string(nil), r.support[id]...)
		out = append(out, item)
	}
	return out
}

// Counts summarises grades.
func (r *Report) Counts() map[Status]int {
	counts := map[Status]int{}
	for _, item := range r.Items() {
		counts[item.Status]++
	}
	return counts
}

// Passf records a passing item.
func Passf(t *testing.T, id, spec, title, evidence string, args ...any) {
	t.Helper()
	TheReport.record(Item{
		ID: id, Spec: spec, Title: title, Status: StatusPass,
		Evidence: fmt.Sprintf(evidence, args...),
	})
	t.Logf("[PASS] %s %s — %s", id, title, fmt.Sprintf(evidence, args...))
}

// Failf records a defect and fails the test.
func Failf(t *testing.T, id, spec, title, evidence string, args ...any) {
	t.Helper()
	detail := fmt.Sprintf(evidence, args...)
	TheReport.record(Item{ID: id, Spec: spec, Title: title, Status: StatusFail, Evidence: detail})
	t.Errorf("[FAIL] %s %s — %s", id, title, detail)
}

// Partialf records an item that holds for part of its stated claim.
func Partialf(t *testing.T, id, spec, title, evidence, missing string, args ...any) {
	t.Helper()
	TheReport.record(Item{
		ID: id, Spec: spec, Title: title, Status: StatusPartial,
		Evidence: fmt.Sprintf(evidence, args...), Detail: missing,
	})
	t.Logf("[PARTIAL] %s %s — %s (missing: %s)", id, title, fmt.Sprintf(evidence, args...), missing)
}

// Blockedf records a capability this build does not have yet. Not a defect, but
// it stops its verdict group from passing.
func Blockedf(t *testing.T, id, spec, title, reason string) {
	t.Helper()
	TheReport.record(Item{ID: id, Spec: spec, Title: title, Status: StatusBlocked, Detail: reason})
	t.Logf("[BLOCKED] %s %s — %s", id, title, reason)
}

// Skippedf records an item that was not exercised in this run.
func Skippedf(t *testing.T, id, spec, title, reason string) {
	t.Helper()
	TheReport.record(Item{ID: id, Spec: spec, Title: title, Status: StatusSkipped, Detail: reason})
	t.Logf("[SKIPPED] %s %s — %s", id, title, reason)
}
