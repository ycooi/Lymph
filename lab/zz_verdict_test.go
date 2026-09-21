package lab

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestZZVerdict writes the report and applies the acceptance gate.
//
// It runs last because Go runs tests in the order they appear in the files of a
// package, sorted by file name. A FAIL anywhere is a defect and fails the run; a
// BLOCKED item is a scope statement and shows up as a verdict that cannot pass.
func TestZZVerdict(t *testing.T) {
	counts := TheReport.Counts()
	jsonPath, mdPath, err := TheReport.Write(".")
	if err != nil {
		t.Fatalf("write report: %v", err)
	}
	abs, _ := filepath.Abs(mdPath)

	t.Logf("report: %s", abs)
	t.Logf("items: %d — PASS %d, FAIL %d, PARTIAL %d, BLOCKED %d, SKIPPED %d",
		len(TheReport.Items()), counts[StatusPass], counts[StatusFail],
		counts[StatusPartial], counts[StatusBlocked], counts[StatusSkipped])

	for _, verdict := range TheReport.Evaluate() {
		state := "NOT VALIDATED"
		if verdict.Passed {
			state = "VALIDATED"
		}
		t.Logf("%s: %s", verdict.Name, state)
		for _, missing := range verdict.Missing {
			t.Logf("    not satisfied: %s", missing)
		}
		for _, blocker := range verdict.Blockers {
			t.Logf("    blocked: %s", blocker)
		}
	}
	t.Logf("%s: %s", FinalGate, map[bool]string{true: "READY", false: "NOT READY"}[TheReport.Ready()])

	for _, verdict := range TheReport.EvaluateProtocol() {
		state := "NOT VALIDATED"
		if verdict.Passed {
			state = "VALIDATED"
		}
		t.Logf("%s: %s", verdict.Name, state)
		for _, missing := range verdict.Missing {
			t.Logf("    not satisfied: %s", missing)
		}
	}
	t.Logf("CLIENT_PROTOCOL_READY: %s",
		map[bool]string{true: "READY", false: "NOT READY"}[TheReport.ProtocolReady()])

	if counts[StatusFail] > 0 {
		t.Errorf("%d item(s) failed; see %s and %s", counts[StatusFail], jsonPath, abs)
	}

	// A run that produced no items means the suite did not execute, which is
	// itself a failure of the harness.
	if len(TheReport.Items()) < 20 {
		t.Fatalf("only %d items were graded; the lab did not run to completion", len(TheReport.Items()))
	}

	// Keep the report visible even in a passing run.
	if os.Getenv("LYMPH_LAB_PRINT_REPORT") != "" {
		fmt.Println(TheReport.Markdown())
	}
}
