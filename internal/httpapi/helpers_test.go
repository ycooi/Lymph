package httpapi_test

import (
	"os/exec"
	"testing"

	"github.com/ycooi/Lymph/internal/engine"
)

// engineAt opens an engine over a store root for tests that need to inspect or
// seed state behind a running daemon.
func engineAt(t *testing.T, root string) *engine.Engine {
	t.Helper()
	e, err := engine.Open(engine.Options{Root: root, Logger: quiet()})
	if err != nil {
		t.Fatalf("open engine at %s: %v", root, err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

// pythonBinary locates python3, or returns "" so the caller can skip with a
// stated reason (mission section 67).
func pythonBinary() string {
	path, err := exec.LookPath("python3")
	if err != nil {
		return ""
	}
	return path
}

// execCommand builds a command in the repository root so the bundled Python
// client is importable.
func execCommand(t *testing.T, binary string, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(binary, args...)
	cmd.Dir = "../.."
	return cmd
}
