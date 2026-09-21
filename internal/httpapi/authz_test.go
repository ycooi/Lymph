package httpapi

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ycooi/Lymph/internal/engine"
)

func writeACL(t *testing.T, body string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "acl.json")
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatalf("write ACL: %v", err)
	}
	return path
}

func TestLoadACLAcceptsRootAsAnExactUID(t *testing.T) {
	acl, err := LoadACL(writeACL(t,
		`{"principals":[{"uid":0,"roles":["OPERATOR"]}]}`, 0o600))
	if err != nil {
		t.Fatalf("load ACL: %v", err)
	}
	root := acl.Resolve(engine.PeerInfo{UID: 0, GID: 42, Supported: true})
	if !root.Has(RoleOperator) || root.Mode != ModeACL {
		t.Fatalf("root principal was not matched exactly: %+v", root)
	}
	nonRoot := acl.Resolve(engine.PeerInfo{UID: 1000, GID: 42, Supported: true})
	if nonRoot.Has(RoleOperator) {
		t.Fatalf("uid 0 acted as a wildcard: %+v", nonRoot)
	}
}

func TestLoadACLRejectsPrincipalWithoutSelector(t *testing.T) {
	_, err := LoadACL(writeACL(t,
		`{"principals":[{"roles":["OPERATOR"]}]}`, 0o600))
	if err == nil || !strings.Contains(err.Error(), "must set uid or gid") {
		t.Fatalf("expected missing-selector refusal, got %v", err)
	}
}

func TestLoadACLRejectsNegativeIDs(t *testing.T) {
	_, err := LoadACL(writeACL(t,
		`{"principals":[{"uid":-1,"roles":["OPERATOR"]}]}`, 0o600))
	if err == nil || !strings.Contains(err.Error(), "negative uid") {
		t.Fatalf("expected negative uid refusal, got %v", err)
	}
}

func TestLoadACLRejectsUnknownFields(t *testing.T) {
	_, err := LoadACL(writeACL(t,
		`{"principals":[],"default_role":["OPERATOR"]}`, 0o600))
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected strict JSON refusal, got %v", err)
	}
}

func TestLoadACLRejectsWritablePolicy(t *testing.T) {
	path := writeACL(t, `{"principals":[]}`, 0o622)
	if err := os.Chmod(path, 0o622); err != nil {
		t.Fatalf("chmod ACL: %v", err)
	}
	_, err := LoadACL(path)
	if err == nil || !strings.Contains(err.Error(), "writable by group or others") {
		t.Fatalf("expected writable-policy refusal, got %v", err)
	}
}

func TestLoadACLRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.json")
	if err := os.WriteFile(target, []byte(`{"principals":[]}`), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	link := filepath.Join(dir, "acl.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	_, err := LoadACL(link)
	if err == nil || !strings.Contains(err.Error(), "must not be a symbolic link") {
		t.Fatalf("expected symlink refusal, got %v", err)
	}
}
