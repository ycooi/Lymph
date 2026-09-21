package client

import (
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"testing"

	"github.com/ycooi/Lymph/internal/objectstore"
)

// The node verifies delivered content with its own code. These tests are what
// make that claim true rather than decorative: the SDK's tree hash must agree
// with the daemon's, in Go and in Python, and a single flipped byte must be
// refused (mission section 29).

func testBundle() objectstore.Bundle {
	return objectstore.Bundle{
		"rules.yaml": []byte("rules:\n  - phrase: baseline\n  - phrase: booked\n"),
		"meta.json":  []byte("{\"schema\":\"rules-v1\"}\n"),
	}
}

// manifestOf stores a bundle in a real object store and returns the manifest
// the daemon would send.
func manifestOf(t *testing.T, bundle objectstore.Bundle) (string, []ManifestEntry) {
	t.Helper()
	store, err := objectstore.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open object store: %v", err)
	}
	treeHash, err := store.PutBundle(bundle)
	if err != nil {
		t.Fatalf("put bundle: %v", err)
	}
	tree, err := store.GetTree(treeHash)
	if err != nil {
		t.Fatalf("get tree: %v", err)
	}
	entries := make([]ManifestEntry, 0, len(tree.Entries))
	for _, entry := range tree.Entries {
		entries = append(entries, ManifestEntry{
			Path: entry.Path, Hash: entry.Hash, Mode: entry.Mode, Size: entry.Size,
		})
	}
	return treeHash, entries
}

// TestTreeHashAgreesWithTheDaemon: if the client could not reproduce the
// daemon's address, "verify locally" would mean "trust the daemon".
func TestTreeHashAgreesWithTheDaemon(t *testing.T) {
	daemonHash, manifest := manifestOf(t, testBundle())
	clientHash, err := treeHash(manifest)
	if err != nil {
		t.Fatalf("tree hash: %v", err)
	}
	if clientHash != daemonHash {
		t.Fatalf("client computed %s, the daemon stored %s", clientHash, daemonHash)
	}
}

// TestPythonTreeHashAgreesWithGo is the cross-language leg: a Python node must
// be able to verify a bundle it fetched from the same daemon (mission
// section 59).
func TestPythonTreeHashAgreesWithGo(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is unavailable: the cross-language hash leg is skipped, not hidden")
	}
	daemonHash, manifest := manifestOf(t, testBundle())
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}

	script := `
import json, sys
sys.path.insert(0, "examples/python")
from lymph_client import tree_hash
print(tree_hash(json.loads(sys.argv[1])))
`
	cmd := exec.Command(python, "-c", script, string(raw))
	cmd.Dir = "../.."
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("run python: %v", err)
	}
	got := strings.TrimSpace(string(out))
	if got != daemonHash {
		t.Fatalf("python computed %s, the daemon stored %s", got, daemonHash)
	}
}

// validContent builds the content a fetch would return for a real bundle.
func validContent(t *testing.T) *UpdateContent {
	t.Helper()
	bundle := testBundle()
	treeHash, manifest := manifestOf(t, bundle)
	files := make(map[string][]byte, len(bundle))
	for path, data := range bundle {
		files[path] = data
	}
	return &UpdateContent{
		Update: ApprovedUpdate{
			ConfigFamilyID: "rules", RevisionID: "r2", ContentHash: treeHash,
		},
		RootTreeHash: treeHash,
		Manifest:     manifest,
		Bundle:       files,
	}
}

func TestVerifyContentAcceptsTheApprovedBytes(t *testing.T) {
	if err := verifyContent(validContent(t)); err != nil {
		t.Fatalf("an untampered bundle failed verification: %v", err)
	}
}

func TestVerifyContentRefusesAFlip(t *testing.T) {
	content := validContent(t)
	content.Bundle["rules.yaml"] = []byte("rules:\n  - phrase: tampered\n")
	if err := verifyContent(content); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("a tampered blob was accepted: %v", err)
	}
}

func TestVerifyContentRefusesAMissingFile(t *testing.T) {
	content := validContent(t)
	delete(content.Bundle, "meta.json")
	if err := verifyContent(content); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("a bundle missing a manifest entry was accepted: %v", err)
	}
}

func TestVerifyContentRefusesASwappedManifest(t *testing.T) {
	content := validContent(t)
	// The blobs are all individually valid; only the tree is not the approved
	// one. This is the "modified content reusing an old approval" case.
	content.RootTreeHash = "sha256:" + strings.Repeat("0", 64)
	if err := verifyContent(content); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("a bundle whose tree hash does not match the approval was accepted: %v", err)
	}
}
