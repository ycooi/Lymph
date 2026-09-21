package objectstore

import (
	"errors"
	"testing"
	"time"
)

func TestContentAddressingAndDeduplication(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	first, err := s.Put([]byte("alpha: 1\n"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	second, err := s.Put([]byte("alpha: 1\n"))
	if err != nil {
		t.Fatalf("put again: %v", err)
	}
	if first != second {
		t.Fatalf("identical content produced different addresses: %s vs %s", first, second)
	}
	other, err := s.Put([]byte("alpha: 2\n"))
	if err != nil {
		t.Fatalf("put other: %v", err)
	}
	if other == first {
		t.Fatal("different content produced the same address")
	}
	got, err := s.Get(first)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "alpha: 1\n" {
		t.Fatalf("unexpected content %q", got)
	}
	objects, _, err := s.Usage()
	if err != nil {
		t.Fatalf("usage: %v", err)
	}
	if objects != 2 {
		t.Fatalf("expected 2 stored objects, got %d", objects)
	}
}

func TestMissingObjectIsIntegrityFailure(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err = s.Get("sha256:" + "0000000000000000000000000000000000000000000000000000000000000000")
	if !errors.Is(err, ErrObjectMissing) {
		t.Fatalf("expected ErrObjectMissing, got %v", err)
	}
}

func TestRevisionReferencesEveryBlob(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	tree, err := s.PutBundle(Bundle{
		"events.yaml":  []byte("events: []\n"),
		"aliases.yaml": []byte("aliases: []\n"),
	})
	if err != nil {
		t.Fatalf("put bundle: %v", err)
	}
	rev := Revision{
		RevisionID:     "019a0000-0000-7000-8000-000000000001",
		ApplicationID:  "019a0000-0000-7000-8000-000000000002",
		ConfigFamilyID: "019a0000-0000-7000-8000-000000000003",
		Sequence:       1,
		RootTreeHash:   tree,
		CreatedAt:      time.Now().UTC(),
	}
	hash, err := s.PutRevision(rev)
	if err != nil {
		t.Fatalf("put revision: %v", err)
	}
	if err := s.VerifyRevision(hash); err != nil {
		t.Fatalf("verify revision: %v", err)
	}
	loaded, err := s.GetRevision(hash)
	if err != nil {
		t.Fatalf("get revision: %v", err)
	}
	if loaded.RootTreeHash != tree {
		t.Fatalf("tree hash mismatch: %s vs %s", loaded.RootTreeHash, tree)
	}
}
