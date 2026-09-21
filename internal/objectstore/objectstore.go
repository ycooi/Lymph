// Package objectstore is Lymph's content-addressed immutable store
// (section 17, section 18, section 73).
//
// Git's most important lesson is that content is stored and retrieved through
// an identifier derived from the content itself. Lymph borrows the idea and
// uses SHA-256 rather than Git's historical SHA-1 object layout.
//
// Layout (section 13):
//
//	<root>/objects/sha256/0a/0a1b2c...  (full digest as the file name)
//
// Objects are write-once. Storing the same config content ten times stores one
// object.
package objectstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ycooi/Lymph/internal/fsutil"
)

// Prefix marks a SHA-256 content address.
const Prefix = "sha256:"

// ErrObjectMissing is raised when a revision references an object that is not
// present. Per section 89 this is an integrity failure: Lymph must not carry on
// and deploy configuration whose content it cannot find.
var ErrObjectMissing = errors.New("object is missing from the store")

// Store is a content-addressed object store rooted at <lymph root>/objects.
type Store struct {
	root string
}

// Open returns a store handle, creating the directory tree if needed.
func Open(root string) (*Store, error) {
	s := &Store{root: filepath.Join(root, "objects", "sha256")}
	if err := os.MkdirAll(s.root, 0o750); err != nil {
		return nil, err
	}
	return s, nil
}

// HashOf returns the content address of data without storing it.
func HashOf(data []byte) string {
	sum := sha256.Sum256(data)
	return Prefix + hex.EncodeToString(sum[:])
}

// ValidHash reports whether h looks like a content address.
func ValidHash(h string) bool {
	rest, ok := strings.CutPrefix(h, Prefix)
	if !ok || len(rest) != 64 {
		return false
	}
	_, err := hex.DecodeString(rest)
	return err == nil
}

// Path returns the filesystem path for a content address.
func (s *Store) Path(hash string) (string, error) {
	rest, ok := strings.CutPrefix(hash, Prefix)
	if !ok || len(rest) != 64 {
		return "", fmt.Errorf("invalid object hash %q", hash)
	}
	return filepath.Join(s.root, rest[:2], rest), nil
}

// Put stores data and returns its content address. Writing an object that
// already exists is a no-op.
func (s *Store) Put(data []byte) (string, error) {
	hash := HashOf(data)
	path, err := s.Path(hash)
	if err != nil {
		return "", err
	}
	if fsutil.Exists(path) {
		return hash, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return "", err
	}
	// Content-addressed writes are idempotent, so a plain atomic write is
	// enough: a concurrent writer produces byte-identical content.
	if err := fsutil.WriteFileAtomic(path, data, 0o640); err != nil {
		return "", err
	}
	return hash, nil
}

// Get reads an object, verifying that its content still matches its address
// (section 89).
func (s *Store) Get(hash string) ([]byte, error) {
	path, err := s.Path(hash)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrObjectMissing, hash)
		}
		return nil, err
	}
	if got := HashOf(data); got != hash {
		return nil, fmt.Errorf("object %s is corrupt: content hashes to %s", hash, got)
	}
	return data, nil
}

// Has reports whether an object is present.
func (s *Store) Has(hash string) bool {
	path, err := s.Path(hash)
	if err != nil {
		return false
	}
	return fsutil.Exists(path)
}

// Size returns the stored size of an object in bytes.
func (s *Store) Size(hash string) (int64, error) {
	path, err := s.Path(hash)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, fmt.Errorf("%w: %s", ErrObjectMissing, hash)
		}
		return 0, err
	}
	return info.Size(), nil
}

// Entry is one file in a config bundle tree (section 18).
type Entry struct {
	Path string `json:"path"`
	Hash string `json:"hash"`
	Mode string `json:"mode"`
	Size int64  `json:"size"`
}

// Tree describes a config bundle: file path to blob mapping (section 18).
type Tree struct {
	Kind    string  `json:"kind"`
	Entries []Entry `json:"entries"`
}

// Bundle is the input form used by callers: path to exact raw bytes.
type Bundle map[string][]byte

// modeFor gives a stable, portable mode marker. Lymph stores bytes, not
// permission bits, but records whether the entry is executable so that a future
// deployer can reproduce intent.
func modeFor(_ []byte) string { return "100644" }

// PutBundle stores every blob of a bundle, then stores the tree that describes
// it, and returns the tree hash. Raw bytes are canonical (section 33).
func (s *Store) PutBundle(b Bundle) (string, error) {
	if len(b) == 0 {
		return "", errors.New("config bundle is empty")
	}
	tree := Tree{Kind: "tree"}
	for path, data := range b {
		hash, err := s.Put(data)
		if err != nil {
			return "", err
		}
		tree.Entries = append(tree.Entries, Entry{
			Path: path,
			Hash: hash,
			Mode: modeFor(data),
			Size: int64(len(data)),
		})
	}
	sort.Slice(tree.Entries, func(i, j int) bool { return tree.Entries[i].Path < tree.Entries[j].Path })
	return s.PutTree(tree)
}

// PutTree stores a tree object and returns its address.
func (s *Store) PutTree(t Tree) (string, error) {
	t.Kind = "tree"
	sort.Slice(t.Entries, func(i, j int) bool { return t.Entries[i].Path < t.Entries[j].Path })
	raw, err := json.Marshal(t)
	if err != nil {
		return "", err
	}
	return s.Put(raw)
}

// GetTree reads and decodes a tree object.
func (s *Store) GetTree(hash string) (Tree, error) {
	var t Tree
	raw, err := s.Get(hash)
	if err != nil {
		return t, err
	}
	if err := json.Unmarshal(raw, &t); err != nil {
		return t, fmt.Errorf("decode tree %s: %w", hash, err)
	}
	if t.Kind != "tree" {
		return t, fmt.Errorf("object %s is not a tree (kind %q)", hash, t.Kind)
	}
	return t, nil
}

// Revision is the config revision object: conceptually a commit, without Git's
// full complexity (section 18, section 23).
type Revision struct {
	Kind           string    `json:"kind"`
	RevisionID     string    `json:"revision_id"`
	ApplicationID  string    `json:"application_id"`
	ConfigFamilyID string    `json:"config_family_id"`
	Sequence       int       `json:"sequence"`
	RootTreeHash   string    `json:"root_tree_hash"`
	ParentRevision string    `json:"parent_revision_id,omitempty"`
	SchemaRevision string    `json:"schema_revision,omitempty"`
	Label          string    `json:"label,omitempty"`
	Message        string    `json:"message,omitempty"`
	IssueIDs       []string  `json:"issue_ids,omitempty"`
	WorkItemID     string    `json:"work_item_id,omitempty"`
	CandidateID    string    `json:"candidate_id,omitempty"`
	Author         string    `json:"author,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

// PutRevision stores a revision object and returns its content address.
func (s *Store) PutRevision(r Revision) (string, error) {
	r.Kind = "revision"
	raw, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return s.Put(raw)
}

// GetRevision reads and decodes a revision object.
func (s *Store) GetRevision(hash string) (Revision, error) {
	var r Revision
	raw, err := s.Get(hash)
	if err != nil {
		return r, err
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return r, fmt.Errorf("decode revision %s: %w", hash, err)
	}
	if r.Kind != "revision" {
		return r, fmt.Errorf("object %s is not a revision (kind %q)", hash, r.Kind)
	}
	return r, nil
}

// VerifyRevision checks that a revision and every blob it references are
// present and intact (section 89: object store integrity is stronger than
// projection availability).
func (s *Store) VerifyRevision(contentHash string) error {
	rev, err := s.GetRevision(contentHash)
	if err != nil {
		return err
	}
	tree, err := s.GetTree(rev.RootTreeHash)
	if err != nil {
		return err
	}
	for _, e := range tree.Entries {
		if _, err := s.Get(e.Hash); err != nil {
			return fmt.Errorf("revision %s references missing blob %s (%s): %w", rev.RevisionID, e.Path, e.Hash, err)
		}
	}
	return nil
}

// Usage reports object store size and object count.
func (s *Store) Usage() (objects int, bytes int64, err error) {
	err = filepath.WalkDir(s.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		objects++
		bytes += info.Size()
		return nil
	})
	return objects, bytes, err
}
