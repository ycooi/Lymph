// Package identity implements the identity model of Lymph Architecture 001.
//
// Design references:
//   - section 4: Identity Model - UUIDv7 for every persistent object, names are metadata
//   - section 5: Lymph Instance Identity - identity.json, never silently regenerated
//   - section 78: producer instance UUID, section 79: installation UUID
package identity

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

// FormatVersion is the on-disk identity file format.
const FormatVersion = 1

// ErrMissingInstanceIdentity is returned when the store exists but the instance
// identity file does not. Section 5 is explicit: this is a startup failure,
// never a silent re-generation, because a new UUID would make the existing
// store look like it belonged to a different installation.
var ErrMissingInstanceIdentity = errors.New("identity.json is missing but a lymph store exists: refusing to regenerate instance identity")

// File is the contents of identity.json.
type File struct {
	InstanceUUID  string    `json:"instance_uuid"`
	CreatedAt     time.Time `json:"created_at"`
	FormatVersion int       `json:"format_version"`
}

// NewID returns a new UUIDv7 as a canonical string.
func NewID() string {
	id, err := uuid.NewV7()
	if err != nil {
		// NewV7 only fails if the system entropy source fails. Falling back to
		// v4 keeps the daemon alive; identity is still globally unique.
		return uuid.NewString()
	}
	return id.String()
}

// Valid reports whether s parses as a UUID.
func Valid(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil
}

// New builds a fresh identity file value.
func New() File {
	return File{
		InstanceUUID:  NewID(),
		CreatedAt:     time.Now().UTC(),
		FormatVersion: FormatVersion,
	}
}

// Path returns the identity file path for a lymph root directory.
func Path(root string) string { return filepath.Join(root, "identity.json") }

// Load reads the instance identity for a root.
func Load(root string) (File, error) {
	raw, err := os.ReadFile(Path(root))
	if err != nil {
		return File{}, err
	}
	var f File
	if err := json.Unmarshal(raw, &f); err != nil {
		return File{}, fmt.Errorf("parse identity.json: %w", err)
	}
	if !Valid(f.InstanceUUID) {
		return File{}, fmt.Errorf("identity.json contains invalid instance_uuid %q", f.InstanceUUID)
	}
	return f, nil
}

// Ensure loads the instance identity, creating it on first initialization.
//
// If the identity file is absent but the store already holds data, Ensure
// returns ErrMissingInstanceIdentity instead of creating a new instance UUID.
func Ensure(root string) (File, bool, error) {
	f, err := Load(root)
	if err == nil {
		return f, false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return File{}, false, err
	}

	existing, statErr := storeExists(root)
	if statErr != nil {
		return File{}, false, statErr
	}
	if existing {
		return File{}, false, ErrMissingInstanceIdentity
	}

	f = New()
	if err := write(root, f); err != nil {
		return File{}, false, err
	}
	return f, true, nil
}

func write(root string, f File) error {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	tmp := Path(root) + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o640); err != nil {
		return err
	}
	if err := os.Rename(tmp, Path(root)); err != nil {
		return err
	}
	return syncDir(root)
}

// storeExists reports whether a lymph store with content already lives at root.
func storeExists(root string) (bool, error) {
	db := filepath.Join(root, "db", "lymph.sqlite")
	if st, err := os.Stat(db); err == nil && st.Size() > 0 {
		return true, nil
	}
	found := false
	err := filepath.WalkDir(filepath.Join(root, "ledger"), func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if !d.IsDir() && filepath.Ext(d.Name()) == ".log" {
			if info, statErr := d.Info(); statErr == nil && info.Size() > 0 {
				found = true
				return filepath.SkipAll
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
