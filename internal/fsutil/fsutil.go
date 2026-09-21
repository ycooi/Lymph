// Package fsutil holds the small filesystem primitives shared by the ledger,
// the object store and (later) the deployment adapters.
//
// Design references: section 29 atomic single-file install, section 30 staged
// bundle install. Those primitives are also used for the daemon's own
// bookkeeping files so that a crash never leaves a half-written record.
package fsutil

import (
	"fmt"
	"os"
	"path/filepath"
)

// SyncDir fsyncs a directory so that a rename or create inside it is durable.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// WriteFileAtomic writes data to path via a temporary file in the same
// directory, fsyncs it, then renames it into place. The parent directory is
// fsynced afterwards.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		// Best effort cleanup; a successful rename removes the file already.
		_ = os.Remove(tmpName)
	}()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return SyncDir(dir)
}

// Exists reports whether path exists.
func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// DirSize walks dir and returns the total size of regular files below it.
func DirSize(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("measure %s: %w", dir, err)
	}
	return total, nil
}
