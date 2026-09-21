package lab

import (
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// RuntimeStats are the leak indicators the soak test watches.
type RuntimeStats struct {
	Goroutines int
	OpenFiles  int
	HeapMB     float64
}

// runtimeStats samples the process. It is used to spot leaks, not to profile.
func runtimeStats() RuntimeStats {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	open := 0
	if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
		open = len(entries)
	}
	return RuntimeStats{
		Goroutines: runtime.NumGoroutine(),
		OpenFiles:  open,
		HeapMB:     float64(mem.HeapAlloc) / (1 << 20),
	}
}

// percentile returns the p-quantile of a set of durations.
func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(math.Ceil(p*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

// copyTree copies a file or directory tree, preserving structure.
func copyTree(from, to string) error {
	info, err := os.Stat(from)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return copyFile(from, to, info.Mode())
	}
	return filepath.WalkDir(from, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		fileInfo, err := entry.Info()
		if err != nil {
			return err
		}
		return copyFile(path, target, fileInfo.Mode())
	})
}

func copyFile(from, to string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(to), 0o750); err != nil {
		return err
	}
	source, err := os.Open(from)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.OpenFile(to, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode.Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(destination, source); err != nil {
		destination.Close()
		return err
	}
	if err := destination.Sync(); err != nil {
		destination.Close()
		return err
	}
	return destination.Close()
}

// removeProjectionFiles deletes the disposable SQLite projection, leaving the
// canonical ledger and object store untouched.
func removeProjectionFiles(root string) error {
	dbPath := filepath.Join(root, "db", "lymph.sqlite")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(dbPath + suffix); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// diffTables names the tables whose per-table digests differ.
func diffTables(before, after map[string]string) string {
	var differing []string
	for name, value := range before {
		if after[name] != value {
			differing = append(differing, name)
		}
	}
	for name := range after {
		if _, seen := before[name]; !seen {
			differing = append(differing, name)
		}
	}
	sort.Strings(differing)
	if len(differing) == 0 {
		return "none (the difference is in table ordering)"
	}
	return strings.Join(differing, ", ")
}
