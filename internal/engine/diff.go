package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/projection"
)

// Lineage is the ancestor chain of a revision, newest first.
//
// The test plan asks: "show lineage r713" should reconstruct r710 -> r711 ->
// r713, with r712 possibly a failed branch. That works because every revision
// records its parent and every branch is preserved (section 73: complete
// snapshots, not diffs).
func (e *Engine) Lineage(ctx context.Context, revisionID string, limit int) ([]projection.ConfigRevision, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	var chain []projection.ConfigRevision
	seen := map[string]bool{}

	current := revisionID
	for current != "" && len(chain) < limit {
		if seen[current] {
			return chain, fmt.Errorf("revision lineage contains a cycle at %s", current)
		}
		seen[current] = true

		rev, err := e.db.GetRevision(current)
		if err != nil {
			return chain, err
		}
		chain = append(chain, rev)
		current = rev.ParentRevisionID
	}
	return chain, nil
}

// FileDiff is one file's change between two revisions.
type FileDiff struct {
	Path    string `json:"path"`
	Status  string `json:"status"` // ADDED, REMOVED or MODIFIED
	OldHash string `json:"old_hash,omitempty"`
	NewHash string `json:"new_hash,omitempty"`
	OldSize int64  `json:"old_size,omitempty"`
	NewSize int64  `json:"new_size,omitempty"`
	Patch   string `json:"patch,omitempty"`
}

// DiffResult compares two immutable revisions.
type DiffResult struct {
	From  projection.ConfigRevision `json:"from"`
	To    projection.ConfigRevision `json:"to"`
	Files []FileDiff                `json:"files"`
	Notes []string                  `json:"notes,omitempty"`
}

// maxDiffLines bounds the LCS table. Config files are small; anything larger
// gets a summary instead of a patch so the daemon never allocates a matrix
// proportional to a mistake.
const maxDiffLines = 2000

// Diff compares two revisions by file and, for text files, by line.
func (e *Engine) Diff(ctx context.Context, fromRevisionID, toRevisionID string) (DiffResult, error) {
	from, err := e.db.GetRevision(fromRevisionID)
	if err != nil {
		return DiffResult{}, err
	}
	to, err := e.db.GetRevision(toRevisionID)
	if err != nil {
		return DiffResult{}, err
	}
	if from.ConfigFamilyID != to.ConfigFamilyID {
		return DiffResult{}, fmt.Errorf("revisions belong to different config families (%s and %s)",
			from.ConfigFamilyID, to.ConfigFamilyID)
	}

	fromBlobs, err := e.db.RevisionBlobs(from.RevisionID)
	if err != nil {
		return DiffResult{}, err
	}
	toBlobs, err := e.db.RevisionBlobs(to.RevisionID)
	if err != nil {
		return DiffResult{}, err
	}

	fromIndex := indexBlobs(fromBlobs)
	toIndex := indexBlobs(toBlobs)

	result := DiffResult{From: from, To: to}

	for _, path := range unionPaths(fromIndex, toIndex) {
		before, hadBefore := fromIndex[path]
		after, hadAfter := toIndex[path]

		switch {
		case hadBefore && !hadAfter:
			result.Files = append(result.Files, FileDiff{Path: path, Status: "REMOVED",
				OldHash: before.Hash, OldSize: before.Size})
		case !hadBefore && hadAfter:
			result.Files = append(result.Files, FileDiff{Path: path, Status: "ADDED",
				NewHash: after.Hash, NewSize: after.Size})
		case before.Hash == after.Hash:
			continue
		default:
			file := FileDiff{Path: path, Status: "MODIFIED",
				OldHash: before.Hash, NewHash: after.Hash, OldSize: before.Size, NewSize: after.Size}
			patch, note := e.textPatch(path, before.Hash, after.Hash)
			file.Patch = patch
			if note != "" {
				result.Notes = append(result.Notes, note)
			}
			result.Files = append(result.Files, file)
		}
	}
	return result, nil
}

func indexBlobs(entries []objectstore.Entry) map[string]objectstore.Entry {
	out := make(map[string]objectstore.Entry, len(entries))
	for _, entry := range entries {
		out[entry.Path] = entry
	}
	return out
}

func unionPaths(from, to map[string]objectstore.Entry) []string {
	seen := map[string]bool{}
	var paths []string
	for path := range from {
		if !seen[path] {
			seen[path] = true
			paths = append(paths, path)
		}
	}
	for path := range to {
		if !seen[path] {
			seen[path] = true
			paths = append(paths, path)
		}
	}
	sortStrings(paths)
	return paths
}

// textPatch renders a unified diff for one changed file, or returns a note
// explaining why it did not.
func (e *Engine) textPatch(path, oldHash, newHash string) (string, string) {
	oldBytes, err := e.objects.Get(oldHash)
	if err != nil {
		return "", fmt.Sprintf("%s: previous content unavailable (%v)", path, err)
	}
	newBytes, err := e.objects.Get(newHash)
	if err != nil {
		return "", fmt.Sprintf("%s: new content unavailable (%v)", path, err)
	}
	if isBinary(oldBytes) || isBinary(newBytes) {
		return "", fmt.Sprintf("%s: binary content changed", path)
	}

	oldLines := splitLines(string(oldBytes))
	newLines := splitLines(string(newBytes))
	if len(oldLines) > maxDiffLines || len(newLines) > maxDiffLines {
		return "", fmt.Sprintf("%s: %d/%d lines exceeds the %d line diff limit",
			path, len(oldLines), len(newLines), maxDiffLines)
	}
	return unifiedDiff(path, oldLines, newLines), ""
}

func isBinary(data []byte) bool {
	for _, b := range data {
		if b == 0 {
			return true
		}
	}
	return false
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	s = strings.TrimSuffix(s, "\n")
	return strings.Split(s, "\n")
}

type diffOp struct {
	kind byte // ' ', '-', '+'
	text string
}

// unifiedDiff produces a conventional unified diff with three lines of context.
func unifiedDiff(path string, oldLines, newLines []string) string {
	ops := lineDiff(oldLines, newLines)

	var b strings.Builder
	fmt.Fprintf(&b, "--- a/%s\n+++ b/%s\n", path, path)

	const context = 3
	i := 0
	for i < len(ops) {
		if ops[i].kind == ' ' {
			i++
			continue
		}
		start := i - context
		if start < 0 {
			start = 0
		}
		end := i
		lastChange := i
		for end < len(ops) {
			if ops[end].kind != ' ' {
				lastChange = end
				end++
				continue
			}
			// Close the hunk when the gap exceeds twice the context.
			gap := 0
			for end+gap < len(ops) && ops[end+gap].kind == ' ' {
				gap++
			}
			if gap > context*2 {
				break
			}
			end += gap
			lastChange = end - 1
		}
		finish := lastChange + context + 1
		if finish > len(ops) {
			finish = len(ops)
		}

		oldStart, oldCount, newStart, newCount := hunkRange(ops, start, finish)
		fmt.Fprintf(&b, "@@ -%d,%d +%d,%d @@\n", oldStart, oldCount, newStart, newCount)
		for _, op := range ops[start:finish] {
			b.WriteByte(op.kind)
			b.WriteString(op.text)
			b.WriteByte('\n')
		}
		i = finish
	}
	return b.String()
}

func hunkRange(ops []diffOp, start, end int) (oldStart, oldCount, newStart, newCount int) {
	oldLine, newLine := 1, 1
	for i := 0; i < start; i++ {
		if ops[i].kind != '+' {
			oldLine++
		}
		if ops[i].kind != '-' {
			newLine++
		}
	}
	oldStart, newStart = oldLine, newLine
	for _, op := range ops[start:end] {
		if op.kind != '+' {
			oldCount++
		}
		if op.kind != '-' {
			newCount++
		}
	}
	return oldStart, oldCount, newStart, newCount
}

// lineDiff is a plain LCS diff. Deterministic, no heuristics, no library.
func lineDiff(a, b []string) []diffOp {
	n, m := len(a), len(b)
	table := make([][]int, n+1)
	for i := range table {
		table[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				table[i][j] = table[i+1][j+1] + 1
			} else if table[i+1][j] >= table[i][j+1] {
				table[i][j] = table[i+1][j]
			} else {
				table[i][j] = table[i][j+1]
			}
		}
	}

	var ops []diffOp
	i, j := 0, 0
	for i < n && j < m {
		switch {
		case a[i] == b[j]:
			ops = append(ops, diffOp{' ', a[i]})
			i++
			j++
		case table[i+1][j] >= table[i][j+1]:
			ops = append(ops, diffOp{'-', a[i]})
			i++
		default:
			ops = append(ops, diffOp{'+', b[j]})
			j++
		}
	}
	for ; i < n; i++ {
		ops = append(ops, diffOp{'-', a[i]})
	}
	for ; j < m; j++ {
		ops = append(ops, diffOp{'+', b[j]})
	}
	return ops
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
