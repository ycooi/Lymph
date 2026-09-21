package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/ycooi/Lymph/internal/objectstore"
)

// Structural validation happens before a bundle can become a candidate:
// section 32 of the architecture says an invalid candidate never enters
// testing, and the test plan says a worker returning invalid YAML must be
// rejected rather than queued for evaluation.
//
// This is a *parse* check, not a schema check. It answers "is this the format
// the file claims to be?" and nothing more. Meaning — which field is mandatory,
// which value is legal — belongs to the config family's own validation workflow
// (section 2: Lymph does not invent the configuration's meaning).

// ErrStructuralInvalid is returned when a bundle does not parse.
var ErrStructuralInvalid = errors.New("candidate bundle failed structural validation")

// maxParseBytes bounds how much of one file is parsed. A config file larger
// than this is not a config file.
const maxParseBytes = 4 << 20

// ValidateBundleStructure parses every file that declares a structured format.
// Unknown extensions are passed through untouched: Lymph does not guess.
func ValidateBundleStructure(bundle objectstore.Bundle) error {
	if len(bundle) == 0 {
		return fmt.Errorf("%w: bundle is empty", ErrStructuralInvalid)
	}

	paths := make([]string, 0, len(bundle))
	for path := range bundle {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	for _, path := range paths {
		data := bundle[path]
		if len(data) > maxParseBytes {
			return fmt.Errorf("%w: %s is %d bytes, over the %d byte parse limit",
				ErrStructuralInvalid, path, len(data), maxParseBytes)
		}

		switch strings.ToLower(filepath.Ext(path)) {
		case ".yaml", ".yml":
			var out any
			if err := yaml.Unmarshal(data, &out); err != nil {
				return fmt.Errorf("%w: %s is not valid YAML: %s",
					ErrStructuralInvalid, path, firstLine(err.Error()))
			}
		case ".json":
			if !json.Valid(data) {
				return fmt.Errorf("%w: %s is not valid JSON", ErrStructuralInvalid, path)
			}
		default:
			// Not a structured format we claim to validate.
		}

		if bytes.ContainsRune(data, 0) && isTextFormat(path) {
			return fmt.Errorf("%w: %s contains a NUL byte in a text format", ErrStructuralInvalid, path)
		}
	}
	return nil
}

func isTextFormat(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml", ".json", ".toml", ".txt", ".conf", ".ini", ".csv":
		return true
	default:
		return false
	}
}

// firstLine keeps error messages readable when a parser reports a multi-line
// diagnostic.
func firstLine(s string) string {
	if idx := strings.IndexByte(s, '\n'); idx >= 0 {
		return s[:idx]
	}
	return s
}
