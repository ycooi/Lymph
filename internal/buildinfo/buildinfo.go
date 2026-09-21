// Package buildinfo contains release metadata injected by the build.
package buildinfo

import "fmt"

// These variables are set with -ldflags by the Makefile and release workflow.
// Direct `go build` remains useful and reports an honest development build.
var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

// String returns the compact version shown by command-line tools.
func String() string {
	if Commit == "" || Commit == "unknown" {
		return Version
	}
	return fmt.Sprintf("%s (%s)", Version, Commit)
}
