package projection

import (
	"errors"
	"fmt"
)

// ErrNotFound is returned when a row does not exist.
var ErrNotFound = errors.New("not found")

// ErrAlreadyExists is returned when the primary identity is already taken.
var ErrAlreadyExists = errors.New("already exists")

// ErrNameConflict is returned when a human-readable name is already used by a
// different identity. Names are metadata (section 4): a new identity may not
// silently steal one.
var ErrNameConflict = errors.New("name is already registered to another identity")

// ErrPathConflict implements section 26: two applications may not own the same
// managed deployment path.
var ErrPathConflict = errors.New("managed path is already owned by another application")

// ErrStaleCandidate implements section 22 and section 51: promotion compares
// the expected active revision with the actual active revision, and refuses to
// overwrite a revision the candidate was not built on.
var ErrStaleCandidate = errors.New("STALE_CANDIDATE: active revision moved since the candidate was created")

// ErrNoWork is returned by a worker claiming a lease when the queue is empty.
var ErrNoWork = errors.New("no claimable work item")

// NotFoundf wraps ErrNotFound with context.
func NotFoundf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrNotFound, fmt.Sprintf(format, args...))
}
