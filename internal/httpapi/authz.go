package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/ycooi/Lymph/internal/engine"
)

// Local authorization for the control socket.
//
// Lymph keeps one socket and one daemon (mission section 24). What differs
// between a normal application, a worker and an operator is not the transport
// but the privilege of the endpoint, so the daemon resolves the caller into a
// small set of roles and checks the role at the endpoint.
//
// Two deliberate limits:
//
//  1. A role is never taken from the request. `{"role":"OPERATOR"}` in a body
//     or header is ignored; the only inputs are the kernel's peer credentials
//     on the Unix socket and the local policy file (mission section 22).
//  2. This is not RBAC. There are three roles, they are matched by uid or gid,
//     and nothing is inherited or nested (mission section 23).

// Role is a local privilege class.
type Role string

const (
	// RoleApplication may handshake, emit feedback, read its own approved
	// updates, fetch them, and report the outcome.
	RoleApplication Role = "APPLICATION"
	// RoleWorker may claim work and return improvement results and validations.
	RoleWorker Role = "WORKER"
	// RoleOperator may approve, reject, defer and withdraw. It is the only role
	// whose approvals can make content deliverable (mission section 22).
	RoleOperator Role = "OPERATOR"
)

// ValidRole reports whether a string names a known role.
func ValidRole(value string) bool {
	switch Role(strings.ToUpper(strings.TrimSpace(value))) {
	case RoleApplication, RoleWorker, RoleOperator:
		return true
	}
	return false
}

// ForbiddenError is returned when a resolved principal lacks the role an
// endpoint requires. It is the machine-readable refusal for mission sections
// 22, 66 and 75.
type ForbiddenError struct {
	Needed  Role
	PeerUID int
	Reason  string
}

func (e *ForbiddenError) Error() string {
	return fmt.Sprintf("FORBIDDEN: %s requires role %s (peer uid %d)", e.Reason, e.Needed, e.PeerUID)
}

// PrincipalPolicy grants roles to the local user or group that opened the
// connection (mission section 23).
type PrincipalPolicy struct {
	// UID matches a peer by user id. A nil UID means that this policy matches
	// by GID only. Pointer fields distinguish the real root uid (0) from an
	// omitted selector.
	UID *int `json:"uid,omitempty"`
	// GID matches a peer by primary group id. A nil GID means that this policy
	// matches by UID only. When both are present, both must match.
	GID *int `json:"gid,omitempty"`

	Roles []Role `json:"roles"`
	// Applications narrows which applications this principal may act for. An
	// empty list means "no restriction beyond the role".
	Applications []string `json:"applications,omitempty"`
	Comment      string   `json:"comment,omitempty"`
}

// ACL is the local authorization registry, read from a JSON file the operator
// controls.
type ACL struct {
	Principals []PrincipalPolicy `json:"principals"`
	// DefaultRoles apply to a peer that matches no principal. It is empty by
	// default: an unrecognised local user gets no roles at all.
	DefaultRoles []Role `json:"default_roles,omitempty"`

	path string
}

// Path reports where the policy was loaded from.
func (a *ACL) Path() string {
	if a == nil {
		return ""
	}
	return a.path
}

// LoadACL reads a policy file. An empty path returns (nil, nil): the daemon then
// runs in single-principal local trust mode.
func LoadACL(path string) (*ACL, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("stat authorization policy %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("authorization policy %s must not be a symbolic link", path)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("authorization policy %s is not a regular file", path)
	}
	if info.Mode().Perm()&0o022 != 0 {
		return nil, fmt.Errorf("authorization policy %s is writable by group or others (mode %#o)", path, info.Mode().Perm())
	}
	if info.Size() > 1<<20 {
		return nil, fmt.Errorf("authorization policy %s exceeds the 1 MiB limit", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read authorization policy %s: %w", path, err)
	}
	var acl ACL
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&acl); err != nil {
		return nil, fmt.Errorf("parse authorization policy %s: %w", path, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse authorization policy %s: trailing content", path)
	}
	for i, principal := range acl.Principals {
		for _, role := range principal.Roles {
			if !ValidRole(string(role)) {
				return nil, fmt.Errorf("authorization policy %s: principal %d has unknown role %q", path, i, role)
			}
		}
		if principal.UID == nil && principal.GID == nil {
			return nil, fmt.Errorf("authorization policy %s: principal %d must set uid or gid", path, i)
		}
		if principal.UID != nil && *principal.UID < 0 {
			return nil, fmt.Errorf("authorization policy %s: principal %d has negative uid", path, i)
		}
		if principal.GID != nil && *principal.GID < 0 {
			return nil, fmt.Errorf("authorization policy %s: principal %d has negative gid", path, i)
		}
	}
	for _, role := range acl.DefaultRoles {
		if !ValidRole(string(role)) {
			return nil, fmt.Errorf("authorization policy %s: unknown default role %q", path, role)
		}
	}
	acl.path = path
	return &acl, nil
}

// Principal is the resolved caller.
type Principal struct {
	UID int `json:"uid"`
	GID int `json:"gid"`

	Roles        []Role   `json:"roles"`
	Applications []string `json:"applications,omitempty"`

	// Mode says how the principal was decided, so the answer to "why was this
	// allowed?" is visible in an audit rather than inferred.
	Mode string `json:"mode"`
}

// Authorization modes.
const (
	// ModeACL: the operator's policy file decided the roles.
	ModeACL = "ACL"
	// ModeLocalTrust: no policy file is configured, so the daemon's own socket
	// permissions are the boundary and every local peer holds every role. This
	// is single-user installs; it is reported in /v1/status so it is never a
	// silent assumption.
	ModeLocalTrust = "SINGLE_PRINCIPAL_LOCAL_TRUST"
	// ModePeerUnknown: a policy file exists but the transport gave no peer
	// credentials, so the peer matches nothing and receives the default roles.
	ModePeerUnknown = "PEER_UNKNOWN"
)

// Has reports whether the principal holds a role.
func (p Principal) Has(role Role) bool {
	for _, held := range p.Roles {
		if held == role {
			return true
		}
	}
	return false
}

// MayActFor reports whether the principal may act for an application. An
// unrestricted principal may act for any.
func (p Principal) MayActFor(applicationID string) bool {
	if len(p.Applications) == 0 {
		return true
	}
	for _, allowed := range p.Applications {
		if allowed == applicationID {
			return true
		}
	}
	return false
}

// Resolve maps peer credentials to a principal (mission section 23).
func (a *ACL) Resolve(peer engine.PeerInfo) Principal {
	if a == nil {
		// No policy file: single-principal local trust. The socket mode, set at
		// startup and never widened to world-accessible, is the actual gate.
		return Principal{
			UID:   peer.UID,
			GID:   peer.GID,
			Roles: []Role{RoleApplication, RoleWorker, RoleOperator},
			Mode:  ModeLocalTrust,
		}
	}
	if !peer.Supported {
		return Principal{
			UID: peer.UID, GID: peer.GID,
			Roles: append([]Role(nil), a.DefaultRoles...),
			Mode:  ModePeerUnknown,
		}
	}

	principal := Principal{UID: peer.UID, GID: peer.GID, Mode: ModeACL}
	seen := map[Role]bool{}
	for _, policy := range a.Principals {
		if policy.UID != nil && *policy.UID != peer.UID {
			continue
		}
		if policy.GID != nil && *policy.GID != peer.GID {
			continue
		}
		for _, role := range policy.Roles {
			if !seen[role] {
				seen[role] = true
				principal.Roles = append(principal.Roles, role)
			}
		}
		for _, app := range policy.Applications {
			already := false
			for _, existing := range principal.Applications {
				if existing == app {
					already = true
				}
			}
			if !already {
				principal.Applications = append(principal.Applications, app)
			}
		}
	}
	if len(principal.Roles) == 0 {
		principal.Roles = append(principal.Roles, a.DefaultRoles...)
		if len(principal.Roles) == 0 {
			principal.Mode = ModePeerUnknown
		}
	}
	return principal
}

// peerOf reads the kernel's view of the caller. It is empty (UID -1) when the
// transport cannot supply it, which happens for in-process tests and for
// platforms without peer credential support.
func (s *Server) peerOf(r *http.Request) engine.PeerInfo {
	conn, ok := r.Context().Value(connContextKey{}).(net.Conn)
	if !ok {
		return engine.PeerInfo{UID: -1, GID: -1}
	}
	return peerCredentials(conn)
}

// principal resolves the caller for a request.
func (s *Server) principal(r *http.Request) Principal {
	return s.acl.Resolve(s.peerOf(r))
}

// requireRole refuses a request whose principal lacks the role. It writes the
// refusal itself and reports whether the caller may continue.
func (s *Server) requireRole(w http.ResponseWriter, r *http.Request, role Role, reason string) (Principal, bool) {
	principal := s.principal(r)
	if principal.Has(role) {
		return principal, true
	}
	writeForbidden(w, &ForbiddenError{Needed: role, PeerUID: principal.UID, Reason: reason})
	return principal, false
}

func (s *Server) roleHandler(role Role, reason string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, allowed := s.requireRole(w, r, role, reason); !allowed {
			return
		}
		next(w, r)
	})
}

func (s *Server) anyRoleHandler(roles []Role, reason string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal := s.principal(r)
		for _, role := range roles {
			if principal.Has(role) {
				next(w, r)
				return
			}
		}
		needed := RoleOperator
		if len(roles) > 0 {
			needed = roles[0]
		}
		writeForbidden(w, &ForbiddenError{Needed: needed, PeerUID: principal.UID, Reason: reason})
	})
}

func writeForbidden(w http.ResponseWriter, err *ForbiddenError) {
	writeJSON(w, http.StatusForbidden, map[string]any{
		"error":       err.Error(),
		"reason_code": "FORBIDDEN",
		"needed_role": string(err.Needed),
		"peer_uid":    err.PeerUID,
	})
}
