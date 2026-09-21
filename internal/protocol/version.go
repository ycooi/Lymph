package protocol

// Protocol versioning is deliberately separate from the daemon's release
// version: lymphd 0.1.7 may speak protocol 1 forever, and a wire change must
// change the protocol number even if the daemon version does not move.

// Version is the protocol this build speaks.
const Version = "1"

// SupportedVersions lists every protocol version the daemon accepts.
func SupportedVersions() []string { return []string{Version} }

// Versions is an alias used in status payloads.
func Versions() []string { return SupportedVersions() }

// Compatible reports whether a client's requested protocol version is one this
// daemon can serve.
func Compatible(requested string) bool {
	for _, supported := range SupportedVersions() {
		if requested == supported {
			return true
		}
	}
	return false
}

// Header names used after a successful handshake.
const (
	// HeaderSession carries the session UUID on every request after /v1/hello.
	HeaderSession = "X-Lymph-Session"
	// HeaderReplay marks a request as a replay of previously spooled events, in
	// which the event's producer identity may legitimately differ from the
	// session's process.
	HeaderReplay = "X-Lymph-Replay"
)

// Capabilities are the optional features a client may rely on. This is not a
// negotiation engine: it exists so a client can tell whether the daemon in front
// of it understands the things it might send.
func Capabilities() []string {
	return []string{
		"events",
		"spool-replay",
		"sessions",
		"improvement-results",
		"installations",
	}
}

// ReasonCode identifies why a handshake or an event was refused. Clients switch
// on these; they never parse the human-readable message.
type ReasonCode string

// Handshake and session reason codes.
const (
	// ReasonMalformed is a request the daemon could not read at all.
	ReasonMalformed ReasonCode = "MALFORMED_REQUEST"
	// ReasonProtocolIncompatible means the client asked for a protocol version
	// this daemon does not speak.
	ReasonProtocolIncompatible ReasonCode = "PROTOCOL_INCOMPATIBLE"
	// ReasonUnknownApplication means the application UUID is not registered.
	ReasonUnknownApplication ReasonCode = "UNKNOWN_APPLICATION"
	// ReasonUnknownInstallation means the installation UUID is not registered
	// for that application.
	ReasonUnknownInstallation ReasonCode = "UNKNOWN_INSTALLATION"
	// ReasonInstallationMismatch means the installation belongs to another
	// application. It is distinct from "unknown": the identity exists.
	ReasonInstallationMismatch ReasonCode = "INSTALLATION_APPLICATION_MISMATCH"
	// ReasonInvalidIdentity means one of the UUID fields is not a UUID.
	ReasonInvalidIdentity ReasonCode = "INVALID_IDENTITY"
	// ReasonSessionUnknown means the session UUID is not one this daemon knows:
	// it expired, was closed, or the daemon restarted.
	ReasonSessionUnknown ReasonCode = "SESSION_UNKNOWN"
	// ReasonSessionIdentityMismatch means the session is valid but the event's
	// identity disagrees with it.
	ReasonSessionIdentityMismatch ReasonCode = "SESSION_IDENTITY_MISMATCH"
)

// Audit actions recorded canonically. Successful handshakes are not audited:
// they are operational, and canonicalising every hello would drown the ledger.
const (
	AuditHandshakeRejected    = "session.handshake_rejected"
	AuditProtocolIncompatible = "session.protocol_incompatible"
	AuditIdentityMismatch     = "session.identity_mismatch"
	AuditManifestDrift        = "session.manifest_drift"
	AuditDuplicateSession     = "session.duplicate_installation_session"
	AuditInstanceChanged      = "session.lymph_instance_changed"
	AuditSessionlessEvent     = "session.sessionless_event"
)
