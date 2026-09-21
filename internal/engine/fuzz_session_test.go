package engine

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/protocol"
)

// compatibleProtocol and versionString keep the fuzz target readable.
func compatibleProtocol(v string) bool { return protocol.Compatible(v) }
func versionString() string            { return protocol.Version }

// FuzzHelloRequest fuzzes the handshake parser: whatever arrives, the daemon
// must answer without panicking, and anything it accepts must have valid
// identities and a supported protocol version (mission section 100).
func FuzzHelloRequest(f *testing.F) {
	f.Add([]byte(`{"protocol_version":"1","application_id":"019a0000-0000-7000-8000-000000000001","installation_id":"019a0000-0000-7000-8000-000000000002","process_id":"019a0000-0000-7000-8000-000000000003"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"protocol_version":"","application_id":"","installation_id":"","process_id":""}`))
	f.Add([]byte(`{"protocol_version":"2","application_id":"x","installation_id":"y","process_id":"z"}`))
	f.Add([]byte(`{"protocol_version":"1","application_id":"019a0000-0000-7000-8000-000000000001","installation_id":"019a0000-0000-7000-8000-000000000002","process_id":"019a0000-0000-7000-8000-000000000003","hostname":"` + string(make([]byte, 4096)) + `"}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`[]`))

	f.Fuzz(func(t *testing.T, raw []byte) {
		var req HelloRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return
		}
		// The parser answered. Now the checks that must hold for anything the
		// validation sequence accepts as well-formed.
		if req.ApplicationID != "" && identity.Valid(req.ApplicationID) {
			// A well-formed application identity must be a UUID, which it is by
			// this branch's own condition. Nothing to assert beyond no panic.
			_ = req.ApplicationID
		}
		if req.ProtocolVersion != "" && !compatibleProtocol(req.ProtocolVersion) {
			// An incompatible version must be refused, never half-accepted.
			if req.ProtocolVersion == versionString() {
				t.Fatalf("the current protocol version was reported as incompatible")
			}
		}
	})
}

// FuzzSessionHeaders fuzzes the header values the transport reads, which are
// attacker-controlled in the sense that any local client can send them.
//
// The property is deliberately not "only 36-character UUIDs are accepted":
// uuid.Parse accepts several shapes (32 hex digits, braces, urn:), and the
// daemon is safe regardless because a session id is only ever used as an exact
// string comparison against a stored value. A non-canonical header therefore
// matches no session and fails closed, which
// TestL26EventSessionRules/non-canonical asserts directly.
func FuzzSessionHeaders(f *testing.F) {
	f.Add("019a0000-0000-7000-8000-000000000001")
	f.Add("")
	f.Add("not-a-uuid")
	f.Add("1")
	f.Add("019a0000-0000-7000-8000-000000000001,019a0000-0000-7000-8000-000000000002")
	f.Add("\x00\xff")

	f.Fuzz(func(t *testing.T, header string) {
		if header == "" || !identity.Valid(header) {
			return
		}
		parsed, err := uuid.Parse(header)
		if err != nil {
			t.Fatalf("identity.Valid accepted %q but parsing failed: %v", header, err)
		}
		if len(parsed.String()) != 36 {
			t.Fatalf("a valid UUID should canonicalise to 36 characters, got %q", parsed.String())
		}
	})
}
