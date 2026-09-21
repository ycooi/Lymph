//go:build !linux && !darwin

package httpapi

import (
	"net"

	"github.com/ycooi/Lymph/internal/engine"
)

// peerCredentials reports that this platform does not supply peer credentials.
// Handshakes proceed normally; the fields stay unknown rather than failing
// (mission section 39).
func peerCredentials(conn net.Conn) engine.PeerInfo {
	return engine.PeerInfo{UID: -1, GID: -1}
}
