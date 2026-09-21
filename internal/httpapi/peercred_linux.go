//go:build linux

package httpapi

import (
	"net"
	"syscall"

	"github.com/ycooi/Lymph/internal/engine"
)

// peerCredentials reads SO_PEERCRED from a Unix-socket connection.
//
// This is operational metadata: it tells an operator which local user opened a
// session. It is never identity — an application is identified by its registered
// UUIDs, and the kernel's view of a uid changes nothing about that (mission
// sections 39, 118).
func peerCredentials(conn net.Conn) engine.PeerInfo {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return engine.PeerInfo{UID: -1, GID: -1}
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return engine.PeerInfo{UID: -1, GID: -1}
	}

	info := engine.PeerInfo{UID: -1, GID: -1}
	var cred *syscall.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil || credErr != nil || cred == nil {
		return info
	}
	info.UID = int(cred.Uid)
	info.GID = int(cred.Gid)
	info.Supported = true
	return info
}
