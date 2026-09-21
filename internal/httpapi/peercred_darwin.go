//go:build darwin

package httpapi

import (
	"net"

	"golang.org/x/sys/unix"

	"github.com/ycooi/Lymph/internal/engine"
)

// peerCredentials reads LOCAL_PEERCRED from a Unix-socket connection on
// macOS. The kernel supplies the effective uid and primary gid of the peer;
// request headers and bodies are never trusted for authorization.
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
	var cred *unix.Xucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	}); err != nil || credErr != nil || cred == nil {
		return info
	}
	info.UID = int(cred.Uid)
	if cred.Ngroups > 0 {
		info.GID = int(cred.Groups[0])
	}
	info.Supported = true
	return info
}
