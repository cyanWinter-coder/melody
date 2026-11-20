// Platform-specific SO_LINGER support for force-closing sessions with a TCP
// RST instead of a graceful FIN.

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package melody

import "syscall"

// unixSetsockoptLinger sets SO_LINGER to an immediate timeout (l_onoff=1,
// l_linger=0) so the next close(2) on fd emits an RST and discards unsent
// data.
func unixSetsockoptLinger(fd int) error {
	return syscall.SetsockoptLinger(fd, syscall.SOL_SOCKET, syscall.SO_LINGER, &syscall.Linger{
		Onoff:  1,
		Linger: 0,
	})
}
