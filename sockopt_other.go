// Fallback for platforms without golang.org/x/sys/unix support. A plain
// connection close is used; force-close still works, only the RST semantics
// are unavailable.

//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly

package melody

func unixSetsockoptLinger(fd int) error {
	return nil
}
