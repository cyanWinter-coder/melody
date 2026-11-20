package melody

import (
	"errors"
	"fmt"
)

var (
	ErrClosed            = errors.New("melody instance is closed")
	ErrSessionClosed     = errors.New("session is closed")
	ErrWriteClosed       = errors.New("tried to write to closed a session")
	ErrMessageBufferFull = errors.New("session message buffer is full")
	ErrMessageDropped    = errors.New("session message dropped during shutdown drain")
)

// DrainError is returned by CloseWithTimeout when one or more session queues
// could not be drained within the shutdown deadline. Each failing session is
// listed together with the number of queued messages that were discarded.
type DrainError struct {
	// Dropped maps a session to the number of undelivered messages left in its
	// send queue when the drain deadline expired.
	Dropped map[*Session]int
}

func (e *DrainError) Error() string {
	total := 0
	for _, n := range e.Dropped {
		total += n
	}
	return fmt.Sprintf("melody shutdown: %d message(s) dropped across %d session(s)", total, len(e.Dropped))
}
