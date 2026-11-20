package melody

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type hub struct {
	mu       sync.RWMutex
	sessions map[*Session]struct{}
	open     atomic.Bool
	melody   *Melody
}

func newHub(m *Melody) *hub {
	hub := &hub{
		sessions: make(map[*Session]struct{}),
		melody:   m,
	}
	hub.open.Store(true)
	return hub
}

func (h *hub) closed() bool {
	return !h.open.Load()
}

func (h *hub) len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.sessions)
}

func (h *hub) all() []*Session {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make([]*Session, 0, len(h.sessions))
	for s := range h.sessions {
		result = append(result, s)
	}
	return result
}

// register adds s to the hub. It returns false when the hub is shutting down
// or already closed, in which case the caller must reject the new connection
// instead of serving it.
func (h *hub) register(s *Session) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.open.Load() {
		return false
	}

	h.sessions[s] = struct{}{}
	return true
}

func (h *hub) unregister(s *Session) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.sessions, s)
}

// enqueue delivers msg to a single session. Session-closed errors are expected
// during shutdown and swallowed; every other error is reported through the
// melody error handler.
func (h *hub) enqueue(s *Session, msg envelope) error {
	err := s.enqueue(msg)
	if err != nil && err != ErrSessionClosed {
		h.melody.errorHandler(s, err)
	}
	return err
}

// broadcast fans msg out to every matching session. It only waits for each
// bounded queue to accept the message, never for network writes, so one slow
// session cannot stall delivery to the others.
func (h *hub) broadcast(msg envelope, reportOverflow bool) []*Session {
	h.mu.RLock()
	sessions := make([]*Session, 0, len(h.sessions))
	for s := range h.sessions {
		if msg.filter == nil || msg.filter(s) {
			sessions = append(sessions, s)
		}
	}
	h.mu.RUnlock()

	var overflows []*Session

	for _, s := range sessions {
		err := h.enqueue(s, msg)
		if err == ErrMessageBufferFull && reportOverflow {
			overflows = append(overflows, s)
		}
	}

	return overflows
}

// exit performs a safe shutdown in two phases. First the hub stops accepting
// broadcasts and new connections. Each session then receives a graceful close
// envelope after everything already queued, and the call waits up to timeout
// for the healthy write pumps to drain their queues and exit. Sessions whose
// queues do not drain in time have their remaining messages explicitly marked
// as discarded (via ErrMessageDropped) and are force-closed with
// CloseDrainTimeout. Only the timed-out sessions are affected; healthy
// sessions drain untouched.
func (h *hub) exit(msg envelope, timeout time.Duration) *DrainError {
	h.mu.Lock()
	if !h.open.Load() {
		h.mu.Unlock()
		return &DrainError{Dropped: map[*Session]int{}}
	}

	// Phase 1: stop receiving. New registrations and broadcasts are rejected
	// from this point on.
	h.open.Store(false)

	sessions := make([]*Session, 0, len(h.sessions))
	for s := range h.sessions {
		sessions = append(sessions, s)
	}
	h.sessions = make(map[*Session]struct{})
	h.mu.Unlock()

	// Phase 2: queue the graceful close behind every pending message and let
	// the write pumps drain within the shutdown deadline.
	deadline := time.Now().Add(timeout)
	waitTimer := time.NewTimer(time.Hour)
	waitTimer.Stop()
	defer waitTimer.Stop()
	resetTimer := func(d time.Duration) {
		if !waitTimer.Stop() {
			select {
			case <-waitTimer.C:
			default:
			}
		}
		waitTimer.Reset(d)
	}

	for _, s := range sessions {
		closeMsg := envelope{t: websocket.CloseMessage, msg: msg.msg}
		if err := s.enqueue(closeMsg); err == ErrMessageBufferFull {
			// Full queue under ReturnOverflowError: wait for the pump to make
			// room, but never beyond the drain deadline. The output channel is
			// never closed, so a closed session is observed via outputDone.
			remaining := time.Until(deadline)
			if remaining <= 0 {
				continue
			}
			resetTimer(remaining)
			select {
			case s.output <- closeMsg:
			case <-s.outputDone:
			case <-waitTimer.C:
			}
		}
	}

	timedOut := make(map[*Session]int)

	for _, s := range sessions {
		remaining := time.Until(deadline)

		if remaining > 0 {
			resetTimer(remaining)
			select {
			case <-s.pumpDone:
				// Drained within the deadline.
			case <-waitTimer.C:
				// Deadline elapsed while waiting. Only a session still holding
				// undelivered messages is considered stalled; healthy sessions
				// whose close frame is already in flight (queue empty) are
				// left untouched.
				if n := len(s.output); n > 0 {
					timedOut[s] = n
					h.melody.errorHandler(s, ErrMessageDropped)
					// Non-blocking: the pump and connection wind down
					// asynchronously after the abort.
					go s.forceClose(CloseDrainTimeout, closeReasonDrainTimeout)
				}
			}
			continue
		}

		// The global deadline was already consumed by earlier sessions (or a
		// zero timeout was requested). If this one still holds messages it
		// cannot drain in time; otherwise it is healthy and needs no forced
		// teardown.
		if n := len(s.output); n > 0 {
			select {
			case <-s.pumpDone:
			default:
				timedOut[s] = n
				h.melody.errorHandler(s, ErrMessageDropped)
				go s.forceClose(CloseDrainTimeout, closeReasonDrainTimeout)
			}
		}
	}

	if len(timedOut) > 0 {
		return &DrainError{Dropped: timedOut}
	}
	return nil
}
