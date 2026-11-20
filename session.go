package melody

import (
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Library-defined close codes (RFC 6455 reserves 4000-4999 for private use).
const (
	// CloseSessionQueueFull is sent when a slow session is disconnected
	// because its bounded send queue overflowed.
	CloseSessionQueueFull = 4000

	// CloseDrainTimeout is sent when the hub shuts down before a session's
	// send queue finished draining; queued messages were discarded.
	CloseDrainTimeout = 4001
)

// Human-readable close reasons accompanying the library-defined close codes.
const (
	closeReasonQueueFull    = "session send queue full"
	closeReasonDrainTimeout = "shutdown drain timeout"
)

// Session wrapper around websocket connections.
type Session struct {
	Request    *http.Request
	Keys       map[string]any
	conn       *websocket.Conn
	output     chan envelope
	outputDone chan struct{}
	// pumpDone is closed when writePump has returned, i.e. no more network
	// writes are in flight and the ping ticker has been stopped.
	pumpDone chan struct{}
	melody   *Melody

	rwmutex sync.RWMutex
	open    bool
	// closing is flipped to true by the first closer so that concurrent
	// overflow enqueues never request more than one disconnect.
	closing bool

	closeOnce sync.Once
	reasonMu  sync.RWMutex
	closeCode int
	closeText string
}

// enqueue offers message to the bounded session send queue. It never blocks on
// the network and never sends on a closed channel: the output channel is never
// closed, and closed sessions are rejected up front.
//
// On a full queue the configured Config.BufferOverflow policy applies:
// ReturnOverflowError reports ErrMessageBufferFull and leaves the session
// untouched; DisconnectSlowSession requests a single, reason-tagged disconnect
// of this session only.
func (s *Session) enqueue(message envelope) error {
	s.rwmutex.RLock()
	if !s.open || s.closing {
		s.rwmutex.RUnlock()
		return ErrSessionClosed
	}
	s.rwmutex.RUnlock()

	select {
	case s.output <- message:
		return nil
	default:
	}

	switch s.melody.Config.BufferOverflow {
	case DisconnectSlowSession:
		s.beginClose()
		return ErrMessageBufferFull
	default:
		s.melody.errorHandler(s, ErrMessageBufferFull)
		return ErrMessageBufferFull
	}
}

// beginClose marks the session as closing exactly once and performs the
// reason-tagged teardown asynchronously so that broadcast callers never wait on
// a network write.
func (s *Session) beginClose() {
	s.rwmutex.Lock()
	if !s.open || s.closing {
		s.rwmutex.Unlock()
		return
	}
	s.closing = true
	s.rwmutex.Unlock()

	go s.forceClose(CloseSessionQueueFull, closeReasonQueueFull)
}

// forceClose tears a session down with a recognizable reason. The write pump
// is stopped and its still-queued messages are discarded. If the pump is
// stuck in a network write the connection is aborted with a TCP RST (via
// SO_LINGER=0), which purges unsent kernel data and unblocks the client
// immediately instead of letting stale data delay the teardown.
//
// forceClose never blocks on the pump: a session's own HandleRequest
// goroutine waits for pumpDone, so blocking here would deadlock a shutdown
// invoked from within that goroutine. The pump and the connection are released
// asynchronously once the aborted network write returns.
func (s *Session) forceClose(code int, text string) {
	s.setCloseReason(code, text)

	// Non-blocking probe: if the pump is still running it is committed to a
	// network write (or about to start one), so abort the connection to
	// unblock it rather than wait out the full write deadline.
	select {
	case <-s.pumpDone:
	default:
		s.abortConn()
	}

	// Best-effort close frame; harmless when the connection was already
	// aborted, in which case the client observes the RST directly.
	deadline := s.melody.Config.WriteWait
	if deadline > 0 {
		_ = s.conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(code, text),
			time.Now().Add(deadline),
		)
	}

	s.finishClose()
}

// abortConn closes the underlying TCP connection with SO_LINGER=0 so that an
// RST segment is sent, unsent buffered data is discarded and blocked writes
// return immediately. Non-TCP connections fall back to a plain close.
func (s *Session) abortConn() {
	if tcpConn, ok := s.conn.UnderlyingConn().(*net.TCPConn); ok {
		if raw, err := tcpConn.SyscallConn(); err == nil {
			_ = raw.Control(func(fd uintptr) {
				_ = unixSetsockoptLinger(int(fd))
			})
		}
		_ = tcpConn.Close()
		return
	}
	_ = s.conn.UnderlyingConn().Close()
}

// finishClose releases the session exactly once. Closing the underlying
// connection unblocks readPump and writePump; outputDone makes writePump exit
// even when the network write is blocked on a deadline.
func (s *Session) finishClose() {
	s.closeOnce.Do(func() {
		s.rwmutex.Lock()
		wasOpen := s.open
		s.open = false
		s.rwmutex.Unlock()

		if wasOpen {
			s.conn.Close()
			close(s.outputDone)
		}
	})
}

// close preserves the original teardown entry point used by readPump and
// writePump. Sessions closed this way without an explicit reason report
// CloseAbnormalClosure.
func (s *Session) close() {
	s.closeOnce.Do(func() {
		s.rwmutex.Lock()
		wasOpen := s.open
		s.open = false
		s.rwmutex.Unlock()

		if wasOpen {
			s.setDefaultCloseReason(CloseAbnormalClosure, "")
			s.conn.Close()
			close(s.outputDone)
		}
	})
}

func (s *Session) closed() bool {
	s.rwmutex.RLock()
	defer s.rwmutex.RUnlock()

	return !s.open
}

func (s *Session) setCloseReason(code int, text string) {
	s.reasonMu.Lock()
	defer s.reasonMu.Unlock()

	if s.closeCode == 0 {
		s.closeCode = code
		s.closeText = text
	}
}

// setDefaultCloseReason records the teardown reason unless one was already
// recorded (for example by a received close frame or an overflow disconnect).
func (s *Session) setDefaultCloseReason(code int, text string) {
	s.reasonMu.Lock()
	defer s.reasonMu.Unlock()

	if s.closeCode == 0 {
		s.closeCode = code
		s.closeText = text
	}
}

// CloseCode returns the WebSocket close code associated with the session
// shutdown, or 0 while the session is still open. Client-initiated closures
// report the code received in the close frame (CloseNoStatusReceived when the
// frame carried no payload); library-initiated closures report one of the
// Close* constants.
func (s *Session) CloseCode() int {
	s.reasonMu.RLock()
	defer s.reasonMu.RUnlock()

	return s.closeCode
}

// CloseText returns the close reason text associated with the session
// shutdown, or an empty string while the session is still open or when no
// reason text was provided.
func (s *Session) CloseText() string {
	s.reasonMu.RLock()
	defer s.reasonMu.RUnlock()

	return s.closeText
}

// parseClosePayload extracts the close code and reason text from a close frame
// payload. An empty payload yields CloseNoStatusReceived.
func parseClosePayload(payload []byte) (int, string) {
	if len(payload) < 2 {
		return CloseNoStatusReceived, ""
	}

	code := int(payload[0])<<8 | int(payload[1])
	text := string(payload[2:])

	return code, text
}

func (s *Session) writeRaw(message envelope) error {
	if s.closed() {
		return ErrWriteClosed
	}

	// Use per-message deadline if specified, otherwise use global config
	deadline := message.writeWait
	if deadline == 0 {
		deadline = s.melody.Config.WriteWait
	}

	s.conn.SetWriteDeadline(time.Now().Add(deadline))
	err := s.conn.WriteMessage(message.t, message.msg)

	if err != nil {
		return err
	}

	return nil
}

func (s *Session) ping() {
	if err := s.writeRaw(envelope{t: websocket.PingMessage, msg: []byte{}}); err != nil {
		s.melody.errorHandler(s, err)
	}
}

// writePump drains the bounded send queue onto the network. It is the only
// goroutine that calls WriteMessage, which preserves per-session message
// ordering. Broadcasts and unicasts only wait for enqueueing, never for this
// loop. The loop always stops its ping ticker and signals pumpDone on exit, so
// shutdown never leaves a writer goroutine or a heartbeat timer behind.
func (s *Session) writePump() {
	ticker := time.NewTicker(s.melody.Config.PingPeriod)
	defer func() {
		ticker.Stop()
		close(s.pumpDone)
		s.close()
	}()

loop:
	for {
		select {
		case msg := <-s.output:
			err := s.writeRaw(msg)

			if err != nil {
				if err != ErrWriteClosed {
					s.melody.errorHandler(s, err)
				}
				break loop
			}

			if msg.t == websocket.CloseMessage {
				if code, text := parseClosePayload(msg.msg); code != CloseNoStatusReceived {
					s.setDefaultCloseReason(code, text)
				}
				break loop
			}

			if msg.t == websocket.TextMessage {
				s.melody.messageSentHandler(s, msg.msg)
			}

			if msg.t == websocket.BinaryMessage {
				s.melody.messageSentHandlerBinary(s, msg.msg)
			}
		case <-ticker.C:
			s.ping()
		case <-s.outputDone:
			break loop
		}
	}
}

func (s *Session) readPump() {
	s.conn.SetReadLimit(s.melody.Config.MaxMessageSize)
	s.conn.SetReadDeadline(time.Now().Add(s.melody.Config.PongWait))

	s.conn.SetPongHandler(func(string) error {
		s.conn.SetReadDeadline(time.Now().Add(s.melody.Config.PongWait))
		s.melody.pongHandler(s)
		return nil
	})

	s.conn.SetCloseHandler(func(code int, text string) error {
		s.setCloseReason(code, text)
		if s.melody.closeHandler != nil {
			return s.melody.closeHandler(s, code, text)
		}
		return nil
	})

	for {
		t, message, err := s.conn.ReadMessage()

		if err != nil {
			if closeErr, ok := err.(*websocket.CloseError); ok {
				s.setCloseReason(closeErr.Code, closeErr.Text)
			}
			if !s.closed() {
				s.melody.errorHandler(s, err)
			}
			break
		}

		if s.melody.Config.ConcurrentMessageHandling {
			go s.handleMessage(t, message)
		} else {
			s.handleMessage(t, message)
		}
	}
}

func (s *Session) handleMessage(t int, message []byte) {
	switch t {
	case websocket.TextMessage:
		s.melody.messageHandler(s, message)
	case websocket.BinaryMessage:
		s.melody.messageHandlerBinary(s, message)
	}
}

// Write writes message to session.
func (s *Session) Write(msg []byte) error {
	if s.closed() {
		return ErrSessionClosed
	}

	return s.enqueue(envelope{t: websocket.TextMessage, msg: msg})
}

// WriteBinary writes a binary message to session.
func (s *Session) WriteBinary(msg []byte) error {
	if s.closed() {
		return ErrSessionClosed
	}

	return s.enqueue(envelope{t: websocket.BinaryMessage, msg: msg})
}

// Close closes session.
func (s *Session) Close() error {
	if s.closed() {
		return ErrSessionClosed
	}

	return s.enqueue(envelope{t: websocket.CloseMessage, msg: []byte{}})
}

// CloseWithMsg closes the session with the provided payload.
// Use the FormatCloseMessage function to format a proper close message payload.
func (s *Session) CloseWithMsg(msg []byte) error {
	if s.closed() {
		return ErrSessionClosed
	}

	return s.enqueue(envelope{t: websocket.CloseMessage, msg: msg})
}

// Set is used to store a new key/value pair exclusively for this session.
// It also lazy initializes s.Keys if it was not used previously.
func (s *Session) Set(key string, value any) {
	s.rwmutex.Lock()
	defer s.rwmutex.Unlock()

	if s.Keys == nil {
		s.Keys = make(map[string]any)
	}

	s.Keys[key] = value
}

// Get returns the value for the given key, ie: (value, true).
// If the value does not exists it returns (nil, false)
func (s *Session) Get(key string) (value any, exists bool) {
	s.rwmutex.RLock()
	defer s.rwmutex.RUnlock()

	if s.Keys != nil {
		value, exists = s.Keys[key]
	}

	return
}

// MustGet returns the value for the given key if it exists, otherwise it panics.
func (s *Session) MustGet(key string) any {
	if value, exists := s.Get(key); exists {
		return value
	}

	panic("Key \"" + key + "\" does not exist")
}

// UnSet will delete the key and has no return value
func (s *Session) UnSet(key string) {
	s.rwmutex.Lock()
	defer s.rwmutex.Unlock()
	if s.Keys != nil {
		delete(s.Keys, key)
	}
}

// IsClosed returns the status of the connection.
func (s *Session) IsClosed() bool {
	return s.closed()
}

// LocalAddr returns the local addr of the connection.
func (s *Session) LocalAddr() net.Addr {
	return s.conn.LocalAddr()
}

// RemoteAddr returns the remote addr of the connection.
func (s *Session) RemoteAddr() net.Addr {
	return s.conn.RemoteAddr()
}

// WebsocketConnection returns the underlying websocket connection.
// This can be used to e.g. set/read additional websocket options or to write sychronous messages.
func (s *Session) WebsocketConnection() *websocket.Conn {
	return s.conn
}

// WriteWithDeadline writes a text message to the session with a custom write deadline.
// If deadline is 0, uses Config.WriteWait.
func (s *Session) WriteWithDeadline(msg []byte, deadline time.Duration) error {
	if s.closed() {
		return ErrSessionClosed
	}

	return s.enqueue(envelope{
		t:         websocket.TextMessage,
		msg:       msg,
		writeWait: deadline,
	})
}

// WriteBinaryWithDeadline writes a binary message to the session with a custom write deadline.
// If deadline is 0, uses Config.WriteWait.
func (s *Session) WriteBinaryWithDeadline(msg []byte, deadline time.Duration) error {
	if s.closed() {
		return ErrSessionClosed
	}

	return s.enqueue(envelope{
		t:         websocket.BinaryMessage,
		msg:       msg,
		writeWait: deadline,
	})
}
