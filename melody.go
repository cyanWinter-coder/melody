package melody

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Close codes defined in RFC 6455, section 11.7.
// Duplicate of codes from gorilla/websocket for convenience.
const (
	CloseNormalClosure           = 1000
	CloseGoingAway               = 1001
	CloseProtocolError           = 1002
	CloseUnsupportedData         = 1003
	CloseNoStatusReceived        = 1005
	CloseAbnormalClosure         = 1006
	CloseInvalidFramePayloadData = 1007
	ClosePolicyViolation         = 1008
	CloseMessageTooBig           = 1009
	CloseMandatoryExtension      = 1010
	CloseInternalServerErr       = 1011
	CloseServiceRestart          = 1012
	CloseTryAgainLater           = 1013
	CloseTLSHandshake            = 1015
)

type handleMessageFunc func(*Session, []byte)
type handleErrorFunc func(*Session, error)
type handleCloseFunc func(*Session, int, string) error
type handleSessionFunc func(*Session)
type filterFunc func(*Session) bool

// BroadcastResult reports the outcome of a result-reporting broadcast.
// Enqueueing is all-or-nothing per session: sessions whose bounded queue was
// full are listed in Overflows and never received the message, every other
// matching session accepted it. Broadcasts never wait for the network writes
// themselves.
type BroadcastResult struct {
	// Overflows lists the sessions that rejected the message because their
	// send queue was full. When Config.BufferOverflow is
	// DisconnectSlowSession these sessions have also been scheduled for
	// disconnect with CloseSessionQueueFull.
	Overflows []*Session
}

// Melody implements a websocket manager.
type Melody struct {
	Config                   *Config
	Upgrader                 *websocket.Upgrader
	messageHandler           handleMessageFunc
	messageHandlerBinary     handleMessageFunc
	messageSentHandler       handleMessageFunc
	messageSentHandlerBinary handleMessageFunc
	errorHandler             handleErrorFunc
	closeHandler             handleCloseFunc
	connectHandler           handleSessionFunc
	disconnectHandler        handleSessionFunc
	pongHandler              handleSessionFunc
	hub                      *hub
}

// New creates a new melody instance with default Upgrader and Config.
func New() *Melody {
	upgrader := &websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}

	m := &Melody{
		Config:                   newConfig(),
		Upgrader:                 upgrader,
		messageHandler:           func(*Session, []byte) {},
		messageHandlerBinary:     func(*Session, []byte) {},
		messageSentHandler:       func(*Session, []byte) {},
		messageSentHandlerBinary: func(*Session, []byte) {},
		errorHandler:             func(*Session, error) {},
		closeHandler:             nil,
		connectHandler:           func(*Session) {},
		disconnectHandler:        func(*Session) {},
		pongHandler:              func(*Session) {},
	}
	m.hub = newHub(m)

	return m
}

// HandleConnect fires fn when a session connects.
func (m *Melody) HandleConnect(fn func(*Session)) {
	m.connectHandler = fn
}

// HandleDisconnect fires fn when a session disconnects.
func (m *Melody) HandleDisconnect(fn func(*Session)) {
	m.disconnectHandler = fn
}

// HandlePong fires fn when a pong is received from a session.
func (m *Melody) HandlePong(fn func(*Session)) {
	m.pongHandler = fn
}

// HandleMessage fires fn when a text message comes in.
// NOTE: by default Melody handles messages sequentially for each
// session. This has the effect that a message handler exceeding the
// read deadline (Config.PongWait, by default 1 minute) will time out
// the session. Concurrent message handling can be turned on by setting
// Config.ConcurrentMessageHandling to true.
func (m *Melody) HandleMessage(fn func(*Session, []byte)) {
	m.messageHandler = fn
}

// HandleMessageBinary fires fn when a binary message comes in.
func (m *Melody) HandleMessageBinary(fn func(*Session, []byte)) {
	m.messageHandlerBinary = fn
}

// HandleSentMessage fires fn when a text message is successfully sent.
func (m *Melody) HandleSentMessage(fn func(*Session, []byte)) {
	m.messageSentHandler = fn
}

// HandleSentMessageBinary fires fn when a binary message is successfully sent.
func (m *Melody) HandleSentMessageBinary(fn func(*Session, []byte)) {
	m.messageSentHandlerBinary = fn
}

// HandleError fires fn when a session has an error.
func (m *Melody) HandleError(fn func(*Session, error)) {
	m.errorHandler = fn
}

// HandleClose sets the handler for close messages received from the session.
// The code argument to h is the received close code or CloseNoStatusReceived
// if the close message is empty. The default close handler sends a close frame
// back to the session.
//
// The application must read the connection to process close messages as
// described in the section on Control Frames above.
//
// The connection read methods return a CloseError when a close frame is
// received. Most applications should handle close messages as part of their
// normal error handling. Applications should only set a close handler when the
// application must perform some action before sending a close frame back to
// the session.
func (m *Melody) HandleClose(fn func(*Session, int, string) error) {
	if fn != nil {
		m.closeHandler = fn
	}
}

// HandleRequest upgrades http requests to websocket connections and dispatches them to be handled by the melody instance.
func (m *Melody) HandleRequest(w http.ResponseWriter, r *http.Request) error {
	return m.HandleRequestWithKeys(w, r, nil)
}

// HandleRequestWithKeys does the same as HandleRequest but populates session.Keys with keys.
func (m *Melody) HandleRequestWithKeys(w http.ResponseWriter, r *http.Request, keys map[string]any) error {
	if m.hub.closed() {
		return ErrClosed
	}

	conn, err := m.Upgrader.Upgrade(w, r, w.Header())

	if err != nil {
		return err
	}

	session := &Session{
		Request:    r,
		Keys:       keys,
		conn:       conn,
		output:     make(chan envelope, m.Config.MessageBufferSize),
		outputDone: make(chan struct{}),
		pumpDone:   make(chan struct{}),
		melody:     m,
		open:       true,
	}

	if !m.hub.register(session) {
		// The hub started shutting down between the closed check above and the
		// upgrade; reject the fresh connection instead of leaking it.
		session.close()
		return ErrClosed
	}

	m.connectHandler(session)

	go session.writePump()

	session.readPump()

	if !m.hub.closed() {
		m.hub.unregister(session)
	}

	// Signal the write pump to stop and wait for it to finish so that a
	// session teardown never leaves a writer goroutine (or its ping ticker)
	// behind. close is idempotent with the pump's own deferred teardown.
	session.close()
	<-session.pumpDone

	m.disconnectHandler(session)

	return nil
}

// Broadcast broadcasts a text message to all sessions.
func (m *Melody) Broadcast(msg []byte) error {
	if m.hub.closed() {
		return ErrClosed
	}

	message := envelope{t: websocket.TextMessage, msg: msg}
	m.hub.broadcast(message, false)

	return nil
}

// BroadcastFilter broadcasts a text message to all sessions that fn returns true for.
func (m *Melody) BroadcastFilter(msg []byte, fn func(*Session) bool) error {
	if m.hub.closed() {
		return ErrClosed
	}

	message := envelope{t: websocket.TextMessage, msg: msg, filter: fn}
	m.hub.broadcast(message, false)

	return nil
}

// BroadcastOthers broadcasts a text message to all sessions except session s.
func (m *Melody) BroadcastOthers(msg []byte, s *Session) error {
	return m.BroadcastFilter(msg, func(q *Session) bool {
		return s != q
	})
}

// BroadcastMultiple broadcasts a text message to multiple sessions given in the sessions slice.
func (m *Melody) BroadcastMultiple(msg []byte, sessions []*Session) error {
	for _, sess := range sessions {
		if writeErr := sess.Write(msg); writeErr != nil {
			return writeErr
		}
	}
	return nil
}

// BroadcastBinary broadcasts a binary message to all sessions.
func (m *Melody) BroadcastBinary(msg []byte) error {
	if m.hub.closed() {
		return ErrClosed
	}

	message := envelope{t: websocket.BinaryMessage, msg: msg}
	m.hub.broadcast(message, false)

	return nil
}

// BroadcastBinaryFilter broadcasts a binary message to all sessions that fn returns true for.
func (m *Melody) BroadcastBinaryFilter(msg []byte, fn func(*Session) bool) error {
	if m.hub.closed() {
		return ErrClosed
	}

	message := envelope{t: websocket.BinaryMessage, msg: msg, filter: fn}
	m.hub.broadcast(message, false)

	return nil
}

// BroadcastBinaryOthers broadcasts a binary message to all sessions except session s.
func (m *Melody) BroadcastBinaryOthers(msg []byte, s *Session) error {
	return m.BroadcastBinaryFilter(msg, func(q *Session) bool {
		return s != q
	})
}

// BroadcastWithResult broadcasts a text message to all sessions and reports
// which sessions rejected it because their bounded send queue was full.
// Delivery to healthy sessions is unaffected by the overflows.
func (m *Melody) BroadcastWithResult(msg []byte) (BroadcastResult, error) {
	if m.hub.closed() {
		return BroadcastResult{}, ErrClosed
	}

	message := envelope{t: websocket.TextMessage, msg: msg}
	return BroadcastResult{Overflows: m.hub.broadcast(message, true)}, nil
}

// BroadcastFilterWithResult broadcasts a text message to the sessions fn
// returns true for and reports per-session queue overflows.
func (m *Melody) BroadcastFilterWithResult(msg []byte, fn func(*Session) bool) (BroadcastResult, error) {
	if m.hub.closed() {
		return BroadcastResult{}, ErrClosed
	}

	message := envelope{t: websocket.TextMessage, msg: msg, filter: fn}
	return BroadcastResult{Overflows: m.hub.broadcast(message, true)}, nil
}

// BroadcastBinaryWithResult broadcasts a binary message to all sessions and
// reports per-session queue overflows.
func (m *Melody) BroadcastBinaryWithResult(msg []byte) (BroadcastResult, error) {
	if m.hub.closed() {
		return BroadcastResult{}, ErrClosed
	}

	message := envelope{t: websocket.BinaryMessage, msg: msg}
	return BroadcastResult{Overflows: m.hub.broadcast(message, true)}, nil
}

// Sessions returns all sessions. An error is returned if the melody session is closed.
func (m *Melody) Sessions() ([]*Session, error) {
	if m.hub.closed() {
		return nil, ErrClosed
	}
	return m.hub.all(), nil
}

// Close closes the melody instance and all connected sessions. It first stops
// accepting new messages and connections, then waits up to
// Config.ShutdownTimeout for the healthy session queues to drain. Sessions that
// cannot drain in time are force-closed with CloseDrainTimeout and their
// undelivered messages are reported through the error handler and a
// *DrainError.
func (m *Melody) Close() error {
	if m.hub.closed() {
		return ErrClosed
	}

	return m.closeWithTimeout(envelope{t: websocket.CloseMessage, msg: []byte{}}, m.Config.ShutdownTimeout)
}

// CloseWithMsg closes the melody instance with the given close payload and all connected sessions.
// Use the FormatCloseMessage function to format a proper close message payload.
func (m *Melody) CloseWithMsg(msg []byte) error {
	if m.hub.closed() {
		return ErrClosed
	}

	return m.closeWithTimeout(envelope{t: websocket.CloseMessage, msg: msg}, m.Config.ShutdownTimeout)
}

// CloseWithTimeout works like CloseWithMsg but bounds the drain phase with the
// given timeout instead of Config.ShutdownTimeout. A timeout of zero only
// enqueues the graceful close frames; sessions that cannot accept them
// immediately are force-closed with CloseDrainTimeout.
func (m *Melody) CloseWithTimeout(msg []byte, timeout time.Duration) error {
	if m.hub.closed() {
		return ErrClosed
	}

	return m.closeWithTimeout(envelope{t: websocket.CloseMessage, msg: msg}, timeout)
}

func (m *Melody) closeWithTimeout(msg envelope, timeout time.Duration) error {
	if drainErr := m.hub.exit(msg, timeout); drainErr != nil {
		return drainErr
	}
	return nil
}

// Len return the number of connected sessions.
func (m *Melody) Len() int {
	return m.hub.len()
}

// IsClosed returns the status of the melody instance.
func (m *Melody) IsClosed() bool {
	return m.hub.closed()
}

// FormatCloseMessage formats closeCode and text as a WebSocket close message.
func FormatCloseMessage(closeCode int, text string) []byte {
	return websocket.FormatCloseMessage(closeCode, text)
}

// BroadcastWithDeadline broadcasts a text message with a custom write deadline.
// If deadline is 0, uses Config.WriteWait.
func (m *Melody) BroadcastWithDeadline(msg []byte, deadline time.Duration) error {
	if m.hub.closed() {
		return ErrClosed
	}

	message := envelope{
		t:         websocket.TextMessage,
		msg:       msg,
		writeWait: deadline,
	}
	m.hub.broadcast(message, false)

	return nil
}

// BroadcastFilterWithDeadline broadcasts a text message to filtered sessions with a custom write deadline.
// If deadline is 0, uses Config.WriteWait.
func (m *Melody) BroadcastFilterWithDeadline(msg []byte, deadline time.Duration, fn func(*Session) bool) error {
	if m.hub.closed() {
		return ErrClosed
	}

	message := envelope{
		t:         websocket.TextMessage,
		msg:       msg,
		filter:    fn,
		writeWait: deadline,
	}
	m.hub.broadcast(message, false)

	return nil
}

// BroadcastBinaryWithDeadline broadcasts a binary message with a custom write deadline.
// If deadline is 0, uses Config.WriteWait.
func (m *Melody) BroadcastBinaryWithDeadline(msg []byte, deadline time.Duration) error {
	if m.hub.closed() {
		return ErrClosed
	}

	message := envelope{
		t:         websocket.BinaryMessage,
		msg:       msg,
		writeWait: deadline,
	}
	m.hub.broadcast(message, false)

	return nil
}

// BroadcastBinaryFilterWithDeadline broadcasts a binary message to filtered sessions with a custom write deadline.
// If deadline is 0, uses Config.WriteWait.
func (m *Melody) BroadcastBinaryFilterWithDeadline(msg []byte, deadline time.Duration, fn func(*Session) bool) error {
	if m.hub.closed() {
		return ErrClosed
	}

	message := envelope{
		t:         websocket.BinaryMessage,
		msg:       msg,
		filter:    fn,
		writeWait: deadline,
	}
	m.hub.broadcast(message, false)

	return nil
}
