package melody

import (
	"errors"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

// bigMsg exceeds what a loopback socket buffer can absorb once the client
// stops reading, forcing the server write pump to block on its deadline.
var bigMsg = make([]byte, 16*1024)

// slowConfig gives every session a tiny queue and a write deadline longer
// than the shutdown timeouts used in the drain tests, so a stalled pump is
// still blocked when the drain deadline expires.
func slowConfig(m *Melody) {
	m.Config.MessageBufferSize = 2
	m.Config.WriteWait = 2 * time.Second
}

// dialSlow opens a client connection whose data is never read, so its
// server-side write pump eventually stalls and the bounded send queue can
// fill up.
func dialSlow(t *testing.T, url string) *websocket.Conn {
	conn := MustNewDialer(url)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// findSession returns the server-side session backed by clientConn. The
// client's LocalAddr is the address as seen by the server (its RemoteAddr).
func findSession(t *testing.T, sessions []*Session, clientConn *websocket.Conn) *Session {
	t.Helper()
	want := clientConn.LocalAddr().String()
	for _, s := range sessions {
		if s.conn.RemoteAddr().String() == want {
			return s
		}
	}
	t.Fatalf("no session found for client %s", want)
	return nil
}

// stallSession keeps writing bigMsg directly to s until its bounded queue
// overflows and stays full, i.e. the write pump is demonstrably blocked on the
// non-reading client. Only s is written, so sibling sessions are unaffected.
func stallSession(t *testing.T, s *Session) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		err := s.Write(bigMsg)
		if errors.Is(err, ErrMessageBufferFull) {
			// Confirm the pump cannot make progress rather than being
			// briefly behind. Under the disconnect policy the session may
			// already be closing; either state means it is the slow one.
			time.Sleep(30 * time.Millisecond)
			if s.IsClosed() || len(s.output) == cap(s.output) {
				return
			}
		}
		if err != nil && !errors.Is(err, ErrMessageBufferFull) &&
			!errors.Is(err, ErrSessionClosed) {
			t.Fatalf("unexpected write error: %v", err)
		}
	}
	t.Fatal("session queue never filled")
}

// readUntilError drains frames until the connection reports a terminal read
// error, which must happen within the deadline. Buffered frames may arrive
// first on some platforms.
func readUntilError(t *testing.T, conn *websocket.Conn) error {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var err error
	for {
		if _, _, err = conn.ReadMessage(); err != nil {
			return err
		}
	}
}

// readUntilMessage drains frames until one equals want or a read error
// occurs. It tolerates unrelated frames queued ahead of want.
func readUntilMessage(t *testing.T, conn *websocket.Conn, want []byte) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("did not receive expected message: %v", err)
		}
		if string(msg) == string(want) {
			return
		}
	}
}

func TestOverflowDisconnectsSlowSession(t *testing.T) {
	ws := NewTestServer()
	slowConfig(ws.m)
	ws.m.Config.BufferOverflow = DisconnectSlowSession

	var disconnected atomic.Bool
	var gotCode atomic.Int32
	ws.m.HandleDisconnect(func(s *Session) {
		gotCode.Store(int32(s.CloseCode()))
		disconnected.Store(true)
	})

	server := httptest.NewServer(ws)
	defer server.Close()

	slow := dialSlow(t, server.URL)
	healthy := MustNewDialer(server.URL)
	defer healthy.Close()

	ss, err := ws.m.Sessions()
	assert.Nil(t, err)
	assert.Len(t, ss, 2)

	slowSess := findSession(t, ss, slow)
	stallSession(t, slowSess)

	// The slow session must be torn down with a recognizable reason.
	assert.Eventually(t, disconnected.Load, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, CloseSessionQueueFull, int(gotCode.Load()))
	assert.Equal(t, closeReasonQueueFull, slowSess.CloseText())
	assert.True(t, slowSess.IsClosed())

	// The client's read must end with an error (exact wire error depends on
	// whether the best-effort close frame beat the teardown).
	assert.NotNil(t, readUntilError(t, slow))

	// The healthy session is untouched and keeps receiving broadcasts.
	assert.Nil(t, ws.m.Broadcast(TestMsg))
	readUntilMessage(t, healthy, TestMsg)
}

func TestOverflowReturnKeepsSessionOpen(t *testing.T) {
	ws := NewTestServer()
	slowConfig(ws.m)
	ws.m.Config.BufferOverflow = ReturnOverflowError

	server := httptest.NewServer(ws)
	defer server.Close()

	slow := dialSlow(t, server.URL)
	healthy := MustNewDialer(server.URL)
	defer healthy.Close()

	ss, err := ws.m.Sessions()
	assert.Nil(t, err)
	assert.Len(t, ss, 2)

	slowSess := findSession(t, ss, slow)
	stallSession(t, slowSess)

	// Result-reporting broadcast lists only the overflowing session and does
	// not block on its network write.
	result, err := ws.m.BroadcastWithResult(TestMsg)
	assert.Nil(t, err)
	assert.Len(t, result.Overflows, 1)
	assert.Same(t, slowSess, result.Overflows[0])

	// The slow session stays open and the healthy one still gets the message.
	assert.False(t, slowSess.IsClosed())
	readUntilMessage(t, healthy, TestMsg)
}

func TestBroadcastIsolationDoesNotBlockOnSlowSession(t *testing.T) {
	ws := NewTestServer()
	slowConfig(ws.m)
	ws.m.Config.BufferOverflow = ReturnOverflowError

	server := httptest.NewServer(ws)
	defer server.Close()

	slow := dialSlow(t, server.URL)
	healthy := MustNewDialer(server.URL)
	defer healthy.Close()

	ss, err := ws.m.Sessions()
	assert.Nil(t, err)
	slowSess := findSession(t, ss, slow)
	stallSession(t, slowSess)

	// Repeated broadcasts must return promptly even though the slow session
	// cannot make network progress.
	done := make(chan struct{})
	started := time.Now()
	go func() {
		for i := 0; i < 50; i++ {
			_ = ws.m.Broadcast(TestMsg)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcast blocked on the slow session queue")
	}
	assert.Less(t, time.Since(started), 500*time.Millisecond)

	// The healthy session receives the broadcast promptly despite the slow one.
	readUntilMessage(t, healthy, TestMsg)

	// Slow session remains isolated and open under the return-error policy.
	assert.False(t, slowSess.IsClosed())
}

func TestHealthySessionMessageOrdering(t *testing.T) {
	ws := NewTestServer()
	server := httptest.NewServer(ws)
	defer server.Close()

	conn := MustNewDialer(server.URL)
	defer conn.Close()

	ss, err := ws.m.Sessions()
	assert.Nil(t, err)
	assert.Len(t, ss, 1)
	s := ss[0]

	const n = 200
	for i := 0; i < n; i++ {
		assert.Nil(t, s.Write([]byte{byte(i)}))
	}

	for i := 0; i < n; i++ {
		_, msg, err := conn.ReadMessage()
		assert.Nil(t, err)
		assert.Len(t, msg, 1)
		assert.Equal(t, byte(i), msg[0])
	}
}

func TestCloseDrainsHealthyQueues(t *testing.T) {
	ws := NewTestServer()
	ws.m.Config.ShutdownTimeout = 5 * time.Second

	var disconnects atomic.Int32
	ws.m.HandleDisconnect(func(s *Session) {
		disconnects.Add(1)
	})

	server := httptest.NewServer(ws)
	defer server.Close()

	const n = 5
	conns := make([]*websocket.Conn, n)
	for i := range conns {
		conns[i] = MustNewDialer(server.URL)
		defer conns[i].Close()
	}

	// Queue an identical burst on every session via per-session writes, then
	// shut down. Every queued message must arrive before the close frame.
	const perSession = 50
	ss, err := ws.m.Sessions()
	assert.Nil(t, err)
	for _, s := range ss {
		for i := 0; i < perSession; i++ {
			assert.Nil(t, s.Write(TestMsg))
		}
	}

	assert.Nil(t, ws.m.Close())
	assert.True(t, ws.m.IsClosed())

	for _, conn := range conns {
		for i := 0; i < perSession; i++ {
			_, msg, err := conn.ReadMessage()
			assert.Nil(t, err)
			assert.Equal(t, TestMsg, msg)
		}
		// Next read must end with a terminal close error.
		assert.NotNil(t, readUntilError(t, conn))
	}

	assert.Eventually(t, func() bool { return disconnects.Load() == n }, 2*time.Second, 5*time.Millisecond)
}

func TestCloseDrainTimeoutMarksDiscards(t *testing.T) {
	ws := NewTestServer()
	slowConfig(ws.m)
	// A zero drain timeout deterministically force-closes every session that
	// still holds queued messages, with no dependence on wall-clock scheduling.
	ws.m.Config.WriteWait = time.Minute

	var dropped atomic.Bool
	ws.m.HandleError(func(s *Session, err error) {
		if errors.Is(err, ErrMessageDropped) {
			dropped.Store(true)
		}
	})

	server := httptest.NewServer(ws)
	defer server.Close()

	slow := dialSlow(t, server.URL)
	healthy := MustNewDialer(server.URL)
	defer healthy.Close()

	ss, err := ws.m.Sessions()
	assert.Nil(t, err)
	slowSess := findSession(t, ss, slow)
	stallSession(t, slowSess)

	// Re-assert the stall immediately before shutdown: the non-reading client
	// cannot drain, so the queue must still be full.
	assert.Eventually(t, func() bool {
		return len(slowSess.output) == cap(slowSess.output)
	}, time.Second, 5*time.Millisecond)

	start := time.Now()
	err = ws.m.CloseWithTimeout(nil, 0)
	elapsed := time.Since(start)

	// Shutdown returns promptly instead of waiting on the stuck writer.
	assert.Less(t, elapsed, 2*time.Second)

	var drainErr *DrainError
	assert.ErrorAs(t, err, &drainErr)
	assert.Contains(t, drainErr.Dropped, slowSess)
	assert.Greater(t, drainErr.Dropped[slowSess], 0)
	assert.True(t, dropped.Load())

	// The timed-out session is force-closed with the recognizable reason.
	assert.Eventually(t, slowSess.IsClosed, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, CloseDrainTimeout, slowSess.CloseCode())
	assert.Equal(t, closeReasonDrainTimeout, slowSess.CloseText())

	// The healthy session drains its (empty) queue and gets a terminal error.
	assert.NotNil(t, readUntilError(t, healthy))
}

func TestCloseAfterHubClosedReturnsErrClosed(t *testing.T) {
	ws := NewTestServer()
	ws.m.Config.ShutdownTimeout = 50 * time.Millisecond
	server := httptest.NewServer(ws)
	defer server.Close()

	conn := MustNewDialer(server.URL)
	defer conn.Close()

	assert.Nil(t, ws.m.Close())
	assert.ErrorIs(t, ws.m.Close(), ErrClosed)
	assert.ErrorIs(t, ws.m.CloseWithMsg(TestMsg), ErrClosed)
	assert.ErrorIs(t, ws.m.CloseWithTimeout(TestMsg, time.Second), ErrClosed)
}

func TestDisconnectHandlerFiresExactlyOnce(t *testing.T) {
	ws := NewTestServer()
	slowConfig(ws.m)
	ws.m.Config.BufferOverflow = DisconnectSlowSession

	var disconnects atomic.Int32
	ws.m.HandleDisconnect(func(s *Session) {
		disconnects.Add(1)
	})

	server := httptest.NewServer(ws)
	defer server.Close()

	conn := dialSlow(t, server.URL)

	ss, err := ws.m.Sessions()
	assert.Nil(t, err)
	assert.Len(t, ss, 1)
	s := ss[0]
	stallSession(t, s)

	// Hammer the closing session concurrently; only one disconnect may happen.
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Write(bigMsg)
			_ = s.Close()
			_ = s.CloseWithMsg(TestMsg)
		}()
	}
	wg.Wait()

	assert.Eventually(t, s.IsClosed, 2*time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(1), disconnects.Load())

	// Client sees a terminal error.
	assert.NotNil(t, readUntilError(t, conn))
}

func TestCloseReasonForClientInitiatedClose(t *testing.T) {
	res := make(chan *Session, 1)

	ws := NewTestServer()
	ws.m.HandleDisconnect(func(s *Session) {
		res <- s
	})

	server := httptest.NewServer(ws)
	defer server.Close()

	conn := MustNewDialer(server.URL)
	defer conn.Close()

	code := ClosePolicyViolation
	text := "bye"
	assert.Nil(t, conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(code, text)))

	s := <-res
	assert.Equal(t, code, s.CloseCode())
	assert.Equal(t, text, s.CloseText())
}

func TestNoLeakedWriterGoroutinesOrTickers(t *testing.T) {
	ws := NewTestServer()
	ws.m.Config.PingPeriod = 10 * time.Millisecond
	server := httptest.NewServer(ws)
	defer server.Close()

	const n = 10
	conns := make([]*websocket.Conn, n)
	for i := range conns {
		conns[i] = MustNewDialer(server.URL)
	}

	// Wait for every session to register.
	assert.Eventually(t, func() bool { return ws.m.Len() == n }, 2*time.Second, 5*time.Millisecond)

	// Well-behaved clients drain until the close frame before tearing down,
	// giving the server pumps room to finish.
	for _, conn := range conns {
		go func(c *websocket.Conn) {
			for {
				if _, _, err := c.ReadMessage(); err != nil {
					return
				}
			}
		}(conn)
	}
	for _, conn := range conns {
		assert.Nil(t, conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(CloseNormalClosure, "")))
	}

	// After disconnect the per-session write pumps (and their tickers) must be
	// gone; otherwise HandleRequest would still be blocked in pumpDone.
	assert.Eventually(t, func() bool { return ws.m.Len() == 0 }, 2*time.Second, 5*time.Millisecond)

	before := runtime.NumGoroutine()
	time.Sleep(100 * time.Millisecond)
	assert.LessOrEqual(t, runtime.NumGoroutine(), before)
}
