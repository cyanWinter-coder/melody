package melody

import "time"

// BufferOverflowPolicy decides what happens to a session whose bounded send
// queue is full when a new message is enqueued.
type BufferOverflowPolicy int

const (
	// ReturnOverflowError keeps the session open and reports the overflow to
	// the caller (and, for broadcasts, in BroadcastResult.Overflows) without
	// dropping the offered message.
	ReturnOverflowError BufferOverflowPolicy = iota

	// DisconnectSlowSession closes the session with CloseSessionQueueFull when
	// its send queue is full, isolating it from the rest of the hub.
	DisconnectSlowSession
)

// Config melody configuration struct.
type Config struct {
	WriteWait                 time.Duration        // Duration until write times out.
	PongWait                  time.Duration        // Timeout for waiting on pong.
	PingPeriod                time.Duration        // Duration between pings.
	MaxMessageSize            int64                // Maximum size in bytes of a message.
	MessageBufferSize         int                  // The max amount of messages that can be in a sessions buffer before it starts dropping them.
	ConcurrentMessageHandling bool                 // Handle messages from sessions concurrently.
	BufferOverflow            BufferOverflowPolicy // What happens when a session send queue is full.
	ShutdownTimeout           time.Duration        // Maximum time Close/CloseWithMsg wait for healthy queues to drain.
}

func newConfig() *Config {
	return &Config{
		WriteWait:         10 * time.Second,
		PongWait:          60 * time.Second,
		PingPeriod:        54 * time.Second,
		MaxMessageSize:    512,
		MessageBufferSize: 256,
		BufferOverflow:    ReturnOverflowError,
		ShutdownTimeout:   10 * time.Second,
	}
}
