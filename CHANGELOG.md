## Unreleased (v1.5.0)

* Session-level backpressure and isolation.
* Per-session bounded send queues that never block broadcasts on a network write.
* `Config.BufferOverflow` policy: return an overflow result or disconnect the slow session with a recognizable close reason (`CloseSessionQueueFull`).
* `BroadcastWithResult` / `BroadcastFilterWithResult` / `BroadcastBinaryWithResult` report per-session queue overflows.
* Safe hub shutdown: stop receiving, drain healthy queues within `Config.ShutdownTimeout`, force-close and mark timed-out queues as discarded (`CloseDrainTimeout`, `ErrMessageDropped`, `*DrainError`).
* `CloseWithTimeout`, `Session.CloseCode` and `Session.CloseText`.
* Write pumps and ping tickers are always reclaimed; close channels are never sent on after close.

## 2025-10-28 (v1.4.0)

* Support per-message write deadlines by @troutowicz (PR-100)

## 2025-06-08 (v1.3.0)

* Refactor and simplify hub by @TheSven73 (PR-98).

## 2022-09-12 (v1.1.0)

* Create Go module.
* Update examples.
* Fix concurrent panic (PR-65).
* Add `Sessions` to get all sessions (PR-53).
* Add `LocalAddr` and `RemoteAddr` (PR-55).

## 2017-05-18

* Fix `HandleSentMessageBinary`.

## 2017-04-11

* Allow any origin by default.
* Add `BroadcastMultiple`.

## 2017-04-09

* Add control message support.
* Add `IsClosed` to Session.

## 2017-02-10

* Return errors for some exposed methods.
* Add `HandleRequestWithKeys`.
* Add `HandleSentMessage` and `HandleSentMessageBinary`.

## 2017-01-20

* Add `Len()` to fetch number of connected sessions.

## 2016-12-09

* Add metadata management for sessions.

## 2016-05-09

* Add method `HandlePong` to melody instance.

## 2015-10-07

* Add broadcast methods for binary messages.

## 2015-09-03

* Add `Close` method to melody instance.

### 2015-06-10

* Support for binary messages.
* BroadcastOthers method.
