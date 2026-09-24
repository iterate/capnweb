---
"@iterate-com/capnweb": patch
---

Fix WebSocket tunnel lifecycle and `onCall` ownership.

- Tunneled sockets flush queued messages and their close before release, copy binary sends, and
  deliver binary messages as exactly-sized `ArrayBuffer`s instead of `Uint8Array`s. A throwing
  listener no longer fails the tunnel. Close codes such as 1001 and 1011 are preserved where the
  runtime allows. A socket can no longer be sent in two separate calls. Tunnels no longer retain a
  session callback after they close.
- `onCall` must call `invoke()` synchronously and exactly once, and return its result. Otherwise
  the call rejects; previously a hook that awaited before invoking still ran the application.
  Arguments are released when a hook rejects without invoking, and the result is released when a
  hook fails after invoking.
- Stream flow control ignores zero-length round trips from coarse clocks, which could otherwise
  block a writer forever.
