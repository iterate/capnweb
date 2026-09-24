# Iterate fork of Cap'n Web

This fork is published as `@iterate-com/capnweb`. It is one commit on top of an unmodified upstream
`capnweb` release, currently 0.12.0. To install it under the upstream name:

```
npm i capnweb@npm:@iterate-com/capnweb
```

## Extensions

### WebSocket upgrade Responses over RPC

A `Response` with a `webSocket` property (a Workers upgrade Response) can be sent over RPC. It
travels as a pair of streams: see `protocol.md` and `src/websocket-streams.ts`. Messages start
flowing when the Response is serialized, and use stream flow control.

- On Workers, the receiver gets a status 101 Response with a native `WebSocket`, which a fetch
  handler can return to complete a real upgrade. Elsewhere, it gets a status 200 Response whose
  `webSocket` is a WebSocket-like object that can carry a nested `newWebSocketRpcSession()`.
  Detect upgrades by `webSocket`, not status.
- Binary messages arrive as exactly-sized `ArrayBuffer`s. Sends are copied immediately.
- A socket that the receiver never touches is released with its payload, like an unread
  `ReadableStream`. A method that receives an upgrade Response as an argument must interact with
  the socket, typically by calling `accept()`, before returning if it wants to keep it.
- Close codes and reasons propagate where the runtime accepts them. A tunnel-initiated close is
  completed at the sender's edge, so the tunnel does not support half-close.
- A socket can be sent over RPC only once. Bare `WebSocket`s are not serializable.

`upgradeWebSocketResponse(socket, init?)` builds an upgrade Response on any runtime.
`WebSocketPair` is the native class on Workers and a pure-JavaScript pair with workerd semantics
elsewhere, for providers that are themselves the endpoint. Sockets without `accept()` (such as
`ws`) drop messages that arrive before serialization. For servers that speak first, relay them
through a `WebSocketPair` as you dial.

### `onCall`

`RpcSessionOptions.onCall` wraps each local application function invoked by the peer, including
calls delivered through promise pipelining. It adds nothing to the wire. The hook must call
`invoke()` synchronously and exactly once, and return its result. Otherwise the call rejects.
Throwing before invoking rejects the call and releases its arguments. If the hook fails after
invoking, the application's result is released.

Property reads, calls inside `map()` callbacks, and calls forwarded to another session bypass the
hook. If a method returns a pipelined `RpcPromise`, the hook completes before that promise
resolves. It is not an authorization boundary.

## Known limitations

- Only fork peers understand upgrade Responses; upstream aborts the session on them. There is no
  agreed upstream format yet ([#187](https://github.com/cloudflare/capnweb/issues/187)).
- Await an upgrade Response before forwarding it to another session. Forwarding an unresolved
  promise fails with an owned-payload error (compare
  [#228](https://github.com/cloudflare/capnweb/issues/228)).
- On Workers, the receiver eagerly claims the tunnel, so an ignored upgrade Response keeps it until
  the session ends. Sockets accepted for hibernation cannot be tunneled.
- `send()` has no `bufferedAmount`, so outgoing messages can queue without bound.
- Once serialization has started on a socket, it is consumed, even if a later value in the same
  payload fails to serialize.
- Ping and pong frames are not forwarded.

## Maintenance

Keep the patch small: rebuild it as one commit on each new upstream release tag instead of merging
upstream into fork history. Upstream files stay identical except for the WebSocket and `onCall`
changes in `src/`, one flow-control fix (below), their tests, `protocol.md`, the README banner, the
package name and repository metadata, the changesets repository, and these workflow changes:

- Cloudflare-only jobs (`bonk`, `bonk-pr-review` and `cla`) are guarded to run only in the
  `cloudflare` organization. Upstream's changesets release jobs already are.
- `release.yml` adds `fork-test` and `fork-publish` jobs. They run `test.yml` and then publish to
  npm when a `v<version>` tag matching `package.json` is pushed.
- `test.yml` runs on pull requests to any branch and can be called by `release.yml`.

Prefer upstream fixes to fork-only changes, and drop fork code once upstream covers it.

One general fix is carried in `src/streams.ts`. With a coarse clock, `FlowController` can measure
a zero round trip and compute a `NaN` window, which blocks the sender forever. Upstream's WebKit
test "applies backpressure when window fills up" is intermittently flaky because of this. With the
fork's extra tests running alongside it, the test failed consistently in local `npm run test:ci`
runs. Drop the guard once upstream fixes this.

To release, run `npx changeset version`, commit the result, and push a matching `v<version>` tag.
Fork versions share the upstream base's major and minor version.
