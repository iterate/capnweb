---
"@iterate-com/capnweb": minor
---

Add `upgradeWebSocketResponse()` and a universal `WebSocketPair` -- the blessed way to answer a fetch with a WebSocket upgrade from any runtime. `upgradeWebSocketResponse(socket, init?)` spells "answer this fetch with this WebSocket" identically everywhere: on Cloudflare Workers it builds the native `new Response(null, { status: 101, webSocket })`, elsewhere a wire-equivalent status-200 Response carrying the socket (an upgrade's status is never serialized). The exported `WebSocketPair` is the native class on Workers and a workerd-faithful pure-JS pair elsewhere (halves born OPEN, unbounded buffering until `accept()`, strictly asynchronous delivery), for providers that are themselves the endpoint or that wrap a speak-first upstream socket. The `WebSocketLike` interface the tunnel accepts is now exported too.
