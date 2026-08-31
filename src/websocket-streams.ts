// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

/// <reference types="@cloudflare/workers-types" />

// Support for sending WebSockets over RPC, as the `webSocket` property of a `Response`
// representing a completed HTTP upgrade. (`Response.webSocket` is a Cloudflare Workers extension
// to the Fetch API.)
//
// A live socket can't literally be transferred, so we represent it as a pair of streams: the
// sender wraps its socket in a ReadableStream (messages arriving on the socket) and a
// WritableStream (messages to send on the socket), and those are serialized using Cap'n Web's
// existing stream support. That means messages start streaming toward the receiver the moment
// the Response is serialized -- before the receiver even knows they're coming -- and both
// directions get the streams' flow control.
//
// Messages are chunks of type string (text frames) or Uint8Array (binary frames). Closure with a
// code and reason is conveyed in-band as a final `{"close": {"code", "reason"}}` chunk, since
// the streams themselves can only signal an undifferentiated end-of-stream.
//
// On the receiving side, the pair is wrapped back up in a WebSocket-like object
// (TunneledWebSocket below). A tunneled socket is fully functional; in particular it can carry a
// nested Cap'n Web session. __tests__/websocket-tunnel.test.ts proves transport equivalence by
// running the shared session test battery (__tests__/session-battery.ts) over a tunneled socket,
// mirroring how index.test.ts runs the same battery over a direct WebSocket connection.
//
// This file also covers the sender's edge: upgradeWebSocketResponse() spells "answer this fetch
// with this WebSocket" identically on every runtime (only Cloudflare Workers can construct the
// native 101 Response), and the exported WebSocketPair gives non-Workers providers the same
// two-crosswired-halves primitive Workers has, for when the provider *is* the endpoint.

import { StubHook, RpcPayload, streamImpl } from "./core.js";

// The runtime's native WebSocketPair (a Cloudflare Workers API), captured at module load time.
// We must probe the *global* here rather than referencing the bare name below: this module
// exports a `WebSocketPair` of its own, and the module-scope binding would shadow the global --
// making a bare `typeof WebSocketPair` check truthy on every platform and sending
// makeUpgradeResponse() down the workerd-only branch on Node and in browsers.
const nativeWebSocketPair: (new () => { 0: WebSocket, 1: WebSocket }) | undefined =
    (globalThis as any).WebSocketPair;

/**
 * The subset of the WebSocket API that the tunnel relies on. Covers browser WebSockets, `ws` /
 * undici client sockets, Cloudflare Workers WebSockets (which add accept()), one half of a
 * `WebSocketPair`, and a tunneled socket received from another session (which is what we wrap
 * when proxying an already-tunneled socket onward to a third party).
 */
export interface WebSocketLike {
  send(data: string | Uint8Array): void;
  close(code?: number, reason?: string): void;
  accept?(): void;
  addEventListener(type: string, listener: (event: any) => void): void;
  binaryType?: string;
}

// A chunk conveying socket closure, sent as the final chunk before the stream ends.
type CloseRecord = { close: { code: number, reason: string } };

function isCloseRecord(chunk: unknown): chunk is CloseRecord {
  return typeof chunk === "object" && chunk !== null && "close" in chunk;
}

// Coerce a message payload to the types we send as chunks: text stays a string, binary data
// becomes a Uint8Array.
function toStringOrBytes(data: unknown): string | Uint8Array {
  if (typeof data === "string") {
    return data;
  } else if (data instanceof Uint8Array) {
    return data;
  } else if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  } else if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  } else {
    throw new TypeError("Unsupported WebSocket message type.");
  }
}

// Close `socket`, propagating `code` and `reason` when possible. close() only accepts code 1000
// or codes in the range 3000-4999, but a code being propagated from a close *event* can fall
// outside that (e.g. 1005 "no status received" or 1006 "abnormal closure"); codes that can't be
// re-sent are dropped.
function closeSocket(socket: WebSocketLike, code?: number, reason?: string): void {
  try {
    if (code === 1000 || (code !== undefined && code >= 3000 && code <= 4999)) {
      socket.close(code, reason);
    } else {
      socket.close();
    }
  } catch {
    // Probably already closed or closing.
  }
}

// Wraps the sender-side socket in a pair of streams suitable for serialization. Must be called
// at most once per socket: it attaches the socket's event listeners.
export function webSocketToStreams(socket: WebSocketLike)
    : { readable: ReadableStream, writable: WritableStream } {
  // Workers WebSockets must be accept()ed before they can be used; conveniently, they also
  // buffer incoming messages until then, so nothing is lost even though we only get to attach
  // listeners when the Response is serialized. (Sockets without accept() -- e.g. `ws` -- may
  // drop messages that arrive before that point; there's nothing we can do about those, as the
  // serialization layer is the first to see the socket at all.)
  socket.accept?.();

  // Where the socket distinguishes (i.e. in browsers), ask for binary messages as ArrayBuffer
  // rather than Blob, which can't be read synchronously.
  try { socket.binaryType = "arraybuffer"; } catch {}

  let closed = false;

  let readable = new ReadableStream({
    start(controller) {
      socket.addEventListener("message", (event: any) => {
        if (closed) return;
        try {
          controller.enqueue(toStringOrBytes(event.data));
        } catch (err) {
          closed = true;
          try { controller.error(err); } catch {}
          closeSocket(socket);
        }
      });
      socket.addEventListener("close", (event: any) => {
        if (closed) return;
        closed = true;
        try {
          controller.enqueue(
              { close: { code: event.code ?? 1005, reason: event.reason ?? "" } });
          controller.close();
        } catch {}
      });
      socket.addEventListener("error", () => {
        if (closed) return;
        closed = true;
        try { controller.error(new Error("WebSocket failed.")); } catch {}
      });
    },

    cancel() {
      // The receiver released the socket without consuming it (or canceled mid-stream); there's
      // no one left to talk to.
      closed = true;
      closeSocket(socket);
    },
  });

  let writable = new WritableStream({
    write(chunk) {
      if (isCloseRecord(chunk)) {
        closeSocket(socket, chunk.close.code, chunk.close.reason);
      } else {
        socket.send(toStringOrBytes(chunk));
      }
    },
    close() {
      closeSocket(socket);
    },
    abort() {
      closeSocket(socket);
    },
  });

  return { readable, writable };
}

type Listener = (event: any) => void;

// A WebSocket-like object wrapping the receiving ends of a tunneled socket's stream pair. It is
// born in the OPEN state (the underlying socket was already connected when it was serialized)
// and never fires an "open" event -- just like the sockets of a Cloudflare Workers
// WebSocketPair. Implements enough of the WebSocket API to be passed to
// newWebSocketRpcSession().
//
// Lifetime: the streams passed to the constructor are owned by the payload the Response arrived
// in. The socket only takes its own references -- locking the readable and duplicating the
// writable's hook -- when the application first interacts with it: accept(), attaching a
// listener, send(), or close(). This mirrors how an unread ReadableStream is canceled when its
// payload is disposed unless the app locks it: an upgrade Response whose socket nobody touched
// releases both streams when the payload is disposed (e.g. when the RPC call it arrived in
// returns), closing the underlying connection rather than holding it open for a receiver that
// will never use it. In particular, an RPC method that receives an upgrade Response in params
// and wants to keep the socket beyond the call must claim it -- most idiomatically with
// accept(), just like a Workers WebSocket -- before returning.
//
// Note that messages stream in regardless of claiming: they accumulate in the readable's buffer
// (bounded by the streams' flow-control window) so they're already on hand when the app attaches
// its first listener.
export class TunneledWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  #readable: ReadableStream;
  // Hook for the sender's WritableStream. Borrowed from the containing payload until #claim(),
  // an owned dup() thereafter; undefined once released (or if claiming failed).
  #writableHook?: StubHook;
  #claimed = false;
  #writer?: WritableStreamDefaultWriter;
  #readyState: number = TunneledWebSocket.OPEN;
  #listeners = new Map<string, { listener: Listener, once: boolean }[]>();
  #onmessage: Listener | null = null;
  #onclose: Listener | null = null;
  #onerror: Listener | null = null;

  constructor(readable: ReadableStream, writableHook: StubHook) {
    this.#readable = readable;
    this.#writableHook = writableHook;
    writableHook.onBroken((error: any) => this.#fail(error));
  }

  get readyState(): number { return this.#readyState; }

  // (Assigning null clears a handler; that's not an interaction with the socket, so it does not
  // claim it.)
  get onmessage() { return this.#onmessage; }
  set onmessage(listener: Listener | null) { this.#onmessage = listener; if (listener) this.#claim(); }
  get onclose() { return this.#onclose; }
  set onclose(listener: Listener | null) { this.#onclose = listener; if (listener) this.#claim(); }
  get onerror() { return this.#onerror; }
  set onerror(listener: Listener | null) { this.#onerror = listener; if (listener) this.#claim(); }

  // Claims the socket (see class comment). Otherwise a no-op, for compatibility with the Workers
  // WebSocket API; the sender side accepted the real socket when it was serialized.
  accept(): void {
    this.#claim();
  }

  send(data: string | ArrayBuffer | ArrayBufferView): void {
    if (this.#readyState !== TunneledWebSocket.OPEN) {
      throw new Error("Can't call send() on a WebSocket that is closing or closed.");
    }
    this.#claim();
    this.#write(toStringOrBytes(data));
  }

  close(code?: number, reason?: string): void {
    if (this.#readyState >= TunneledWebSocket.CLOSING) return;
    this.#claim();
    this.#readyState = TunneledWebSocket.CLOSING;

    // Closing the sender's socket makes its close event come back through the readable,
    // completing the close.
    if (this.#writer) {
      this.#write({ close: { code: code ?? 1005, reason: reason ?? "" } });
      this.#writer.close().catch(() => {});
    }
  }

  [Symbol.dispose](): void {
    this.close();
    this.#release();
  }

  addEventListener(type: string, listener: Listener, options?: { once?: boolean }): void {
    let list = this.#listeners.get(type);
    if (!list) {
      list = [];
      this.#listeners.set(type, list);
    }
    list.push({ listener, once: !!options?.once });

    // Claim after registering, so that if the tunnel turns out to be gone, this listener still
    // hears the resulting error/close events.
    this.#claim();
  }

  removeEventListener(type: string, listener: Listener): void {
    let list = this.#listeners.get(type);
    let index = list?.findIndex(entry => entry.listener === listener) ?? -1;
    if (index >= 0) {
      list!.splice(index, 1);
    }
  }

  #dispatchEvent(type: string, event: any): void {
    for (let entry of [...this.#listeners.get(type) ?? []]) {
      if (entry.once) this.removeEventListener(type, entry.listener);
      entry.listener(event);
    }
    let handler = (this as any)["on" + type];
    if (typeof handler === "function") handler.call(this, event);
  }

  // Takes our own references to the stream pair and starts dispatching messages. Called on the
  // app's first interaction with the socket; until then, the streams belong to the containing
  // payload.
  #claim(): void {
    if (this.#claimed) return;
    this.#claimed = true;

    if (!this.#writableHook || this.#readyState === TunneledWebSocket.CLOSED) return;

    let writableHook;
    try {
      writableHook = this.#writableHook.dup();
    } catch (err) {
      // The payload was disposed before the app claimed the socket, so the streams have already
      // been released and the sender has closed the connection. Fail asynchronously so that a
      // listener whose registration triggered this claim still hears about it.
      this.#writableHook = undefined;
      queueMicrotask(() => this.#fail(err));
      return;
    }
    this.#writableHook = writableHook;

    // Wrapping the hook in a proxy WritableStream gets us the streams' flow control on sends.
    this.#writer = streamImpl.createWritableStreamFromHook(writableHook).getWriter();

    // Locking the readable prevents payload disposal from canceling it.
    this.#readLoop(this.#readable.getReader()).catch(err => this.#fail(err));
  }

  async #readLoop(reader: ReadableStreamDefaultReader): Promise<void> {
    while (true) {
      let { done, value } = await reader.read();
      if (this.#readyState === TunneledWebSocket.CLOSED) return;

      if (done) {
        // Stream ended without a close record; treat as a closure with no status, like a
        // WebSocket whose connection ended without a Close frame.
        this.#close(1005, "");
        return;
      } else if (isCloseRecord(value)) {
        this.#close(value.close.code, value.close.reason);
        return;
      } else {
        this.#dispatchEvent("message", { type: "message", data: toStringOrBytes(value) });
      }
    }
  }

  #close(code: number, reason: string): void {
    this.#readyState = TunneledWebSocket.CLOSED;
    this.#release();
    this.#dispatchEvent("close", { type: "close", code, reason });
  }

  // Called when the streams or the RPC session failed: act like a failed WebSocket.
  #fail(error: any): void {
    if (this.#readyState === TunneledWebSocket.CLOSED) return;
    this.#readyState = TunneledWebSocket.CLOSED;
    this.#release();
    this.#dispatchEvent("error", { type: "error", error });
    this.#dispatchEvent("close", { type: "close", code: 1006, reason: "WebSocket tunnel failed." });
  }

  // Write a chunk, fire-and-forget. A rejection means the socket or session has failed, which
  // we'll separately hear about through the readable or onBroken().
  #write(chunk: unknown): void {
    this.#writer?.write(chunk).catch(() => {});
  }

  #release(): void {
    // Only dispose the hook if it's our own dup; before #claim() it belongs to the payload.
    if (this.#claimed) this.#writableHook?.dispose();
    this.#writableHook = undefined;
    this.#writer?.close().catch(() => {});
    this.#writer = undefined;
  }
}

// Reconstructs an upgrade Response on the receiving side, given the receiving ends of the stream
// pair.
//
// On Cloudflare Workers, the runtime requires a native WebSocket to complete an HTTP upgrade
// (e.g. by returning the Response from a fetch handler), and can also mint Responses with status
// 101. So there, we create a native WebSocketPair, pump one end to and from the tunneled socket,
// and attach the other end to the Response.
//
// On other platforms the Response carries a TunneledWebSocket directly, and keeps the default
// status 200, since the standard Response constructor refuses to produce 1xx statuses.
export function makeUpgradeResponse(
    readable: ReadableStream, writableHook: StubHook, init: ResponseInit): Response {
  let socket = new TunneledWebSocket(readable, writableHook);

  if (nativeWebSocketPair !== undefined) {
    let pair = new nativeWebSocketPair();
    pumpNativeSocket(pair[1], socket);
    return new Response(null, { ...init, status: 101, webSocket: pair[0] } as ResponseInit);
  } else {
    let response = new Response(null, init);
    Object.defineProperty(response, "webSocket", { value: socket, configurable: true });
    return response;
  }
}

/**
 * Constructs a `Response` that answers a `fetch()` with a WebSocket upgrade, carrying
 * `webSocket` -- the universal spelling of what Cloudflare Workers code writes as
 * `new Response(null, { status: 101, webSocket })`.
 *
 * On Workers this produces exactly that native Response, so it can also be returned straight
 * from a real fetch handler. On every other runtime -- where the standard Response constructor
 * refuses to produce 1xx statuses -- it produces a status-200 `Response` (any `init.status` /
 * `init.statusText` is stripped) carrying `webSocket` as the non-standard own-property. The two
 * are identical on the wire: an upgrade implies status 101, so the status is never serialized.
 * `init.headers` (e.g. a negotiated Sec-WebSocket-Protocol) rides along.
 *
 * The socket's frames begin tunneling toward the receiver the moment the Response is
 * serialized, and a socket can be sent over RPC only once. Note the passthrough caveat: for
 * sockets without accept() (e.g. `ws` or browser sockets being proxied through), frames that
 * arrive between the dial's open and the Response's serialization are dropped -- Cap'n Web is
 * the first to see the socket at serialization time. Either await open and accept that window
 * (fine for servers that speak second), or wrap the socket in a `WebSocketPair` as you dial and
 * answer with the other half (for servers that speak first; the pair buffers).
 */
export function upgradeWebSocketResponse(webSocket: WebSocketLike, init?: ResponseInit): Response {
  // Validate here, at construction, so that a mistake surfaces at the call site rather than
  // deep inside the serializer (or worse, only on the platform you didn't test on).
  let missing = (["send", "close", "addEventListener"] as const)
      .filter(method => typeof (webSocket as any)?.[method] !== "function");
  if (missing.length > 0) {
    throw new TypeError(
        `upgradeWebSocketResponse() expected a WebSocket-like object with callable send(), ` +
        `close(), and addEventListener(), but ${missing.join(", ")} ` +
        `${missing.length === 1 ? "is" : "are"} missing. (Did you pass the whole ` +
        `WebSocketPair instead of one half?)`);
  }
  if (init?.status !== undefined && init.status !== 101) {
    throw new TypeError(
        `A WebSocket upgrade Response implies status 101; got status ${init.status}. Omit ` +
        `init.status (or pass 101).`);
  }

  if (nativeWebSocketPair !== undefined) {
    // Workers can (and must, if this Response is to complete a real upgrade) build the native
    // form directly.
    return new Response(null, { ...init, status: 101, webSocket } as ResponseInit);
  } else {
    let { status, statusText, ...rest } = init ?? {};
    let response = new Response(null, rest);
    Object.defineProperty(response, "webSocket", { value: webSocket, configurable: true });
    return response;
  }
}

// Forward messages and closure between a native WebSocket (one end of a WebSocketPair) and a
// tunneled socket, in both directions.
//
// Note that attaching the pump's listeners claims the tunneled socket immediately, so on Workers
// an ignored upgrade Response does not release the tunnel when its payload is disposed -- we
// can't observe whether the app ever accept()s the native end. The runtime cleans up the pair
// when the session ends.
function pumpNativeSocket(native: WebSocket, tunneled: TunneledWebSocket): void {
  native.accept();

  // Sends can race with closure from the other direction; messages that arrive after the
  // destination has begun closing are dropped, as they would be on a direct connection.
  native.addEventListener("message", event => {
    try { tunneled.send(toStringOrBytes(event.data)); } catch {}
  });
  tunneled.addEventListener("message", event => {
    try { native.send(event.data); } catch {}
  });

  native.addEventListener("close", event => tunneled.close(event.code, event.reason));
  native.addEventListener("error", () => tunneled.close());

  tunneled.addEventListener("close", event => closeSocket(native, event.code, event.reason));
  tunneled.addEventListener("error", () => closeSocket(native));
}

// An event queued for delivery to one half of a JsWebSocketPair.
type PairEvent = { type: "message", data: string | ArrayBuffer }
               | { type: "close", code: number, reason: string };

// Copy an outgoing message into the form we deliver. Binary payloads are copied (and normalized
// to ArrayBuffer, which is what workerd sockets deliver): delivery is asynchronous, so without a
// copy, a sender reusing a scratch buffer would corrupt frames it already "sent".
function copyMessage(data: string | ArrayBuffer | ArrayBufferView): string | ArrayBuffer {
  if (typeof data === "string") {
    return data;
  } else if (data instanceof ArrayBuffer) {
    return data.slice(0);
  } else if (ArrayBuffer.isView(data)) {
    return (data.buffer as ArrayBuffer).slice(data.byteOffset, data.byteOffset + data.byteLength);
  } else {
    throw new TypeError("Unsupported WebSocket message type.");
  }
}

// One half of a pure-JavaScript WebSocketPair, with workerd-faithful semantics. Its event
// plumbing deliberately rhymes with TunneledWebSocket's above (same listener bookkeeping, same
// event shapes) -- but it is a self-contained in-memory pipe, with none of the tunnel's
// claim-on-interaction lifetime rules, so the two are kept as separate classes.
//
// The one rule that makes this a faithful stand-in for the native pair: ALL inbound delivery is
// asynchronous -- each half buffers (without bound, like workerd) until its accept() is called,
// and then drains via a microtask pump, strict FIFO, messages before close. That makes both
// real-world consumption orders correct: webSocketToStreams() calls accept() first and attaches
// its listeners synchronously afterwards (synchronous replay inside accept() would drop every
// buffered frame), while other consumers attach listeners first and call accept() last. It also
// means no listener ever runs synchronously inside the peer's send().
class JsWebSocketHalf {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  // Set once by makePair(); a half never exists without its peer.
  #peer!: JsWebSocketHalf;
  // Halves are born OPEN, like the native pair's; no "open" event ever fires.
  #readyState: number = JsWebSocketHalf.OPEN;
  #accepted = false;
  // Inbound events awaiting delivery. Unbounded, matching workerd's pre-accept buffering.
  #inbox: PairEvent[] = [];
  #drainScheduled = false;
  #listeners = new Map<string, { listener: Listener, once: boolean }[]>();
  #onmessage: Listener | null = null;
  #onclose: Listener | null = null;
  #onerror: Listener | null = null;

  // The serializer (and WebSocketTransport) sets this. Binary messages are always delivered as
  // ArrayBuffer -- workerd behavior -- so there is nothing for it to change.
  binaryType: string = "arraybuffer";

  static makePair(): [JsWebSocketHalf, JsWebSocketHalf] {
    let a = new JsWebSocketHalf();
    let b = new JsWebSocketHalf();
    a.#peer = b;
    b.#peer = a;
    return [a, b];
  }

  get readyState(): number { return this.#readyState; }

  get onmessage() { return this.#onmessage; }
  set onmessage(listener: Listener | null) { this.#onmessage = listener; }
  get onclose() { return this.#onclose; }
  set onclose(listener: Listener | null) { this.#onclose = listener; }
  get onerror() { return this.#onerror; }
  set onerror(listener: Listener | null) { this.#onerror = listener; }

  // Starts delivery. Idempotent, like workerd's.
  accept(): void {
    if (this.#accepted) return;
    this.#accepted = true;
    this.#scheduleDrain();
  }

  send(data: string | ArrayBuffer | ArrayBufferView): void {
    if (!this.#accepted) {
      // workerd's exact message.
      throw new TypeError("You must call accept() on this WebSocket before sending messages.");
    }
    if (this.#readyState !== JsWebSocketHalf.OPEN) {
      throw new Error("Can't call send() on a WebSocket that is closing or closed.");
    }
    this.#peer.#deliver({ type: "message", data: copyMessage(data) });
  }

  close(code?: number, reason?: string): void {
    // workerd-faithful validation: close() can send code 1000 or 3000-4999 (or none, read as
    // 1005 "no status received"), and a close reason is capped by RFC 6455's 125-byte control
    // frame payload minus the 2-byte status code.
    if (code !== undefined && code !== 1000 && !(code >= 3000 && code <= 4999)) {
      throw new TypeError(
          `Invalid WebSocket close code: ${code}. Close codes must be 1000 or in the range ` +
          `3000-4999.`);
    }
    if (reason !== undefined && new TextEncoder().encode(reason).length > 123) {
      throw new TypeError("WebSocket close reason may be at most 123 bytes when UTF-8 encoded.");
    }
    if (this.#readyState >= JsWebSocketHalf.CLOSING) return;
    this.#readyState = JsWebSocketHalf.CLOSING;

    // The closing half stops receiving: whatever was queued for it will never be delivered.
    // (Closing before accept() is legal -- that's how an unsent upgrade Response's socket is
    // released -- the peer, once accepted, still hears the close.)
    this.#inbox.length = 0;

    let close = { type: "close" as const, code: code ?? 1005, reason: reason ?? "" };
    this.#peer.#deliver(close);

    // For an in-memory pipe the close handshake completes immediately, so -- like a real socket
    // whose peer's stack acknowledged the Close frame -- the closing half hears its own close
    // event too (asynchronously, after which both halves are CLOSED). webSocketToStreams()
    // depends on this echo: a close written down the tunnel close()s the wrapped half, and the
    // resulting close event on that same half is what carries the closure back to the tunnel's
    // other end. (Bypasses #deliver(), which drops events once closing has begun.)
    this.#inbox.push({ ...close });
    this.#scheduleDrain();
  }

  [Symbol.dispose](): void {
    this.close();
  }

  addEventListener(type: string, listener: Listener, options?: { once?: boolean }): void {
    let list = this.#listeners.get(type);
    if (!list) {
      list = [];
      this.#listeners.set(type, list);
    }
    list.push({ listener, once: !!options?.once });
  }

  removeEventListener(type: string, listener: Listener): void {
    let list = this.#listeners.get(type);
    let index = list?.findIndex(entry => entry.listener === listener) ?? -1;
    if (index >= 0) {
      list!.splice(index, 1);
    }
  }

  // (Note that "error" listeners are accepted but never dispatched: an in-memory pipe has no
  // transport failures.)
  #dispatchEvent(type: string, event: any): void {
    for (let entry of [...this.#listeners.get(type) ?? []]) {
      if (entry.once) this.removeEventListener(type, entry.listener);
      entry.listener(event);
    }
    let handler = (this as any)["on" + type];
    if (typeof handler === "function") handler.call(this, event);
  }

  // Called by the peer to queue an event for this half. Once this half has begun closing, it no
  // longer receives anything: messages racing with closure are dropped, as they would be on a
  // direct connection.
  #deliver(event: PairEvent): void {
    if (this.#readyState >= JsWebSocketHalf.CLOSING) return;
    this.#inbox.push(event);
    this.#scheduleDrain();
  }

  // The microtask pump: one per half. Scheduling is a no-op until accept().
  #scheduleDrain(): void {
    if (this.#drainScheduled || !this.#accepted || this.#inbox.length === 0) return;
    this.#drainScheduled = true;
    queueMicrotask(() => {
      this.#drainScheduled = false;
      this.#drain();
    });
  }

  #drain(): void {
    // Events appended while draining (e.g. by a listener poking the peer, which synchronously
    // delivers back to us) are picked up by the same loop -- still asynchronous with respect to
    // the send() that queued them.
    while (this.#accepted && this.#inbox.length > 0) {
      let event = this.#inbox.shift()!;
      if (event.type === "close") {
        this.#readyState = JsWebSocketHalf.CLOSED;
        this.#inbox.length = 0;
        this.#dispatchEvent("close", { type: "close", code: event.code, reason: event.reason });
        return;
      } else {
        this.#dispatchEvent("message", { type: "message", data: event.data });
      }
    }
  }
}

// A pure-JavaScript WebSocketPair with the native class's shape: construct it, get two
// crosswired halves at indexes 0 and 1.
class JsWebSocketPair {
  0: JsWebSocketHalf;
  1: JsWebSocketHalf;

  constructor() {
    let [a, b] = JsWebSocketHalf.makePair();
    this[0] = a;
    this[1] = b;
  }
}

// The surface a pair half guarantees on every platform -- the intersection of the native
// workerd WebSocket and JsWebSocketHalf that the endpoint idiom needs. (On workerd the halves
// are genuinely native WebSockets, which carry more.)
type WebSocketPairHalf = {
  accept(): void;
  send(data: string | ArrayBuffer | ArrayBufferView): void;
  close(code?: number, reason?: string): void;
  readonly readyState: number;
  addEventListener(type: string, listener: (event: any) => void,
                   options?: { once?: boolean }): void;
  removeEventListener(type: string, listener: (event: any) => void): void;
};

/**
 * Two crosswired WebSocket halves: whatever is sent on one is received by the other. On
 * Cloudflare Workers this IS the native `WebSocketPair` (aliased at module load, so there is
 * exactly one behavior per platform); elsewhere it is a pure-JavaScript pair with workerd
 * semantics -- halves born OPEN (no "open" event ever fires), each buffering inbound messages
 * without bound until its accept() is called, delivery always asynchronous.
 *
 * Use it when the provider answering an upgrade IS the endpoint, with no underlying socket to
 * pass through:
 *
 *     let pair = new WebSocketPair();
 *     pair[1].accept();
 *     pair[1].addEventListener("message", event => pair[1].send(`echo: ${event.data}`));
 *     return upgradeWebSocketResponse(pair[0]);
 *
 * ...or to wrap an upstream socket whose server speaks first: wire the socket to one half as
 * you dial, and answer with the other half -- the pair buffers frames that would otherwise be
 * dropped before the Response is serialized.
 */
export const WebSocketPair: new () => { 0: WebSocketPairHalf, 1: WebSocketPairHalf } =
    (nativeWebSocketPair ?? JsWebSocketPair) as any;
