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

import { StubHook, RpcPayload, streamImpl } from "./core.js";

// The subset of the WebSocket API that we rely on. Covers browser WebSockets, the `ws` package,
// Cloudflare Workers WebSockets (which add accept()), and TunneledWebSocket itself (which is
// what we wrap when proxying an already-tunneled socket onward to a third party).
interface WebSocketLike {
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

  if (typeof WebSocketPair !== "undefined") {
    let pair = new WebSocketPair();
    pumpNativeSocket(pair[1], socket);
    return new Response(null, { ...init, status: 101, webSocket: pair[0] } as ResponseInit);
  } else {
    let response = new Response(null, init);
    Object.defineProperty(response, "webSocket", { value: socket, configurable: true });
    return response;
  }
}

// =======================================================================================
// Deferred materialization
//
// A deferring session -- the default on Cloudflare Workers, or any session with the
// `deferUpgradeMaterialization` option set -- delivers an upgrade Response's tunneled socket as
// an opaque `{ readable, writable, init }` byte pair (the DeferredWebSocketUpgrade below)
// instead of materializing a socket, so that infrastructure code can carry the tunnel across
// further hops before materializeUpgrade() turns it back into a real socket at the hop that
// serves it.
//
// The pair is BYTE-oriented (chunks are Uint8Arrays): the boundaries the pair exists to cross
// -- in particular native Cloudflare Workers RPC between isolates -- proxy only byte streams,
// refusing streams of arbitrary values. So the tunnel's frames are wrapped in a
// length-prefixed encoding: 1 byte frame type (0 text, 1 binary, 2 close), 4 bytes big-endian
// payload length, then the payload (UTF-8 text, raw bytes, or `{"code","reason"}` JSON
// respectively).
//
// Although the encoding never appears on a Cap'n Web session's own wire, it IS a persistent
// cross-deployment format: the deferring worker and the worker that materializes are deployed
// independently and can run different versions of this library during a rollout. The header
// layout (1-byte type + 4-byte big-endian length) is therefore frozen, and evolution is
// append-only: new frame types may be added, and a decoder that meets an unknown type fails
// loudly (tearing the tunnel down) rather than desynchronizing. Apps must still treat the pair
// as opaque -- the encoding is a contract between two versions of this library, not an API.
//
// The pair inherits the tunnel's flow control end to end: stream acks fire as the pair is
// read, so an in-transit or unconsumed pair keeps the provider throttled to the flow-control
// window, and a deferring receiver pools at most a window's worth of chunks. Once
// materialized, the endpoint behaves like a non-deferred one: messages are dispatched as they
// arrive, and a slow final consumer is absorbed by the native socket's send buffer, as on a
// direct WebSocket.

// The runtime default when a session doesn't set `deferUpgradeMaterialization` explicitly:
// defer on Cloudflare Workers, where a socket materialized at the session endpoint could never
// cross another RPC hop anyway (and the serving isolate rebuilds it with materializeUpgrade());
// materialize elsewhere, where there are no internal RPC hops to worry about and the app wants
// a usable socket directly.
export function deferUpgradesByDefault(): boolean {
  return typeof WebSocketPair !== "undefined";
}

const FRAME_TEXT = 0, FRAME_BINARY = 1, FRAME_CLOSE = 2;

// Far above any message a supported transport delivers (Workers caps incoming WebSocket
// messages at 1 MiB; `ws` defaults to 100 MiB), this bounds what a corrupt length header can
// make the decoder buffer. Exceeding it tears the tunnel down.
const MAX_FRAME_LENGTH = 128 * 1024 * 1024;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function encodeFrame(chunk: unknown): Uint8Array {
  let type: number, payload: Uint8Array;
  if (isCloseRecord(chunk)) {
    type = FRAME_CLOSE;
    payload = textEncoder.encode(
        JSON.stringify({ code: chunk.close.code, reason: chunk.close.reason }));
  } else {
    let data = toStringOrBytes(chunk);
    if (typeof data === "string") {
      type = FRAME_TEXT;
      payload = textEncoder.encode(data);
    } else {
      type = FRAME_BINARY;
      payload = data;
    }
  }
  if (payload.length > MAX_FRAME_LENGTH) {
    // Enforced on both sides: failing here attributes the error at the offending send instead
    // of at the far end's decoder.
    throw new TypeError(`WebSocket tunnel frame of ${payload.length} bytes exceeds the maximum.`);
  }
  let frame = new Uint8Array(5 + payload.length);
  frame[0] = type;
  new DataView(frame.buffer).setUint32(1, payload.length);
  frame.set(payload, 5);
  return frame;
}

// Decodes frames from a byte stream that may be re-chunked at arbitrary boundaries in transit.
class FrameDecoder {
  // Bytes of the frame in progress, kept as a chunk list with a running total so a frame
  // delivered in many pieces is assembled once when complete, not re-copied on every push.
  // Invariant: the buffered bytes always begin at a frame boundary.
  #chunks: Uint8Array[] = [];
  #size = 0;

  // A partial frame is buffered; end-of-stream in this state means truncation, not a clean end.
  get hasPartialFrame(): boolean {
    return this.#size > 0;
  }

  push(bytes: Uint8Array): unknown[] {
    if (bytes.length > 0) {
      this.#chunks.push(bytes);
      this.#size += bytes.length;
    }

    let frames: unknown[] = [];
    while (this.#size >= 5) {
      let header = this.#peek(5);
      let length = new DataView(header.buffer, header.byteOffset).getUint32(1);
      if (length > MAX_FRAME_LENGTH) {
        throw new TypeError(`WebSocket tunnel frame of ${length} bytes exceeds the maximum.`);
      }
      if (this.#size < 5 + length) break;

      let frame = this.#take(5 + length);
      let payload = frame.subarray(5);
      switch (frame[0]) {
        case FRAME_TEXT:
          frames.push(textDecoder.decode(payload));
          break;
        case FRAME_BINARY:
          // Copy: the payload may be a view into a caller's chunk.
          frames.push(payload.slice());
          break;
        case FRAME_CLOSE: {
          let { code, reason } = JSON.parse(textDecoder.decode(payload));
          frames.push({ close: {
            code: typeof code === "number" ? code : 1005,
            reason: typeof reason === "string" ? reason : "",
          } });
          break;
        }
        default:
          throw new TypeError(`Unknown tunnel frame type: ${frame[0]}`);
      }
    }
    return frames;
  }

  // A view whose first n bytes are the first n buffered bytes, without consuming them. May be
  // longer than n (the fast path returns the whole first chunk). Requires n <= #size.
  #peek(n: number): Uint8Array {
    if (this.#chunks[0].length >= n) return this.#chunks[0];
    let out = new Uint8Array(n);
    let pos = 0;
    for (let chunk of this.#chunks) {
      let take = Math.min(n - pos, chunk.length);
      out.set(chunk.subarray(0, take), pos);
      pos += take;
      if (pos === n) break;
    }
    return out;
  }

  // Removes the first n buffered bytes and returns them contiguously.
  #take(n: number): Uint8Array {
    this.#size -= n;
    let first = this.#chunks[0];
    if (first.length === n) {
      return this.#chunks.shift()!;
    } else if (first.length > n) {
      this.#chunks[0] = first.subarray(n);
      return first.subarray(0, n);
    }
    let out = new Uint8Array(n);
    let pos = 0;
    while (pos < n) {
      let chunk = this.#chunks[0];
      let take = Math.min(n - pos, chunk.length);
      out.set(chunk.subarray(0, take), pos);
      pos += take;
      if (take === chunk.length) this.#chunks.shift();
      else this.#chunks[0] = chunk.subarray(take);
    }
    return out;
  }
}

function coerceBytes(chunk: unknown): Uint8Array {
  let bytes = toStringOrBytes(chunk);
  if (typeof bytes === "string") {
    throw new TypeError("Expected bytes on a deferred tunnel stream, got a string.");
  }
  return bytes;
}

// A ReadableStream whose chunks are `getReader()`'s chunks mapped through `transform`, which
// may yield zero or more output chunks per input (a decoder can need more bytes; one byte chunk
// can hold several frames). Built with highWaterMark 0 and a lazy reader thunk so the inner
// stream is locked only when a consumer actually reads: an untouched deferred Response's inner
// streams stay payload-owned and are released on disposal, like an unclaimed TunneledWebSocket.
// (A TransformStream can't do this: pipeThrough locks the inner stream immediately, and the
// default highWaterMark of 1 would pull -- and lock -- at construction time.) `flush` runs at
// end-of-stream; it can throw to turn a truncated stream into an error instead of a clean end.
function transformReadable(getReader: () => ReadableStreamDefaultReader,
    transform: (chunk: unknown) => unknown[], flush?: () => void): ReadableStream {
  let reader: ReadableStreamDefaultReader | undefined;
  return new ReadableStream({
    async pull(controller) {
      reader ??= getReader();
      // Loop until we enqueue something (or end): a transform can consume a chunk without
      // producing output (a decoder mid-frame), and the stream machinery only re-invokes
      // pull() after an enqueue -- returning empty-handed would strand the pending read.
      while (true) {
        let { done, value } = await reader.read();
        if (done) {
          flush?.();
          controller.close();
          return;
        }
        let chunks = transform(value);
        if (chunks.length > 0) {
          for (let chunk of chunks) {
            controller.enqueue(chunk);
          }
          return;
        }
      }
    },
    cancel(reason) {
      reader ??= getReader();
      return reader.cancel(reason);
    },
  }, { highWaterMark: 0 });
}

// The WritableStream counterpart: transforms each incoming chunk and forwards the results to
// the writer, acquired lazily on first use. A transform or flush error means the tunnel's
// framing is broken, so fail closed: abort the inner writable (tearing down the sender's
// socket) rather than leaving it half-open.
function transformWritable(getWriter: () => WritableStreamDefaultWriter,
    transform: (chunk: unknown) => unknown[], flush?: () => void): WritableStream {
  let writer: WritableStreamDefaultWriter | undefined;
  return new WritableStream({
    async write(chunk) {
      writer ??= getWriter();
      try {
        for (let out of transform(chunk)) {
          await writer.write(out);
        }
      } catch (err) {
        writer.abort(err).catch(() => {});
        throw err;
      }
    },
    close() {
      writer ??= getWriter();
      try {
        flush?.();
      } catch (err) {
        writer.abort(err).catch(() => {});
        throw err;
      }
      return writer.close();
    },
    abort(reason) {
      writer ??= getWriter();
      return writer.abort(reason);
    },
  });
}

// The opaque pair a deferring session delivers as `Response.webSocket`. `init` carries the
// provider's upgrade ResponseInit (notably negotiated headers such as Sec-WebSocket-Protocol);
// it is plain data, so infrastructure that forwards the whole pair object across a hop
// preserves the headers for materializeUpgrade() automatically.
export interface DeferredWebSocketUpgrade {
  readable: ReadableStream;
  writable: WritableStream;
  init?: ResponseInit;
}

// Builds the Response a deferring session delivers: the tunnel's value streams wrapped into the
// opaque framed-byte pair. The readable encodes the tunnel's chunks into framed bytes; the
// writable decodes framed bytes and forwards the frames to the sender's WritableStream hook.
// Both inner ends stay owned by the containing payload, exactly like a ReadableStream and
// WritableStream received over RPC as plain values, and are only locked when the pair is used.
export function makeDeferredUpgradeResponse(
    readable: ReadableStream, writableHook: StubHook, init: ResponseInit): Response {
  let decoder = new FrameDecoder();
  let webSocket: DeferredWebSocketUpgrade = {
    readable: transformReadable(() => readable.getReader(), chunk => [encodeFrame(chunk)]),
    writable: transformWritable(
        () => streamImpl.createWritableStreamFromHook(writableHook).getWriter(),
        bytes => decoder.push(coerceBytes(bytes)),
        () => {
          if (decoder.hasPartialFrame) {
            throw new Error("WebSocket tunnel closed with a truncated frame.");
          }
        }),
    init,
  };
  let response = new Response(null, init);
  Object.defineProperty(response, "webSocket", { value: webSocket, configurable: true });
  return response;
}

// Turns a tunneled socket's deferred byte pair -- as delivered by a deferring session -- back
// into a real upgrade Response. This is the
// counterpart the final hop calls: infrastructure code forwards the pair across boundaries
// that can serialize byte streams but not sockets (e.g. Cloudflare Workers RPC between
// isolates), then materializes the socket exactly once, where the 101 is actually returned to
// the client.
//
// The provider's upgrade headers ride on `webSocket.init`; an explicit `init` argument
// replaces it wholesale (merge the provider's headers in yourself if you want both). Reserved
// handshake headers that ride along (Connection, Upgrade, Sec-WebSocket-Accept/-Key/-Version,
// Content-Length, ...) are recomputed or dropped by the runtime when the Response completes a
// real upgrade, so only non-reserved headers such as Sec-WebSocket-Protocol reach the wire;
// callers on in-process hops (service bindings) see init's headers verbatim.
//
// The pair is consumed: both streams are locked synchronously, so a second
// materializeUpgrade() of the same pair throws instead of corrupting the first. On Workers the
// result is pumped by in-memory listeners, so a live materialized tunnel keeps its isolate (or
// Durable Object) resident for the socket's lifetime.
export function materializeUpgrade(
    webSocket: DeferredWebSocketUpgrade, init?: ResponseInit): Response {
  let reader = webSocket.readable.getReader();
  let writer: WritableStreamDefaultWriter;
  try {
    writer = webSocket.writable.getWriter();
  } catch (err) {
    reader.releaseLock();
    throw err;
  }

  // The inverse wrappers of makeDeferredUpgradeResponse's, with the write direction wrapped in
  // a local WritableStream hook so the socket manages it exactly as it manages a hook received
  // over RPC.
  let decoder = new FrameDecoder();
  let readable = transformReadable(() => reader,
      bytes => decoder.push(coerceBytes(bytes)),
      () => {
        if (decoder.hasPartialFrame) {
          throw new Error("WebSocket tunnel ended with a truncated frame.");
        }
      });
  let writableHook = streamImpl.createWritableStreamHook(
      transformWritable(() => writer, chunk => [encodeFrame(chunk)]));
  return makeUpgradeResponse(readable, writableHook, init ?? webSocket.init ?? {});
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
