// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

/// <reference types="@cloudflare/workers-types" />

// Iterate fork: sending WebSockets over RPC, as the `webSocket` property of a `Response` that
// completed an HTTP upgrade (a Cloudflare Workers extension to the Fetch API).
//
// The sender wraps its socket in a ReadableStream (messages arriving on the socket) and a
// WritableStream (messages to send on it), which use Cap'n Web's existing stream support: messages
// start flowing as soon as the Response is serialized, and both directions get flow control.
// Chunks are strings (text frames) or Uint8Arrays (binary frames). Closure is conveyed in-band as a
// final `{"close": {"code", "reason"}}` chunk. The receiver wraps the pair in a TunneledWebSocket.
//
// This file also provides upgradeWebSocketResponse() and a portable WebSocketPair, so providers on
// any runtime can answer upgrade requests the way Workers code does.

import { StubHook, streamImpl } from "./core.js";

// Probe the global: this module's own `WebSocketPair` export shadows the bare name.
const nativeWebSocketPair: (new () => { 0: WebSocket, 1: WebSocket }) | undefined =
    (globalThis as any).WebSocketPair;

/**
 * The subset of the WebSocket API that the tunnel relies on. Covers browser, `ws` and Workers
 * WebSockets, `WebSocketPair` halves, and sockets received through another tunnel.
 */
export interface WebSocketLike {
  send(data: string | Uint8Array): void;
  close(code?: number, reason?: string): void;
  accept?(): void;
  addEventListener(type: string, listener: (event: any) => void): void;
  binaryType?: string;
}

type CloseRecord = { close: { code: number, reason: string } };

function isCloseRecord(chunk: unknown): chunk is CloseRecord {
  return typeof chunk === "object" && chunk !== null && "close" in chunk;
}

// Normalize a message to a chunk: text stays a string, binary data becomes a Uint8Array.
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

// Copy a message, normalizing binary data to an exactly-sized ArrayBuffer (what Workers delivers).
// Queued sends must not observe later changes to the caller's buffer, and received bytes must not
// alias a shared pool. Copying via Uint8Array also turns a SharedArrayBuffer view into a plain
// ArrayBuffer.
function copyMessage(data: string | ArrayBuffer | ArrayBufferView): string | ArrayBuffer {
  if (typeof data === "string") {
    return data;
  } else if (data instanceof ArrayBuffer) {
    return data.slice(0);
  } else if (ArrayBuffer.isView(data)) {
    let copy = new Uint8Array(data.byteLength);
    copy.set(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
    return copy.buffer;
  } else {
    throw new TypeError("Unsupported WebSocket message type.");
  }
}

// Close `socket`, preserving the code and reason where the runtime accepts them, and otherwise
// closing without a code. Reserved observation-only codes can't be sent, and reasons are limited to
// 123 bytes.
function closeSocket(socket: WebSocketLike, code?: number, reason?: string): void {
  if (code !== undefined && code !== 1005 && code !== 1006 && code !== 1015) {
    try {
      let bytes = new TextEncoder().encode(reason ?? "");
      let boundedReason = bytes.length <= 123 ? reason
          : new TextDecoder().decode(bytes.subarray(0, 123), { stream: true });
      socket.close(code, boundedReason);
      return;
    } catch {}
  }
  try { socket.close(); } catch {}
}

// Wrapping a socket is irreversible (even if a later part of serialization fails), so a socket can
// be sent over RPC only once, across all payloads.
const sentWebSockets = new WeakSet<object>();

// Wraps the sender-side socket in a pair of streams suitable for serialization.
export function webSocketToStreams(socket: WebSocketLike)
    : { readable: ReadableStream, writable: WritableStream } {
  if (sentWebSockets.has(socket)) {
    throw new Error("A WebSocket can only be sent over RPC once.");
  }
  sentWebSockets.add(socket);

  // Workers WebSockets must be accept()ed, and buffer incoming messages until then. Sockets without
  // accept() (e.g. `ws`) drop messages that arrive before serialization attaches our listeners.
  socket.accept?.();
  // Browsers would otherwise deliver binary messages as Blobs, which can't be read synchronously.
  try { socket.binaryType = "arraybuffer"; } catch {}

  let closed = false;
  let readableController!: ReadableStreamDefaultController;

  let readable = new ReadableStream({
    start(controller) {
      readableController = controller;
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
      // The receiver released the socket; there's no one left to talk to.
      closed = true;
      closeSocket(socket);
    },
  });

  let writable = new WritableStream({
    write(chunk) {
      if (isCloseRecord(chunk)) {
        closeSocket(socket, chunk.close.code, chunk.close.reason);

        // Complete the close here by echoing the record back through the readable. A WebSocket
        // never fires a close event for its own close() (a pair half stays CLOSING until its peer
        // closes back), so waiting for one could hang the other end forever. The tunnel does not
        // implement half-close: frames the socket delivers after this point are discarded.
        if (!closed) {
          closed = true;
          try {
            readableController.enqueue(
                { close: { code: chunk.close.code, reason: chunk.close.reason } });
            readableController.close();
          } catch {}
        }
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

// Event plumbing shared by TunneledWebSocket and the pure-JS WebSocketPair halves. A throwing
// listener is reported without stopping delivery to other listeners or of later events.
class WebSocketEvents {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  // Binary messages are always delivered as ArrayBuffer, as on Workers.
  binaryType: string = "arraybuffer";

  #listeners = new Map<string, { listener: Listener, once: boolean }[]>();
  #handlers: Record<string, Listener | null> = { message: null, close: null, error: null };

  get onmessage() { return this.#handlers.message; }
  set onmessage(listener: Listener | null) { this.#setHandler("message", listener); }
  get onclose() { return this.#handlers.close; }
  set onclose(listener: Listener | null) { this.#setHandler("close", listener); }
  get onerror() { return this.#handlers.error; }
  set onerror(listener: Listener | null) { this.#setHandler("error", listener); }

  addEventListener(type: string, listener: Listener, options?: { once?: boolean }): void {
    let list = this.#listeners.get(type);
    if (!list) {
      list = [];
      this.#listeners.set(type, list);
    }
    list.push({ listener, once: !!options?.once });
    this.listening();
  }

  removeEventListener(type: string, listener: Listener): void {
    let list = this.#listeners.get(type);
    let index = list?.findIndex(entry => entry.listener === listener) ?? -1;
    if (index >= 0) {
      list!.splice(index, 1);
    }
  }

  // Called after the application registers a listener or handler.
  protected listening(): void {}

  protected emit(type: string, event: any): void {
    for (let entry of [...this.#listeners.get(type) ?? []]) {
      if (entry.once) this.removeEventListener(type, entry.listener);
      try {
        entry.listener.call(this, event);
      } catch (err) {
        console.error(err);
      }
    }
    let handler = this.#handlers[type];
    if (handler) {
      try {
        handler.call(this, event);
      } catch (err) {
        console.error(err);
      }
    }
  }

  #setHandler(type: string, listener: Listener | null): void {
    this.#handlers[type] = listener;
    if (listener) this.listening();
  }
}

// The receiving end of a tunneled socket, usable wherever a WebSocket is expected (including
// newWebSocketRpcSession()). Like a Workers WebSocketPair half, it is born OPEN.
//
// Until the application first interacts with the socket (accept(), a listener or handler, send()
// or close()), the streams belong to the payload the Response arrived in, so an untouched socket is
// released when that payload is disposed, like an unread ReadableStream. A method receiving an
// upgrade Response in its params must claim the socket before returning to keep it, idiomatically
// with accept(). Messages buffer in the readable, subject to flow control, until then.
export class TunneledWebSocket extends WebSocketEvents {
  #readable: ReadableStream;
  // Borrowed from the payload until claimed, then our own dup(); undefined once released.
  #writableHook?: StubHook;
  #claimed = false;
  #writer?: WritableStreamDefaultWriter;
  #readyState: number = TunneledWebSocket.OPEN;

  constructor(readable: ReadableStream, writableHook: StubHook) {
    super();
    this.#readable = readable;
    this.#writableHook = writableHook;
  }

  get readyState(): number { return this.#readyState; }

  // Claims the socket. The sender already accepted the real socket.
  accept(): void {
    this.#claim();
  }

  send(data: string | ArrayBuffer | ArrayBufferView): void {
    if (this.#readyState !== TunneledWebSocket.OPEN) {
      throw new Error("Can't call send() on a WebSocket that is closing or closed.");
    }
    this.#claim();
    // The write may wait for flow control, so copy the bytes now, as a WebSocket does.
    this.#write(toStringOrBytes(copyMessage(data)));
  }

  close(code?: number, reason?: string): void {
    if (this.#readyState >= TunneledWebSocket.CLOSING) return;
    this.#claim();
    this.#readyState = TunneledWebSocket.CLOSING;

    // The sender's edge closes its socket and echoes the record back, completing our close.
    if (this.#writer) {
      this.#write({ close: { code: code ?? 1005, reason: reason ?? "" } });
      this.#writer.close().catch(() => {});
    }
  }

  [Symbol.dispose](): void {
    this.close();
    this.#release();
  }

  protected listening(): void {
    // Claim after registering, so that the new listener hears about a tunnel that is already gone.
    this.#claim();
  }

  #claim(): void {
    if (this.#claimed) return;
    this.#claimed = true;

    if (!this.#writableHook || this.#readyState === TunneledWebSocket.CLOSED) return;

    let writableHook;
    try {
      writableHook = this.#writableHook.dup();
    } catch (err) {
      // The payload was already disposed, releasing the streams. Fail asynchronously so that a
      // listener whose registration triggered this claim hears about it.
      this.#writableHook = undefined;
      queueMicrotask(() => this.#fail(err));
      return;
    }
    this.#writableHook = writableHook;

    // A proxy WritableStream gives sends flow control.
    this.#writer = streamImpl.createWritableStreamFromHook(writableHook).getWriter();

    // Locking the readable prevents payload disposal from canceling it.
    this.#readLoop(this.#readable.getReader()).catch(err => this.#fail(err));
  }

  async #readLoop(reader: ReadableStreamDefaultReader): Promise<void> {
    while (true) {
      let { done, value } = await reader.read();
      if (this.#readyState === TunneledWebSocket.CLOSED) return;

      if (done) {
        // Ended without a close record, like a connection closed without a Close frame.
        this.#close(1005, "");
        return;
      } else if (isCloseRecord(value)) {
        this.#close(value.close.code, value.close.reason);
        return;
      } else {
        this.emit("message", { type: "message", data: copyMessage(toStringOrBytes(value)) });
      }
    }
  }

  #close(code: number, reason: string): void {
    this.#readyState = TunneledWebSocket.CLOSED;
    this.#release();
    this.emit("close", { type: "close", code, reason });
  }

  // The streams or the RPC session failed: act like a failed WebSocket.
  #fail(error: any): void {
    if (this.#readyState === TunneledWebSocket.CLOSED) return;
    this.#readyState = TunneledWebSocket.CLOSED;
    this.#release();
    this.emit("error", { type: "error", error });
    this.emit("close", { type: "close", code: 1006, reason: "WebSocket tunnel failed." });
  }

  // Fire-and-forget: a failed write means the tunnel failed, which the read loop reports.
  #write(chunk: unknown): void {
    this.#writer?.write(chunk).catch(() => {});
  }

  #release(): void {
    // Closing the proxy writer releases our hook after queued writes (including a close record)
    // drain. Disposing the hook directly would drop them.
    if (this.#writer) this.#writer.close().catch(() => {});
    else if (this.#claimed) this.#writableHook?.dispose();
    this.#writableHook = undefined;
    this.#writer = undefined;
  }
}

// Reconstructs an upgrade Response on the receiving side. Workers requires a native WebSocket (and
// status 101) to complete a real HTTP upgrade, so there we pump a native pair to and from the
// tunneled socket. Elsewhere, the Response carries the TunneledWebSocket and keeps status 200,
// since standard Response constructors refuse 1xx statuses.
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
 * Constructs a `Response` answering a `fetch()` with a WebSocket upgrade, which can be sent over
 * RPC. On Cloudflare Workers, given a native WebSocket, this is exactly
 * `new Response(null, { status: 101, webSocket })`. Elsewhere, where Response constructors refuse
 * 1xx statuses, it is a status-200 Response carrying `webSocket` as an own property. Both are
 * identical on the wire. `init.headers` are kept; `init.status` must be omitted or 101.
 *
 * A socket can be sent over RPC only once. Its messages begin tunneling when the Response is
 * serialized; sockets without accept() (e.g. `ws`) drop messages that arrive before then. For
 * servers that speak first, relay the socket through a `WebSocketPair` as you dial and answer with
 * the other half, which buffers.
 */
export function upgradeWebSocketResponse(webSocket: WebSocketLike, init?: ResponseInit): Response {
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

  // The Workers Response constructor only accepts native sockets. Other WebSocketLikes still
  // tunnel over RPC, but can't complete a real upgrade from a fetch handler.
  if (nativeWebSocketPair !== undefined &&
      webSocket instanceof (globalThis as any).WebSocket) {
    return new Response(null, { ...init, status: 101, webSocket } as ResponseInit);
  } else {
    let { status, statusText, ...rest } = init ?? {};
    let response = new Response(null, rest);
    Object.defineProperty(response, "webSocket", { value: webSocket, configurable: true });
    return response;
  }
}

// Forward messages and closure between one end of a native WebSocketPair and a tunneled socket.
// Attaching listeners claims the tunneled socket immediately, so on Workers an ignored upgrade
// Response keeps its tunnel until the session ends.
function pumpNativeSocket(native: WebSocket, tunneled: TunneledWebSocket): void {
  native.accept();

  // Messages racing with closure from the other direction are dropped, as on a direct connection.
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

type PairEvent = { type: "message", data: string | ArrayBuffer }
               | { type: "close", code: number, reason: string };

// One half of the pure-JavaScript WebSocketPair, following native workerd semantics (pinned by
// __tests__/websocket-pair.test.ts). All delivery is asynchronous: each half buffers without bound
// until accept(), then drains in FIFO order from a microtask. So listeners may be attached either
// before or right after accept(), and never run inside the peer's send().
//
// Closure follows RFC 6455 half-close: close() makes a half CLOSING and queues a close event to the
// peer, but the half keeps receiving until the peer closes back. A half never hears its own close.
class JsWebSocketHalf extends WebSocketEvents {
  // Set by makePair(); a half never exists without its peer.
  #peer!: JsWebSocketHalf;
  #readyState: number = JsWebSocketHalf.OPEN;
  #accepted = false;
  // Whether this half called close(). This gates sending, not receiving.
  #sentClose = false;
  // Whether the peer's close event was dispatched here; closing back then completes the handshake.
  #receivedClose = false;
  #inbox: PairEvent[] = [];
  #drainScheduled = false;

  static makePair(): [JsWebSocketHalf, JsWebSocketHalf] {
    let a = new JsWebSocketHalf();
    let b = new JsWebSocketHalf();
    a.#peer = b;
    b.#peer = a;
    return [a, b];
  }

  get readyState(): number { return this.#readyState; }

  accept(): void {
    if (this.#accepted) return;
    this.#accepted = true;
    this.#scheduleDrain();
  }

  send(data: string | ArrayBuffer | ArrayBufferView): void {
    if (!this.#accepted) {
      throw new TypeError(
          "You must call one of accept() or state.acceptWebSocket() on this WebSocket before " +
          "sending messages.");
    }
    if (this.#sentClose) {
      // Note a half that is CLOSING because its peer closed may still send.
      throw new TypeError("Can't call WebSocket send() after close().");
    }
    this.#peer.#deliver({ type: "message", data: copyMessage(data) });
  }

  close(code?: number, reason?: string): void {
    if (!this.#accepted) {
      throw new TypeError(
          "You must call one of accept() or state.acceptWebSocket() on this WebSocket before " +
          "sending messages.");
    }
    // As on workerd, a repeated close() is ignored before its arguments are validated.
    if (this.#sentClose) return;

    if (code !== undefined &&
        !(code >= 1000 && code <= 4999 &&
          code !== 1004 && code !== 1005 && code !== 1006 && code !== 1015)) {
      throw new TypeError(`Invalid WebSocket close code: ${code}.`);
    }
    if (reason !== undefined && code === undefined) {
      throw new TypeError(
          "If you specify a WebSocket close reason, you must also specify a code.");
    }

    this.#sentClose = true;
    this.#readyState =
        this.#receivedClose ? JsWebSocketHalf.CLOSED : JsWebSocketHalf.CLOSING;
    this.#peer.#deliver({ type: "close", code: code ?? 1005, reason: reason ?? "" });
  }

  // No state guard: a closing half keeps receiving, and a close event is always the last one a
  // peer delivers.
  #deliver(event: PairEvent): void {
    this.#inbox.push(event);
    this.#scheduleDrain();
  }

  #scheduleDrain(): void {
    if (this.#drainScheduled || !this.#accepted || this.#inbox.length === 0) return;
    this.#drainScheduled = true;
    queueMicrotask(() => {
      this.#drainScheduled = false;
      this.#drain();
    });
  }

  #drain(): void {
    // Also picks up events that listeners cause the peer to deliver while we drain.
    while (this.#accepted && this.#inbox.length > 0) {
      let event = this.#inbox.shift()!;
      if (event.type === "close") {
        this.#receivedClose = true;
        // Update the state before dispatching, as native does.
        this.#readyState =
            this.#sentClose ? JsWebSocketHalf.CLOSED : JsWebSocketHalf.CLOSING;
        this.emit("close",
            { type: "close", code: event.code, reason: event.reason, wasClean: true });
        return;
      } else {
        this.emit("message", { type: "message", data: event.data });
      }
    }
  }
}

class JsWebSocketPair {
  0: JsWebSocketHalf;
  1: JsWebSocketHalf;

  constructor() {
    let [a, b] = JsWebSocketHalf.makePair();
    this[0] = a;
    this[1] = b;
  }
}

// The surface a pair half provides on every platform.
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
 * Cloudflare Workers this is the native `WebSocketPair`. Elsewhere it is a pure-JavaScript pair
 * with workerd semantics: halves are born OPEN, buffer inbound messages until `accept()`, deliver
 * asynchronously, and follow RFC 6455 half-close. Unlike native, it copies binary data at `send()`,
 * delivers plain-object events, keeps delivering after a listener throws, and always completes a
 * close handshake cleanly.
 *
 * Use it when the provider answering an upgrade is itself the endpoint:
 *
 *     let pair = new WebSocketPair();
 *     pair[1].accept();
 *     pair[1].addEventListener("message", event => pair[1].send(`echo: ${event.data}`));
 *     return upgradeWebSocketResponse(pair[0]);
 */
export const WebSocketPair: new () => { 0: WebSocketPairHalf, 1: WebSocketPairHalf } =
    (nativeWebSocketPair ?? JsWebSocketPair) as any;
