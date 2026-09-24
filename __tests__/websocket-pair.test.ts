// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

// Unit tests for the pure-JavaScript WebSocketPair and for upgradeWebSocketResponse() -- no RPC
// involved. These pin the workerd-faithful semantics the pair promises: halves born OPEN,
// unbounded buffering until accept(), strictly asynchronous FIFO delivery (which makes both the
// serializer's accept-then-listen order and the listen-then-accept order correct), and workerd's
// close and validation behavior. This file runs under Node, where no native WebSocketPair
// exists, so it exercises the pure-JS implementation; workerd.test.ts covers the native alias.

import { expect, it, describe, vi } from "vitest";
import { upgradeWebSocketResponse, WebSocketPair } from "../src/index.js";

// Let the pair's microtask pumps run. A few hops, since a delivery can schedule another.
async function pumpMicrotasks(): Promise<void> {
  for (let i = 0; i < 16; i++) {
    await Promise.resolve();
  }
}

function collectMessages(half: { addEventListener(type: string, l: (e: any) => void): void }) {
  let messages: unknown[] = [];
  half.addEventListener("message", event => messages.push(event.data));
  return messages;
}

describe("pure-JS WebSocketPair", () => {
  it("echoes in both directions, text and binary, in FIFO order", async () => {
    let pair = new WebSocketPair();
    pair[0].accept();
    pair[1].accept();

    // pair[1] echoes everything back, marking text so we can see it made the round trip.
    pair[1].addEventListener("message", event => {
      pair[1].send(typeof event.data === "string" ? `echo:${event.data}` : event.data);
    });

    let received = collectMessages(pair[0]);
    pair[0].send("one");
    pair[0].send(new Uint8Array([1, 2, 3]));
    pair[0].send("two");
    await pumpMicrotasks();

    expect(received.length).toBe(3);
    expect(received[0]).toBe("echo:one");
    // Binary frames are delivered as ArrayBuffer (workerd behavior).
    expect(received[1]).toBeInstanceOf(ArrayBuffer);
    expect(Array.from(new Uint8Array(received[1] as ArrayBuffer))).toEqual([1, 2, 3]);
    expect(received[2]).toBe("echo:two");
  });

  it("buffers frames until accept() and replays them asynchronously", async () => {
    // The serializer's consumption order: webSocketToStreams() calls accept() FIRST and attaches
    // its listeners synchronously afterwards. If accept() replayed the buffer synchronously,
    // every buffered frame would be dropped.
    let pair = new WebSocketPair();
    pair[0].accept();
    pair[0].send("one");
    pair[0].send("two");
    pair[0].send("three");

    pair[1].accept();
    let received = collectMessages(pair[1]);
    // Nothing may be delivered within accept()'s synchronous frame.
    expect(received).toEqual([]);

    await pumpMicrotasks();
    expect(received).toEqual(["one", "two", "three"]);
  });

  it("loses nothing when listeners are attached first and accept() is called last", async () => {
    // The other real-world order (e.g. the platform relay's): listeners first, accept() last.
    let pair = new WebSocketPair();
    pair[0].accept();

    let received = collectMessages(pair[1]);
    pair[0].send("a");
    pair[0].send("b");
    pair[1].accept();

    await pumpMicrotasks();
    expect(received).toEqual(["a", "b"]);
  });

  it("propagates close code and reason to the peer, which stays CLOSING (half-close)", async () => {
    let pair = new WebSocketPair();
    pair[1].accept();
    let events: any[] = [];
    pair[1].addEventListener("close", event => {
      events.push({ code: event.code, reason: event.reason, wasClean: event.wasClean,
                    readyState: pair[1].readyState });
    });

    pair[0].accept();
    pair[0].close(4001, "bye");
    expect(pair[0].readyState).toBe(2);  // CLOSING -- not CLOSED until the peer closes back

    await pumpMicrotasks();
    // The peer observes CLOSING (not CLOSED) inside its close listener, like workerd's pair:
    // it hasn't closed its own side yet, and may still send.
    expect(events).toEqual([{ code: 4001, reason: "bye", wasClean: true, readyState: 2 }]);
    expect(pair[0].readyState).toBe(2);  // still CLOSING: the peer never closed back
    expect(pair[1].readyState).toBe(2);
  });

  it("completes the close handshake when the peer closes back", async () => {
    // The full native shape: A closes, B hears it and closes back, both reach CLOSED. B never
    // hears an event for its own close, and A hears the handshake's completion as the
    // responding close's real code/reason, already CLOSED when the event dispatches.
    let pair = new WebSocketPair();
    pair[0].accept();
    pair[1].accept();
    let closerEvents: any[] = [];
    pair[0].addEventListener("close", event => {
      closerEvents.push({ code: event.code, reason: event.reason, wasClean: event.wasClean,
                          readyState: pair[0].readyState });
    });
    let peerEvents: any[] = [];
    pair[1].addEventListener("close", () => peerEvents.push(pair[1].readyState));

    pair[0].close(1000, "done");
    await pumpMicrotasks();
    pair[1].close(3000, "ack");
    expect(pair[1].readyState).toBe(3);  // closing back completes B immediately
    await pumpMicrotasks();
    expect(closerEvents).toEqual([
      { code: 3000, reason: "ack", wasClean: true, readyState: 3 },
    ]);
    expect(peerEvents).toEqual([2]);  // exactly one event on B: A's close, heard at CLOSING
    expect(pair[0].readyState).toBe(3);
    expect(pair[1].readyState).toBe(3);
  });

  it("delivers the peer's real code and reason when the closes cross", async () => {
    // If both halves close before either hears the other, neither close is a response -- each
    // side receives the other's actual code/reason (wasClean), already CLOSED at dispatch.
    let pair = new WebSocketPair();
    pair[0].accept();
    pair[1].accept();
    let events: any[] = [];
    pair[0].addEventListener("close", event =>
        events.push(["a", event.code, event.reason, event.wasClean, pair[0].readyState]));
    pair[1].addEventListener("close", event =>
        events.push(["b", event.code, event.reason, event.wasClean, pair[1].readyState]));

    pair[0].close(1000, "from-a");
    pair[1].close(3000, "from-b");
    expect(pair[0].readyState).toBe(2);
    expect(pair[1].readyState).toBe(2);
    await pumpMicrotasks();
    // (b hears a's earlier close first -- the order the native pair delivered when probed.)
    expect(events).toEqual([
      ["b", 1000, "from-a", true, 3],
      ["a", 3000, "from-b", true, 3],
    ]);
  });

  it("keeps delivering to a half that has closed (half-close)", async () => {
    // Native pairs honor RFC 6455 half-close: after A closes, B may flush final frames --
    // the goodbye pattern -- and A receives them, right up until B closes back.
    let pair = new WebSocketPair();
    pair[0].accept();
    pair[1].accept();
    let received: any[] = [];
    pair[0].addEventListener("message", event => received.push(event.data));
    pair[0].addEventListener("close", event => received.push({ code: event.code }));
    pair[1].addEventListener("close", () => {
      pair[1].send("goodbye");
      pair[1].close(1000, "ack");
    });

    pair[0].close(1000, "done");
    await pumpMicrotasks();
    expect(received).toEqual(["goodbye", { code: 1000 }]);
    expect(pair[0].readyState).toBe(3);
    expect(pair[1].readyState).toBe(3);
  });

  it("still delivers a message racing with the receiver's own close()", async () => {
    let pair = new WebSocketPair();
    pair[0].accept();
    pair[1].accept();
    let received = collectMessages(pair[0]);
    pair[0].close(1000);
    pair[1].send("racer");
    await pumpMicrotasks();
    expect(received).toEqual(["racer"]);
  });

  it("delivers a codeless close as 1005 with an empty reason", async () => {
    let pair = new WebSocketPair();
    pair[1].accept();
    let events: any[] = [];
    pair[1].addEventListener("close",
        event => events.push({ code: event.code, reason: event.reason, wasClean: event.wasClean }));

    pair[0].accept();
    pair[0].close();
    await pumpMicrotasks();
    expect(events).toEqual([{ code: 1005, reason: "", wasClean: true }]);
  });

  it("validates close codes the way workerd does", () => {
    // Legal: anything in 1000-4999 except the reserved 1004/1005/1006/1015. (Notably 1001
    // "going away" and the 1000s-2000s protocol codes ARE sendable on a pair.)
    for (let code of [1000, 1001, 1002, 1012, 2999, 3000, 4999]) {
      let pair = new WebSocketPair();
      pair[0].accept();
      pair[0].close(code);
    }
    for (let code of [999, 1004, 1005, 1006, 1015, 5000]) {
      let pair = new WebSocketPair();
      pair[0].accept();
      expect(() => pair[0].close(code))
          .toThrow(new TypeError(`Invalid WebSocket close code: ${code}.`));
    }
  });

  it("requires a code when a reason is given, and puts no cap on reason length", () => {
    // Both ends of this were probed on native workerd: close(undefined, reason) throws, and a
    // pair does NOT enforce RFC 6455's 123-byte wire cap on reasons.
    {
      let pair = new WebSocketPair();
      pair[0].accept();
      expect(() => pair[0].close(undefined, "why"))
          .toThrow("If you specify a WebSocket close reason, you must also specify a code.");
    }
    {
      let pair = new WebSocketPair();
      pair[0].accept();
      pair[0].close(1000, "x".repeat(124));  // over the wire cap; legal on a pair
    }
  });

  it("treats a second close() as a no-op, even with invalid arguments", async () => {
    let pair = new WebSocketPair();
    pair[1].accept();
    let events: any[] = [];
    pair[1].addEventListener("close", event => events.push({ code: event.code, reason: event.reason }));

    pair[0].accept();
    pair[0].close(1000, "first");
    // The no-op check precedes validation, as on workerd: an invalid second close is silently
    // ignored rather than throwing.
    pair[0].close(88, "second");
    pair[0].close(4000, "third");
    await pumpMicrotasks();
    expect(events).toEqual([{ code: 1000, reason: "first" }]);
  });

  it("throws on send() after close()", () => {
    let pair = new WebSocketPair();
    pair[0].accept();
    pair[0].close(1000, "done");
    // workerd's message.
    expect(() => pair[0].send("too late"))
        .toThrow(new TypeError("Can't call WebSocket send() after close()."));
  });

  it("throws on close() before accept(), through the same gate as send()", () => {
    // workerd routes close() through the accept gate too. (This is why RpcPayload.disposeImpl
    // accept()s an unsent upgrade Response's socket before closing it -- see the tunnel tests
    // for the release-on-ignore behavior this enables.)
    let pair = new WebSocketPair();
    expect(() => pair[0].close(1000, "bye"))
        .toThrow("You must call one of accept() or state.acceptWebSocket() on this WebSocket " +
                 "before sending messages.");
  });

  it("delivers buffered messages before the close event", async () => {
    let pair = new WebSocketPair();
    pair[0].accept();
    pair[0].send("m1");
    pair[0].send("m2");
    pair[0].close(1000, "done");

    let events: any[] = [];
    pair[1].addEventListener("message", event => events.push(event.data));
    pair[1].addEventListener("close", event => events.push({ code: event.code }));
    pair[1].accept();
    await pumpMicrotasks();
    expect(events).toEqual(["m1", "m2", { code: 1000 }]);
  });

  it("does not recurse or deadlock under mutual echo listeners", async () => {
    let pair = new WebSocketPair();
    pair[0].accept();
    pair[1].accept();

    let count = 0;
    let done = new Promise<void>(resolve => {
      pair[0].addEventListener("message", event => {
        if (++count >= 100) {
          resolve();
        } else {
          pair[0].send(event.data);
        }
      });
    });
    pair[1].addEventListener("message", event => pair[1].send(event.data));

    pair[0].send("ping");
    // No listener runs synchronously inside send().
    expect(count).toBe(0);
    await done;
    expect(count).toBe(100);
  });

  it("copies binary payloads at send() time", async () => {
    // Delivery is asynchronous, so a reused scratch buffer must not corrupt an undelivered
    // frame. NOTE this is deliberately SAFER than native workerd, which does not copy --
    // don't rely on it in code that must also run on Workers.
    let pair = new WebSocketPair();
    pair[0].accept();
    pair[1].accept();
    let received = collectMessages(pair[1]);

    let scratch = new Uint8Array([1, 2, 3]);
    pair[0].send(scratch);
    scratch.fill(9);

    await pumpMicrotasks();
    expect(Array.from(new Uint8Array(received[0] as ArrayBuffer))).toEqual([1, 2, 3]);
  });

  it("delivers a view over a SharedArrayBuffer as a plain ArrayBuffer", async () => {
    // The copy must normalize the buffer type: slice() on a SharedArrayBuffer yields another
    // SharedArrayBuffer, which would violate the binary-as-ArrayBuffer contract (and kill a
    // tunnel wrapped around the half).
    let pair = new WebSocketPair();
    pair[0].accept();
    pair[1].accept();
    let received = collectMessages(pair[1]);

    let shared = new Uint8Array(new SharedArrayBuffer(3));
    shared.set([7, 8, 9]);
    pair[0].send(shared);

    await pumpMicrotasks();
    expect(received[0]).toBeInstanceOf(ArrayBuffer);
    expect(Array.from(new Uint8Array(received[0] as ArrayBuffer))).toEqual([7, 8, 9]);
  });

  it("throws workerd's error on send() before accept()", () => {
    let pair = new WebSocketPair();
    expect(() => pair[0].send("hello"))
        .toThrow("You must call one of accept() or state.acceptWebSocket() on this WebSocket " +
                 "before sending messages.");
  });

  it("keeps delivering after a listener throws", async () => {
    // A deliberate divergence from native workerd (which permanently wedges delivery after a
    // listener throw): the throw is reported and later listeners/events still run, so one bad
    // handler can't silently swallow a close.
    let errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      let pair = new WebSocketPair();
      pair[0].accept();
      pair[1].accept();
      let received: any[] = [];
      pair[1].addEventListener("message", () => { throw new Error("bad listener"); });
      pair[1].addEventListener("message", event => received.push(event.data));
      pair[1].addEventListener("close", event => received.push({ code: event.code }));

      pair[0].send("one");
      pair[0].send("two");
      pair[0].close(1000, "done");
      await pumpMicrotasks();
      expect(received).toEqual(["one", "two", { code: 1000 }]);
      expect(errorSpy).toHaveBeenCalledTimes(2);
    } finally {
      errorSpy.mockRestore();
    }
  });
});

describe("upgradeWebSocketResponse", () => {
  // These run under Node, so they exercise the non-workerd branch: a status-200 Response with
  // `webSocket` as a configurable own property. workerd.test.ts proves the native-101 branch.

  it("attaches the socket as a configurable own property of a bodyless 200", () => {
    let pair = new WebSocketPair();
    let response = upgradeWebSocketResponse(pair[0]);

    expect(response.status).toBe(200);
    expect(response.body).toBeNull();
    let descriptor = Object.getOwnPropertyDescriptor(response, "webSocket");
    expect(descriptor?.value).toBe(pair[0]);
    expect(descriptor?.configurable).toBe(true);
  });

  it("carries init.headers along", () => {
    let pair = new WebSocketPair();
    let response = upgradeWebSocketResponse(pair[0], {
      headers: { "Sec-WebSocket-Protocol": "chat" },
    });
    expect(response.headers.get("Sec-WebSocket-Protocol")).toBe("chat");
  });

  it("accepts init.status 101 and rejects any other status", () => {
    let pair = new WebSocketPair();
    // 101 is what the response means, so spelling it out is fine (though off-workerd the
    // constructed Response still reads 200 -- the status is never serialized for upgrades).
    expect(upgradeWebSocketResponse(pair[0], { status: 101 }).status).toBe(200);
    expect(() => upgradeWebSocketResponse(pair[1], { status: 418 })).toThrow(TypeError);
  });

  it("rejects non-sockets with a helpful error", () => {
    expect(() => upgradeWebSocketResponse({} as any))
        .toThrow(/send\(\), close\(\), and addEventListener\(\)/);
    // The classic mistake: passing the whole pair instead of one half.
    expect(() => upgradeWebSocketResponse(new WebSocketPair() as any))
        .toThrow(/whole WebSocketPair instead of one half/);
  });
});
