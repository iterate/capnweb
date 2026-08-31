// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

// Unit tests for the pure-JavaScript WebSocketPair and for upgradeWebSocketResponse() -- no RPC
// involved. These pin the workerd-faithful semantics the pair promises: halves born OPEN,
// unbounded buffering until accept(), strictly asynchronous FIFO delivery (which makes both the
// serializer's accept-then-listen order and the listen-then-accept order correct), and workerd's
// close and validation behavior. This file runs under Node, where no native WebSocketPair
// exists, so it exercises the pure-JS implementation; workerd.test.ts covers the native alias.

import { expect, it, describe } from "vitest";
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

  it("propagates close code and reason to the peer", async () => {
    let pair = new WebSocketPair();
    pair[1].accept();
    let events: any[] = [];
    pair[1].addEventListener("close", event => {
      events.push({ code: event.code, reason: event.reason, readyState: pair[1].readyState });
    });

    pair[0].accept();
    pair[0].close(4001, "bye");
    expect(pair[0].readyState).toBeGreaterThanOrEqual(2);  // CLOSING or CLOSED

    await pumpMicrotasks();
    // The peer's half is CLOSED by the time its close event fires.
    expect(events).toEqual([{ code: 4001, reason: "bye", readyState: 3 }]);
    expect(pair[0].readyState).toBe(3);  // CLOSED
  });

  it("delivers a codeless close as 1005 with an empty reason", async () => {
    let pair = new WebSocketPair();
    pair[1].accept();
    let events: any[] = [];
    pair[1].addEventListener("close", event => events.push({ code: event.code, reason: event.reason }));

    pair[0].close();
    await pumpMicrotasks();
    expect(events).toEqual([{ code: 1005, reason: "" }]);
  });

  it("rejects close codes a WebSocket can't send", () => {
    let pair = new WebSocketPair();
    for (let code of [1006, 2999, 1004]) {
      expect(() => pair[0].close(code)).toThrow(TypeError);
    }
    // ...while the legal ones are fine.
    new WebSocketPair()[0].close(1000);
    new WebSocketPair()[0].close(3000);
    new WebSocketPair()[0].close(4999);
  });

  it("rejects close reasons over 123 UTF-8 bytes", () => {
    // The cap is on encoded bytes, not characters: 62 e-acutes encode to 124 bytes.
    expect(() => new WebSocketPair()[0].close(1000, "x".repeat(124))).toThrow(TypeError);
    expect(() => new WebSocketPair()[0].close(1000, "\u00e9".repeat(62))).toThrow(TypeError);
    new WebSocketPair()[0].close(1000, "x".repeat(123));
  });

  it("treats a second close() as a no-op", async () => {
    let pair = new WebSocketPair();
    pair[1].accept();
    let events: any[] = [];
    pair[1].addEventListener("close", event => events.push({ code: event.code, reason: event.reason }));

    pair[0].close(1000, "first");
    pair[0].close(4000, "second");
    await pumpMicrotasks();
    expect(events).toEqual([{ code: 1000, reason: "first" }]);
  });

  it("throws on send() after close()", () => {
    let pair = new WebSocketPair();
    pair[0].accept();
    pair[0].close(1000, "done");
    expect(() => pair[0].send("too late"))
        .toThrow("Can't call send() on a WebSocket that is closing or closed.");
  });

  it("allows close() before accept(), and the peer hears it once accepted", async () => {
    // This is how an unsent upgrade Response releases its pair half: the payload's disposer
    // close()s a socket that was never accepted.
    let pair = new WebSocketPair();
    pair[0].close();

    let events: any[] = [];
    pair[1].addEventListener("close", event => events.push({ code: event.code, reason: event.reason }));
    pair[1].accept();
    await pumpMicrotasks();
    expect(events).toEqual([{ code: 1005, reason: "" }]);
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
    // frame.
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

  it("throws workerd's exact error on send() before accept()", () => {
    let pair = new WebSocketPair();
    expect(() => pair[0].send("hello"))
        .toThrow("You must call accept() on this WebSocket before sending messages.");
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
