// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { expect, it } from "vitest";
import { RpcStub } from "../src/index.js";
import { Counter, TestTarget } from "./test-util.js";

// A connected RPC session under test. Disposing it must tear down the session and any transport
// resources behind it.
export interface SessionFixture {
  stub: RpcStub<TestTarget>;
  [Symbol.asyncDispose](): Promise<void>;
}

// Registers a battery of test cases that exercise core Cap'n Web features end-to-end over a
// session connected to a `TestTarget`. The cases only depend on `connect()`, so the same battery
// can prove different transports equivalent -- e.g. a direct WebSocket connection vs. a WebSocket
// tunneled through another Cap'n Web session. Each case connects a fresh session.
export function registerSessionTestBattery(connect: () => Promise<SessionFixture>) {
  it("supports basic calls", async () => {
    await using session = await connect();
    expect(await session.stub.square(5)).toBe(25);
    expect(await session.stub.generateFibonacci(8)).toEqual([0, 1, 1, 2, 3, 5, 8, 13]);
  });

  it("propagates errors", async () => {
    await using session = await connect();
    await expect(Promise.resolve(session.stub.throwError()))
        .rejects.toThrow(new RangeError("test error"));
  });

  it("supports capabilities returned by the server", async () => {
    await using session = await connect();
    using counter = session.stub.makeCounter(2);
    expect(await counter.increment(3)).toBe(5);
    expect(await counter.increment(1)).toBe(6);
    expect(await counter.value).toBe(6);
  });

  it("supports capabilities passed to the server, called back from it", async () => {
    await using session = await connect();

    // The server calls right back to objects living on the client.
    expect(await session.stub.incrementCounter(new Counter(4), 9)).toBe(13);
    expect(await session.stub.callFunction(async (i: number) => i * 2, 5))
        .toStrictEqual({ result: 10 });
  });

  it("supports promise pipelining", async () => {
    await using session = await connect();

    // Call a method on the result of another call without awaiting in between.
    using counter = session.stub.makeCounter(10);
    using incremented = counter.increment(5);
    expect(await incremented).toBe(15);
  });

  it("supports Blobs (streamed through pipes)", async () => {
    await using session = await connect();
    let blob = new Blob(["hello blob"], { type: "text/plain" });
    let echoed = await session.stub.echoBlob(blob);
    expect(echoed.type).toBe("text/plain");
    expect(await echoed.text()).toBe("hello blob");
  });
}
