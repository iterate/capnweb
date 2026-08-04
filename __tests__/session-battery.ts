// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { expect, it } from "vitest";
import { RpcStub } from "../src/index.js";
import { Counter, TestTarget } from "./test-util.js";

export type SessionTestId =
  | "basic-calls"
  | "byte-arrays"
  | "nested-paths-and-structured-arguments"
  | "errors"
  | "returned-capabilities"
  | "callback-capabilities"
  | "promise-pipelining"
  | "deferred-promise-pipelining"
  | "value-pipelining"
  | "blobs";

export interface SessionKnownFailure {
  /** Why this feature is deliberately outside the peer's compatibility profile. */
  reason: string;
  /** Stable peer error code which proves the intended unsupported-feature path ran. */
  expectedError: string;
}

export interface SessionBatteryOptions {
  knownFailures?: Partial<Record<SessionTestId, SessionKnownFailure>>;
}

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
export function registerSessionTestBattery(
    connect: () => Promise<SessionFixture>,
    options: SessionBatteryOptions = {}) {
  function register(
      id: SessionTestId,
      name: string,
      run: () => Promise<void>) {
    let knownFailure = options.knownFailures?.[id];
    if (!knownFailure) {
      it(name, run);
      return;
    }

    it(`${name} [expected failure: ${knownFailure.reason}]`, async () => {
      let failure: unknown;
      try {
        await run();
      } catch (error) {
        failure = error;
      }

      expect(failure, `Expected ${id} to fail with ${knownFailure.expectedError}`)
          .toBeDefined();
      expect(String(failure)).toContain(knownFailure.expectedError);
    });
  }

  register("basic-calls", "supports basic calls", async () => {
    await using session = await connect();
    expect(await session.stub.square(5)).toBe(25);
    expect(await session.stub.generateFibonacci(8)).toEqual([0, 1, 1, 2, 3, 5, 8, 13]);
  });

  register("byte-arrays", "supports bounded byte arrays", async () => {
    await using session = await connect();
    expect(Array.from(await session.stub.getBytes())).toEqual(
        [0, 1, 2, 127, 128, 254, 255]);
    let largeBytes = await session.stub.getLargeBytes(70_001);
    expect(largeBytes.byteLength).toBe(70_001);
    for (let index of [0, 1, 250, 251, 65_535, 70_000]) {
      expect(largeBytes[index]).toBe(index % 251);
    }
  });

  register("nested-paths-and-structured-arguments",
      "supports nested paths and structured arguments", async () => {
    await using session = await connect();
    expect(await session.stub.servos.move({ yaw: 12, pitch: -3 }))
        .toStrictEqual({ yaw: 12, pitch: -3 });
    expect(await session.stub.renderOnScreen({
      url: "https://example.com/snowman-\u2603?q=\"quoted\"",
    })).toBe(true);
  });

  register("errors", "propagates errors", async () => {
    await using session = await connect();
    await expect(Promise.resolve(session.stub.throwError()))
        .rejects.toThrow(new RangeError("test error"));
  });

  register("returned-capabilities", "supports capabilities returned by the server", async () => {
    await using session = await connect();
    using counter = session.stub.makeCounter(2);
    expect(await counter.increment(3)).toBe(5);
    expect(await counter.increment(1)).toBe(6);
    expect(await counter.value).toBe(6);
  });

  register("callback-capabilities",
      "supports capabilities passed to the server, called back from it", async () => {
    await using session = await connect();

    // The server calls right back to objects living on the client.
    expect(await session.stub.incrementCounter(new Counter(4), 9)).toBe(13);
    expect(await session.stub.callFunction(async (i: number) => i * 2, 5))
        .toStrictEqual({ result: 10 });
  });

  register("promise-pipelining", "supports promise pipelining", async () => {
    await using session = await connect();

    // Call a method on the result of another call without awaiting in between.
    using counter = session.stub.makeCounter(10);
    using incremented = counter.increment(5);
    expect(await incremented).toBe(15);
  });

  register("deferred-promise-pipelining",
      "supports pipelining onto a still-pending capability", async () => {
    await using session = await connect();
    using counter = session.stub.makeDeferredCounter(10);
    expect(await counter.increment(5)).toBe(15);
  });

  register("value-pipelining",
      "supports property pipelining onto a returned value", async () => {
    await using session = await connect();
    expect(await session.stub.makeValue(7).value).toBe(7);
  });

  register("blobs", "supports Blobs (streamed through pipes)", async () => {
    await using session = await connect();
    let blob = new Blob(["hello blob"], { type: "text/plain" });
    let echoed = await session.stub.echoBlob(blob);
    expect(echoed.type).toBe("text/plain");
    expect(await echoed.text()).toBe("hello blob");
  });
}
