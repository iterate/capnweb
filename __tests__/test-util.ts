// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

// Common RPC interfaces / implementations used in several tests.

import { RpcStub, RpcTarget, upgradeWebSocketResponse, WebSocketPair } from '../src/index.js';

export class Counter extends RpcTarget {
  constructor(private i: number = 0) {
    super();
  }

  increment(amount: number = 1): number {
    this.i += amount;
    return this.i;
  }

  get value() {
    return this.i;
  }
}

// Distinct function so we can search for it in the stack trace.
function throwErrorImpl(): never {
  throw new RangeError("test error");
}

export class TestTarget extends RpcTarget {
  square(i: number) {
    return i * i;
  }

  callSquare(self: RpcStub<TestTarget>, i: number) {
    return { result: self.square(i) };
  }

  async callFunction(func: RpcStub<(i: number) => Promise<number>>, i: number) {
    return { result: await func(i) };
  }

  // Store a bare callback now and invoke it later, in a *separate* call -- the "subscription
  // delivery" shape (mirrors the workerd TestDo.subscribe/notify). `dup()` keeps the stub alive
  // past the call it arrived in. The later invocation calls the stub as a bare function (empty
  // path).
  #subscriber?: RpcStub<(v: number) => unknown>;
  subscribeCallback(callback: RpcStub<(v: number) => unknown>) {
    this.#subscriber = (callback as unknown as { dup(): RpcStub<(v: number) => unknown> }).dup();
  }
  async notifySubscriber(value: number) {
    let result = await this.#subscriber!(value);
    (this.#subscriber as unknown as Disposable)[Symbol.dispose]();
    this.#subscriber = undefined;
    return { result };
  }

  throwError() {
    throwErrorImpl();
  }

  makeCounter(i: number) {
    return new Counter(i);
  }

  incrementCounter(c: RpcStub<Counter>, i: number = 1) {
    return c.increment(i);
  }

  generateFibonacci(length: number) {
    let result = [0, 1];
    if (length <= result.length) return result.slice(0, length);

    while (result.length < length) {
      let next = result[result.length - 1] + result[result.length - 2];
      result.push(next);
    }

    return result;
  }

  returnNull() { return null; }
  returnUndefined() { return undefined; }
  returnNumber(i: number) { return i; }

  async echoBlob(blob: Blob): Promise<Blob> {
    let bytes = await blob.arrayBuffer();
    return new Blob([bytes], {type: blob.type});
  }
}

// A provider that IS the WebSocket endpoint: it answers an upgrade with one half of a
// WebSocketPair and speaks through the other. This exact class runs under both Node
// (websocket-tunnel.test.ts, over the pure-JS pair) and workerd (workerd.test.ts, over the
// native pair) -- a tripwire against semantic drift between the two WebSocketPair
// implementations.
export class DeviceEchoTarget extends RpcTarget {
  openDeviceEcho(): Response {
    let pair = new WebSocketPair();
    pair[1].accept();
    pair[1].addEventListener("message", event => pair[1].send(`device-echo:${event.data}`));
    return upgradeWebSocketResponse(pair[0]);
  }
}
