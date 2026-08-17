// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

// Test server implemented in workerd instead of Node.
//
// This is only used by the workerd tests, across a service binding.
//
// This file is JavaScript instead of TypeScript because otherwise we'd need to set up a separate
// build step for it. Instead, we're getting by configuring the worker in vitest.config.ts by
// just specifying the raw JS modules.

import { newWorkersRpcResponse, newWebSocketRpcSession, materializeUpgrade } from "../dist/index-workers.js";
import { RpcTarget, DurableObject } from "cloudflare:workers";

// TODO(cleanup): At present we clone the implementation of Counter and TestTarget because
//   otherwise we need to set up a build step for `test-util.ts`.
export class Counter extends RpcTarget {
  constructor(i) {
    super();
    this.i = i;
  }

  increment(amount = 1) {
    this.i += amount;
    return this.i;
  }
}

export class TestDo extends DurableObject {
  setValue(val) {
    this.value = val;
  }

  getValue() {
    return this.value;
  }

  subscribe(callback) {
    this.subscriber = callback.dup();
  }

  async notify(value) {
    await this.subscriber(value);
    this.subscriber[Symbol.dispose]();
  }

  callCounter(callback) {
    return callback.increment();
  }
}

export class TestTarget extends RpcTarget {
  constructor(env) {
    super();
    this.env = env;
  }

  square(i) {
    return i * i;
  }

  callSquare(self, i) {
    return { result: self.square(i) };
  }

  throwError() {
    throw new RangeError("test error");
  }

  makeCounter(i) {
    return new Counter(i);
  }

  incrementCounter(c, i = 1) {
    return c.increment(i);
  }

  getDurableObject(name) {
    return this.env.TEST_DO.getByName(name);
  }
}

// See openDeferredEchoRemote below.
let deferredEchoRetainer = null;

export default {
  async fetch(req, env, ctx) {
    // The design doc's "fetch-lane exit", end to end: defer a tunneled upgrade from an
    // in-isolate capnweb session, materialize it, and serve it as a REAL HTTP upgrade from
    // this fetch handler -- headers carried on pair.init and all.
    if (new URL(req.url).pathname === "/deferred-upgrade") {
      let origin = new WebSocketPair();
      origin[1].accept();
      origin[1].addEventListener("message", event => origin[1].send(event.data));

      class Target extends RpcTarget {
        openEcho() {
          return new Response(null, {
            status: 101,
            webSocket: origin[0],
            headers: { "sec-websocket-protocol": "itx-v1", "x-provider-custom": "survives" },
          });
        }
      }

      // (Holding a capnweb session in this request-scoped context triggers workerd
      // hang-detector warnings after the test finishes; production holds sessions in a
      // long-lived relay WebSocket context. Accepted as test-only noise.)
      let pair = new WebSocketPair();
      pair[0].accept();
      pair[1].accept();
      let api = newWebSocketRpcSession(pair[0]);  // deferred by default on Workers
      newWebSocketRpcSession(pair[1], new Target());
      let response = await api.openEcho();
      return materializeUpgrade(response.webSocket);
    }

    return newWorkersRpcResponse(req, new TestTarget(env), {
      onSendError(err) { return err; }
    });
  },

  async greet(name, env, ctx) {
    return `Hello, ${name}!`;
  },

  // The relay shape: THIS isolate holds a deferring capnweb session and returns the raw pair
  // in a native RPC return payload; the caller materializes after the call settles. The session
  // and the delivering Response must be retained past the call (module state) -- disposing them
  // would release the payload-owned tunnel -- which is exactly the retention contract a real
  // relay must follow.
  async openDeferredEchoRemote(unused, env, ctx) {
    let origin = new WebSocketPair();
    origin[1].accept();
    origin[1].addEventListener("message", event => origin[1].send(event.data));
    let closeEvent = null;
    origin[1].addEventListener("close",
        event => { closeEvent = { code: event.code, reason: event.reason }; });

    class Target extends RpcTarget {
      openEcho() {
        return new Response(null, {
          status: 101,
          webSocket: origin[0],
          headers: { "sec-websocket-protocol": "itx-v1" },
        });
      }
    }

    // (Session held in an RPC-call context: same accepted hang-detector noise as the
    // /deferred-upgrade route above.)
    let pair = new WebSocketPair();
    pair[0].accept();
    pair[1].accept();
    let api = newWebSocketRpcSession(pair[0]);  // deferred by default on Workers
    newWebSocketRpcSession(pair[1], new Target());
    let response = await api.openEcho();
    deferredEchoRetainer = { api, response, getClose: () => closeEvent };
    return response.webSocket;
  },

  async deferredEchoCloseEvent(unused, env, ctx) {
    return deferredEchoRetainer ? deferredEchoRetainer.getClose() : null;
  },

  // Native workers-RPC leg of deferred upgrade materialization: receives a tunneled socket as a
  // raw { readable, writable } pair (which workerd RPC serializes; a live WebSocket it refuses),
  // materializes the socket in THIS isolate, and runs one echo round trip through it.
  async materializeEcho(webSocket, env, ctx) {
    let response = materializeUpgrade(webSocket);
    let socket = response.webSocket;
    socket.accept();
    let reply = new Promise(resolve => {
      socket.addEventListener("message", event => resolve(event.data), { once: true });
    });
    socket.send("ping across isolates");
    try {
      return await reply;
    } finally {
      socket.close(1000, "");
    }
  }
}
