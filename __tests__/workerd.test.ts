// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

/// <reference types="@cloudflare/workers-types" />
import { expect, it, describe } from "vitest";
import { RpcStub as NativeRpcStub, RpcTarget as NativeRpcTarget, env, DurableObject } from "cloudflare:workers";
import { newHttpBatchRpcSession, newWebSocketRpcSession, materializeUpgrade, RpcStub, RpcTarget } from "../src/index-workers.js";
import { v, wrapServerTarget, type ServiceValidator } from "../packages/capnweb-validate/src/internal/core.js";
import { Counter, TestTarget } from "./test-util.js";

class JsCounter extends RpcTarget {
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

class NativeCounter extends RpcTarget {
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

class CounterFactory extends RpcTarget {
  getNative() {
    return new NativeRpcStub(new NativeCounter());
  }

  getNativeEmbedded() {
    return {stub: new NativeRpcStub(new NativeCounter())};
  }

  getJs() {
    return new RpcStub(new JsCounter());
  }

  getJsEmbedded() {
    return {stub: new RpcStub(new JsCounter())};
  }
}

describe("workerd compatibility", () => {
  it("allows native RpcStubs to be created using userspace RpcTargets", async () => {
    let stub = new NativeRpcStub(new JsCounter());
    expect(await stub.increment()).toBe(1);
    expect(await stub.increment()).toBe(2);

    expect(await stub.value).toBe(2);
  })

  it("allows userspace RpcStubs to be created using native RpcTargets", async () => {
    let stub = new RpcStub(new NativeCounter());
    expect(await stub.increment()).toBe(1);
    expect(await stub.increment()).toBe(2);

    expect(await stub.value).toBe(2);
  })

  it("can wrap a native stub in a userspace stub", async () => {
    let stub = new RpcStub(new NativeRpcStub(new NativeCounter()));
    expect(await stub.increment()).toBe(1);
    expect(await stub.increment()).toBe(2);

    expect(await stub.value).toBe(2);
  })

  it("can return a native stub from a userspace call", async () => {
    // Returning a bare stub.
    {
      let factory = new RpcStub(new CounterFactory());
      let stub = await factory.getNative();
      expect(await stub.increment()).toBe(1);
      expect(await stub.increment()).toBe(2);

      expect(await stub.value).toBe(2);
    }

    // Again with a stub wrapped in an object.
    {
      let factory = new RpcStub(new CounterFactory());
      let obj = await factory.getNativeEmbedded();
      expect(await obj.stub.increment()).toBe(1);
      expect(await obj.stub.increment()).toBe(2);

      expect(await obj.stub.value).toBe(2);
    }
  })

  it("can wrap a native promise or property in a userspace stub", async () => {
    // Wrap a native RpcPromise in a userspace stub.
    {
      let factory = new NativeRpcStub(new CounterFactory());
      let stub = new RpcStub(factory.getNative());
      expect(await stub.increment()).toBe(1);
      expect(await stub.increment()).toBe(2);

      expect(await stub.value).toBe(2);
    }

    // Wrap a native RpcProperty in a userspace stub.
    {
      let factory = new NativeRpcStub(new CounterFactory());
      let stub = new RpcStub(factory.getNativeEmbedded().stub);
      expect(await stub.increment()).toBe(1);
      expect(await stub.increment()).toBe(2);

      expect(await stub.value).toBe(2);
    }
  })

  it("can pipeline on a native stub returned from a userspace call", async () => {
    {
      let factory = new RpcStub(new CounterFactory());
      let obj = factory.getNative();
      expect(await obj.increment()).toBe(1);
      expect(await obj.increment()).toBe(2);

      expect(await obj.value).toBe(2);
    }

    {
      let factory = new RpcStub(new CounterFactory());
      let obj = factory.getNativeEmbedded();
      expect(await obj.stub.increment()).toBe(1);
      expect(await obj.stub.increment()).toBe(2);

      expect(await obj.stub.value).toBe(2);
    }
  })

  it("can wrap a userspace stub in a native stub", async () => {
    let stub = new NativeRpcStub(new RpcStub(new JsCounter()));
    expect(await stub.increment()).toBe(1);
    expect(await stub.increment()).toBe(2);

    expect(await stub.value).toBe(2);
  })

  it("can return a userspace stub from a native call", async () => {
    // Returning a bare stub.
    {
      let factory = new NativeRpcStub(new CounterFactory());
      let stub = await factory.getJs();
      expect(await stub.increment()).toBe(1);
      expect(await stub.increment()).toBe(2);

      expect(await stub.value).toBe(2);
    }

    // Again with a stub wrapped in an object.
    {
      let factory = new NativeRpcStub(new CounterFactory());
      let obj = await factory.getJsEmbedded();
      expect(await obj.stub.increment()).toBe(1);
      expect(await obj.stub.increment()).toBe(2);

      expect(await obj.stub.value).toBe(2);
    }
  })

  it("can wrap a userspace promise or property in a native stub", async () => {
    // Wrap a userspace RpcPromise in a native stub.
    {
      let factory = new RpcStub(new CounterFactory());
      let stub = new NativeRpcStub(factory.getJs());
      expect(await stub.increment()).toBe(1);
      expect(await stub.increment()).toBe(2);

      expect(await stub.value).toBe(2);
    }

    // Wrap a userspace property (which is actually also an RpcPromise) in a native stub.
    {
      let factory = new RpcStub(new CounterFactory());
      let stub = new NativeRpcStub(factory.getJsEmbedded().stub);
      expect(await stub.increment()).toBe(1);
      expect(await stub.increment()).toBe(2);

      expect(await stub.value).toBe(2);
    }
  })

  it("can pipeline on a userspace stub returned from a native call", async () => {
    {
      let factory = new NativeRpcStub(new CounterFactory());
      let obj = factory.getJs();
      expect(await obj.increment()).toBe(1);
      expect(await obj.increment()).toBe(2);

      expect(await obj.value).toBe(2);
    }

    {
      let factory = new NativeRpcStub(new CounterFactory());
      let obj = factory.getJsEmbedded();
      expect(await obj.stub.increment()).toBe(1);
      expect(await obj.stub.increment()).toBe(2);

      expect(await obj.stub.value).toBe(2);
    }
  })

  it("can wrap a ServiceStub in an RpcStub", async () => {
    let result = await new RpcStub((<any>env).testServer).greet("World");
    expect(result).toBe("Hello, World!");
  });

});

interface Env {
  testServer: Fetcher
}

interface TestDo extends DurableObject {
  setValue(val: any): void;
  getValue(): any;
  callCounter(callback: { increment(): Promise<number> }): Promise<number>;

  subscribe(callback: (s: string) => void): void;
  notify(value: string): void;
}

interface WorkerdTestTarget extends TestTarget {
  getDurableObject(name: string): DurableObjectStub<TestDo>;
}

describe("workerd RPC server", () => {
  it("can return a WebSocket upgrade Response over RPC", async () => {
    class WebSocketResponseTarget extends RpcTarget {
      openEcho() {
        let pair = new WebSocketPair();
        pair[1].accept();
        pair[1].addEventListener("message", event => pair[1].send(event.data));
        return new Response(null, { status: 101, webSocket: pair[0] });
      }
    }

    let pair = new WebSocketPair();
    pair[0].accept();
    pair[1].accept();
    // On Workers, upgrades arrive deferred by default; this session opts out because it serves
    // the socket right here at the session endpoint.
    let api: any = newWebSocketRpcSession(pair[0], undefined,
        { deferUpgradeMaterialization: false });
    newWebSocketRpcSession(pair[1], new WebSocketResponseTarget());

    // On workerd, the received Response holds a native WebSocket, suitable for completing a real
    // HTTP upgrade (e.g. by returning the Response from a fetch handler).
    let response: Response = await api.openEcho();
    expect(response.status).toBe(101);
    let socket = response.webSocket;
    expect(socket).toBeTruthy();

    socket!.accept();
    let message = new Promise(resolve => {
      socket!.addEventListener("message", event => resolve(event.data), { once: true });
    });
    socket!.send("hello over Response.webSocket");

    expect(await message).toBe("hello over Response.webSocket");
    socket!.close();
  });

  // Opens an in-isolate deferring session against an echoing WebSocketPair target, returning
  // the deferred Response plus a promise for the close event observed at the origin end -- so
  // tests can assert closure makes it all the way back.
  function openDeferredEcho(): {
    response: Promise<Response>,
    originClose: Promise<{ code: number, reason: string }>,
  } {
    let resolveClose: (event: { code: number, reason: string }) => void;
    let originClose = new Promise<{ code: number, reason: string }>(resolve => {
      resolveClose = resolve;
    });

    class EchoUpgradeTarget extends RpcTarget {
      openEcho() {
        let pair = new WebSocketPair();
        pair[1].accept();
        pair[1].addEventListener("message", event => pair[1].send(event.data));
        pair[1].addEventListener("close",
            event => resolveClose({ code: event.code, reason: event.reason }));
        return new Response(null, { status: 101, webSocket: pair[0] });
      }
    }

    let pair = new WebSocketPair();
    pair[0].accept();
    pair[1].accept();
    let api: any = newWebSocketRpcSession(pair[0]);  // deferred by default on Workers
    newWebSocketRpcSession(pair[1], new EchoUpgradeTarget());
    return { response: api.openEcho(), originClose };
  }

  it("defers upgrade materialization by default on Workers", async () => {
    // On Workers a tunneled upgrade arrives as the transportable pair rather than a
    // materialized socket (which could never cross another RPC hop anyway).
    let response = await openDeferredEcho().response;
    let webSocket: any = (response as any).webSocket;
    expect(webSocket instanceof WebSocket).toBe(false);
    expect(webSocket.readable).toBeInstanceOf(ReadableStream);
    expect(webSocket.writable).toBeInstanceOf(WritableStream);

    // materializeUpgrade() is the final hop: a real 101 carrying a native socket, suitable for
    // completing an actual HTTP upgrade.
    let materialized = materializeUpgrade(webSocket);
    expect(materialized.status).toBe(101);
    let socket = materialized.webSocket!;
    socket.accept();
    let message = new Promise(resolve => {
      socket.addEventListener("message", event => resolve(event.data), { once: true });
    });
    socket.send("hello through a deferred tunnel");
    expect(await message).toBe("hello through a deferred tunnel");
    socket.close();
  });

  it("carries a deferred stream pair across a native workers RPC boundary", async () => {
    // The reason deferUpgradeMaterialization exists: workerd's RPC serializer refuses a live
    // WebSocket but carries ReadableStream/WritableStream natively. Receive a tunnel deferred,
    // hand the raw pair across a real service-binding RPC hop, and let the OTHER isolate
    // materialize the socket and run an echo round trip through the full path:
    //   far isolate <-native RPC streams-> this isolate <-capnweb tunnel-> echoing pair end.
    let { response, originClose } = openDeferredEcho();
    let webSocket: any = ((await response) as any).webSocket;
    let reply = await (<any>(<Env>env).testServer).materializeEcho(webSocket);
    expect(reply).toBe("ping across isolates");

    // The far isolate closed its materialized socket with 1000; the close must traverse the
    // native hop and the tunnel back to the origin pair end.
    expect(await originClose).toMatchObject({ code: 1000 });
  });

  it("carries a deferred pair in a native RPC return payload (the relay-to-DO shape)", async () => {
    // The production topology is the inverse of the test above: the capnweb session lives in
    // the FAR isolate (the relay), the pair crosses in the RETURN payload of a native RPC
    // call, and this isolate materializes and uses the socket after that call has settled.
    let webSocket: any = await (<any>(<Env>env).testServer).openDeferredEchoRemote(null);
    expect(webSocket.readable).toBeInstanceOf(ReadableStream);
    expect(webSocket.writable).toBeInstanceOf(WritableStream);
    // init rides the pair object across the native hop as plain data.
    expect(webSocket.init).toBeTruthy();

    let materialized = materializeUpgrade(webSocket);
    expect(materialized.status).toBe(101);
    expect(materialized.headers.get("sec-websocket-protocol")).toBe("itx-v1");
    let socket = materialized.webSocket!;
    socket.accept();
    let message = new Promise(resolve => {
      socket.addEventListener("message", event => resolve(event.data), { once: true });
    });
    socket.send("ping on the return path");
    expect(await message).toBe("ping on the return path");

    // Close from the materialized end; the origin -- two hops away, behind the native
    // boundary AND the capnweb tunnel, in the other isolate -- must observe it.
    socket.close(1000, "done");
    let closeEvent: any = null;
    for (let i = 0; i < 100 && !closeEvent; i++) {
      await new Promise(resolve => setTimeout(resolve, 20));
      closeEvent = await (<any>(<Env>env).testServer).deferredEchoCloseEvent(null);
    }
    expect(closeEvent).toMatchObject({ code: 1000, reason: "done" });
  });

  it("serves a real HTTP upgrade from a fetch handler via materializeUpgrade", async () => {
    // The design doc's fetch-lane exit, literally: the aux worker defers a tunneled upgrade
    // from an in-isolate capnweb session, materializes it, and returns it from its fetch
    // handler. We complete a real WebSocket handshake against it over the service binding and
    // check the provider's negotiated subprotocol (carried on pair.init) reached the real 101.
    // (Over a service binding the 101 is fabricated in-process: headers pass through verbatim
    // and no Sec-WebSocket-Accept is computed. Real-wire handshake sanitization -- reserved
    // headers recomputed/dropped, Accept derived from the client key -- is owned by workerd/kj
    // and was verified externally with a raw-socket probe during review.)
    let resp = await (<Env>env).testServer.fetch("http://foo/deferred-upgrade", {
      headers: { Upgrade: "websocket", "Sec-WebSocket-Protocol": "itx-v1" },
    });
    expect(resp.status).toBe(101);
    expect(resp.headers.get("sec-websocket-protocol")).toBe("itx-v1");
    expect(resp.headers.get("x-provider-custom")).toBe("survives");
    let ws = resp.webSocket!;
    ws.accept();
    let message = new Promise(resolve => {
      ws.addEventListener("message", event => resolve(event.data), { once: true });
    });
    ws.send("through the fetch lane");
    expect(await message).toBe("through the fetch lane");
    ws.close(1000, "");
  });

  it("can accept WebSocket RPC connections", async () => {
    let resp = await (<Env>env).testServer.fetch("http://foo", {headers: {Upgrade: "websocket"}});
    let ws = resp.webSocket;
    expect(ws).toBeTruthy();

    ws!.accept();
    let cap = newWebSocketRpcSession<WorkerdTestTarget>(ws!);

    expect(await cap.square(5)).toBe(25);

    {
      let counter = cap.makeCounter(2);
      expect(await counter.increment(3)).toBe(5);
    }

    {
      let counter = new Counter(4);
      expect(await cap.incrementCounter(counter, 9)).toBe(13);
    }

    // Test that we can pass a Durable Object stub over RPC.
    {
      let foo = cap.getDurableObject("foo");
      foo.setValue(123);

      let bar = await cap.getDurableObject("bar");
      bar.setValue("abc");

      expect(await foo.getValue()).toBe(123);
      expect(await bar.getValue()).toBe("abc");
    }

    {
      let baz = cap.getDurableObject("baz");

      let receivedValue: any;

      await baz.subscribe((value: any) => {receivedValue = value});

      await baz.notify("hello");

      expect(receivedValue).toBe("hello");
    }

  })

  it("forwards capnweb-validate callback stubs to Durable Objects", async () => {
    let resp = await (<Env>env).testServer.fetch("http://foo", {headers: {Upgrade: "websocket"}});
    let ws = resp.webSocket;
    expect(ws).toBeTruthy();

    ws!.accept();
    let cap = newWebSocketRpcSession<WorkerdTestTarget>(ws!);
    let callbackValidator: ServiceValidator = {
      serviceName: "Counter",
      methods: { increment: { args: [], returns: v.number } },
    };
    let account = cap.getDurableObject("validated-forward");
    let vendor = wrapServerTarget(
      {
        connect(callback: { increment(): Promise<number> }): Promise<number> {
          return account.callCounter(callback);
        },
        connectNested(value: {
          callback: { increment(): Promise<number> };
        }): Promise<number> {
          return account.callCounter(value.callback);
        },
      },
      {
        serviceName: "Vendor",
        methods: {
          connect: {
            args: [v.stubOf(callbackValidator)],
            returns: v.number,
          },
          connectNested: {
            args: [v.object({ callback: v.stubOf(callbackValidator) })],
            returns: v.number,
          },
        },
      }
    ) as {
      connect(callback: { increment(): Promise<number> }): Promise<number>;
      connectNested(value: {
        callback: { increment(): Promise<number> };
      }): Promise<number>;
    };

    let callback = new NativeRpcStub(new NativeCounter());
    expect(await vendor.connect(callback)).toBe(1);
    expect(await vendor.connectNested({ callback })).toBe(2);
  })

  it("can accept HTTP batch RPC connections", async () => {
    let cap = newHttpBatchRpcSession<TestTarget>(
        new Request("http://foo", {fetcher: (<Env>env).testServer}));

    let promise1 = cap.square(6);

    let counter = cap.makeCounter(2);
    let promise2 = counter.increment(3);
    let promise3 = cap.incrementCounter(counter, 4);

    expect(await Promise.all([promise1, promise2, promise3]))
        .toStrictEqual([36, 5, 9]);
  })
});
