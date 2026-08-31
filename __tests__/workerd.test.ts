// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

/// <reference types="@cloudflare/workers-types" />
import { expect, it, describe } from "vitest";
import { RpcStub as NativeRpcStub, RpcTarget as NativeRpcTarget, env, DurableObject } from "cloudflare:workers";
// Cap'n Web's WebSocketPair export is imported under an alias so that the bare `WebSocketPair`
// references in the tests below keep pinning the raw native global spelling.
import { newHttpBatchRpcSession, newWebSocketRpcSession, RpcStub, RpcPromise, RpcTarget,
         WebSocketPair as CapnwebWebSocketPair } from "../src/index-workers.js";
import { v, wrapServerTarget, type ServiceValidator } from "../packages/capnweb-validate/src/internal/core.js";
import { Counter, DeviceEchoTarget, TestTarget } from "./test-util.js";

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

  getBroken(): NativeRpcStub<NativeCounter> {
    throw new RangeError("test error");
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

async function pumpMicrotasks() {
  for (let i = 0; i < 16; i++) {
    await Promise.resolve();
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

  it("can wrap a native promise in a userspace promise", async () => {
    // Wrapping in RpcPromise (rather than RpcStub) exercises the rpc-thenable adoption path,
    // which pipelines calls on the native thenable without eagerly awaiting it.
    let factory = new NativeRpcStub(new CounterFactory());
    let stub = new RpcPromise(factory.getNative());
    expect(await stub.increment()).toBe(1);
    expect(await stub.increment()).toBe(2);

    expect(await stub.value).toBe(2);
  })

  it("can dup a userspace promise wrapping a native promise", async () => {
    let factory = new NativeRpcStub(new CounterFactory());
    let promise = new RpcPromise(factory.getNative());

    // dup() routes through get([]), which must produce an independent hook aliasing the same
    // underlying native promise.
    let dup = promise.dup();
    expect(await dup.increment()).toBe(1);
    expect(await promise.increment()).toBe(2);
  })

  it("can pass a wrapped native promise as an RPC argument", async () => {
    class CounterUser extends RpcTarget {
      useCounter(counter: RpcStub<NativeCounter>) {
        return counter.increment(5);
      }
    }

    let factory = new NativeRpcStub(new CounterFactory());
    let user = new RpcStub(new CounterUser());
    let arg = new RpcPromise(factory.getNative());
    expect(await user.useCounter(<any>arg)).toBe(5);
  })

  it("reports brokenness when a wrapped native promise rejects", async () => {
    let factory = new NativeRpcStub(new CounterFactory());
    let promise = new RpcPromise(<any>factory.getBroken());

    let errors: any[] = [];
    promise.onRpcBroken(err => { errors.push(err); });

    await expect(Promise.resolve(promise)).rejects.toThrow("test error");
    await pumpMicrotasks();
    expect(errors.length).toBe(1);
    expect(errors[0].message).toBe("test error");
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
    let api: any = newWebSocketRpcSession(pair[0]);
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

  it("exports the native WebSocketPair on workerd", () => {
    // On workerd, Cap'n Web's WebSocketPair is the native class itself (aliased at module
    // load), so there is exactly one pair behavior per platform.
    expect(CapnwebWebSocketPair).toBe((globalThis as any).WebSocketPair);
  });

  it("answers an upgrade via upgradeWebSocketResponse() with a genuine 101 and native socket",
      async () => {
    // The provider is the shared DeviceEchoTarget from test-util.ts -- the exact same source
    // runs under Node in websocket-tunnel.test.ts, over the pure-JS pair. Here, on workerd,
    // upgradeWebSocketResponse() must produce the real thing: status 101 and a native
    // WebSocket, suitable for completing an actual HTTP upgrade.
    let pair = new WebSocketPair();
    pair[0].accept();
    pair[1].accept();
    let api: any = newWebSocketRpcSession(pair[0]);
    newWebSocketRpcSession(pair[1], new DeviceEchoTarget());

    let response: Response = await api.openDeviceEcho();
    expect(response.status).toBe(101);
    let socket = response.webSocket;
    expect(socket).toBeInstanceOf(WebSocket);

    socket!.accept();
    let message = new Promise(resolve => {
      socket!.addEventListener("message", event => resolve(event.data), { once: true });
    });
    socket!.send("hello");
    expect(await message).toBe("device-echo:hello");
    socket!.close(1000, "done");
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
