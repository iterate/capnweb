// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { expect, it, describe, beforeAll, afterAll } from "vitest";
import type { AddressInfo } from "node:net";
import { WebSocket as NodeWebSocket, WebSocketServer } from "ws";
import { newWebSocketRpcSession, materializeUpgrade, RpcSession, RpcTarget,
         type RpcTransport } from "../src/index.js";
import { TestTarget } from "./test-util.js";
import { registerSessionTestBattery } from "./session-battery.js";

// Outside of Cloudflare Workers, the Response constructor can't produce an upgrade response, so
// we attach the (non-standard) `webSocket` property the same way the Workers runtime does.
function responseWithWebSocket(socket: unknown): Response {
  let response = new Response(null);
  Object.defineProperty(response, "webSocket", { value: socket });
  return response;
}

function listening(server: WebSocketServer): Promise<number> {
  return new Promise((resolve, reject) => {
    if (server.address()) {
      // Already listening.
      resolve((server.address() as AddressInfo).port);
    } else {
      server.on("error", reject);
      server.on("listening", () => resolve((server.address() as AddressInfo).port));
    }
  });
}

function closeServer(server: WebSocketServer): Promise<void> {
  return new Promise(resolve => {
    for (let client of server.clients) client.terminate();
    server.close(() => resolve());
  });
}

async function openWebSocket(port: number): Promise<NodeWebSocket> {
  let socket = new NodeWebSocket(`ws://127.0.0.1:${port}`);
  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
  return socket;
}

// Wait for one event of the given type. Works on both `ws` sockets and TunneledWebSockets.
function nextEvent(socket: any, type: string): Promise<any> {
  return new Promise(resolve => socket.addEventListener(type, resolve, { once: true }));
}

describe("WebSocket upgrade responses over RPC", () => {
  // An echo server, playing the role of some WebSocket endpoint that the RPC server connects to
  // on the client's behalf.
  let echoServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  echoServer.on("connection", socket => {
    socket.on("message", (data, isBinary) => socket.send(data, { binary: isBinary }));
  });

  // A server that immediately closes each connection, for testing close propagation.
  let slammingServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  slammingServer.on("connection", socket => socket.close(4321, "go away"));

  class TestApi extends RpcTarget {
    constructor(private echoPort: number, private slammingPort: number) { super(); }

    // Returns a bare WebSocket, which is not serializable.
    async openBareSocket(): Promise<unknown> {
      return await openWebSocket(this.echoPort);
    }

    async openEcho(): Promise<Response> {
      return responseWithWebSocket(await openWebSocket(this.echoPort));
    }

    async openSlamming(): Promise<Response> {
      return responseWithWebSocket(await openWebSocket(this.slammingPort));
    }

    // Takes an upgrade Response as a *parameter*, sends a message on its socket, and returns
    // the reply.
    async relay(response: Response): Promise<unknown> {
      let socket: any = (response as any).webSocket;
      let reply = new Promise(resolve => {
        socket.addEventListener("message", (event: any) => resolve(event.data), { once: true });
      });
      socket.send("ping");
      try {
        return await reply;
      } finally {
        socket.close(1000, "");
      }
    }

    // Takes an upgrade Response as a parameter and returns without touching its socket.
    async ignore(response: Response): Promise<void> {}

    // Takes an upgrade Response as a parameter and keeps the socket beyond the call by
    // accept()ing it before returning.
    adoptedSocket: any;
    async adopt(response: Response): Promise<void> {
      this.adoptedSocket = (response as any).webSocket;
      this.adoptedSocket.accept();
    }

    async sendOnAdopted(message: string): Promise<unknown> {
      let reply = new Promise(resolve => {
        this.adoptedSocket.addEventListener(
            "message", (event: any) => resolve(event.data), { once: true });
      });
      this.adoptedSocket.send(message);
      try {
        return await reply;
      } finally {
        this.adoptedSocket.close(1000, "");
      }
    }
  }

  let rpcServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  let clientSocket: NodeWebSocket;
  let api: any;

  beforeAll(async () => {
    let [echoPort, slammingPort, rpcPort] = await Promise.all([
      listening(echoServer), listening(slammingServer), listening(rpcServer)]);
    rpcServer.on("connection", socket => {
      newWebSocketRpcSession(socket as any, new TestApi(echoPort, slammingPort));
    });
    clientSocket = new NodeWebSocket(`ws://127.0.0.1:${rpcPort}`);
    api = newWebSocketRpcSession(clientSocket as any);
  });

  afterAll(async () => {
    clientSocket.close();
    await Promise.all([
      closeServer(rpcServer), closeServer(echoServer), closeServer(slammingServer)]);
  });

  it("refuses to serialize a bare WebSocket", async () => {
    // (Wrapped in Promise.resolve() because an RpcPromise is callable, which confuses
    // expect().rejects into invoking it as a function.)
    await expect(Promise.resolve(api.openBareSocket())).rejects.toThrow(/Cannot serialize/);
  });

  it("tunnels messages in both directions", async () => {
    let response = await api.openEcho();
    expect(response.webSocket).toBeTruthy();
    let socket = response.webSocket;

    let echoed = nextEvent(socket, "message");
    socket.send("hello");
    expect((await echoed).data).toBe("hello");

    let binaryEchoed = nextEvent(socket, "message");
    socket.send(new Uint8Array([1, 2, 3]));
    let bytes = (await binaryEchoed).data;
    expect(typeof bytes).not.toBe("string");
    expect(Array.from(bytes)).toEqual([1, 2, 3]);

    let closeEvent = nextEvent(socket, "close");
    socket.close(1000, "done");
    expect(await closeEvent).toMatchObject({ code: 1000, reason: "done" });
    expect(socket.readyState).toBe(3);  // CLOSED
  });

  it("propagates a close initiated by the remote socket's peer", async () => {
    let socket = (await api.openSlamming()).webSocket;
    expect(await nextEvent(socket, "close")).toMatchObject({ code: 4321, reason: "go away" });
    expect(socket.readyState).toBe(3);  // CLOSED
  });

  it("accepts an upgrade Response passed as a parameter", async () => {
    let socket = await openWebSocket((echoServer.address() as AddressInfo).port);
    expect(await api.relay(responseWithWebSocket(socket))).toBe("ping");
  });

  it("refuses to send the same socket twice", async () => {
    // Wrapping a socket in streams attaches its listeners, which can only happen once, so a
    // payload referencing the same socket twice must fail cleanly rather than double-wrap it.
    let socket = await openWebSocket((echoServer.address() as AddressInfo).port);
    let response = responseWithWebSocket(socket);
    try {
      // The error is thrown synchronously, while serializing the call.
      expect(() => api.relay(response, response)).toThrow(/only be sent over RPC once/);
    } finally {
      socket.close();
    }
  });

  it("closes the connection when the receiver never touches the socket", async () => {
    // Like an unread ReadableStream, an upgrade Response whose socket the receiver never
    // interacts with must release the connection when the call ends, not hold it until the
    // session dies.
    let socket = await openWebSocket((echoServer.address() as AddressInfo).port);
    let closed = nextEvent(socket, "close");
    await api.ignore(responseWithWebSocket(socket));
    await closed;
  });

  it("keeps the socket past the call when the receiver accept()s it", async () => {
    let socket = await openWebSocket((echoServer.address() as AddressInfo).port);
    await api.adopt(responseWithWebSocket(socket));
    expect(await api.sendOnAdopted("kept alive")).toBe("kept alive");
  });

  it("can proxy a tunneled socket through a second session", async () => {
    // A second RPC server that obtains an upgrade Response from the first server and returns it
    // onward. The socket then crosses two tunnels: client <-> proxy <-> TestApi server. (The
    // await matters: like a Response carrying a ReadableStream body, an upgrade Response can't
    // be proxied by returning the unresolved RpcPromise -- the app must receive the Response
    // before re-sending it.)
    class Proxy extends RpcTarget {
      async openEchoViaUpstream(): Promise<Response> {
        return await api.openEcho();
      }
    }

    let proxyServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    proxyServer.on("connection", socket => {
      newWebSocketRpcSession(socket as any, new Proxy());
    });
    let proxySocket = new NodeWebSocket(`ws://127.0.0.1:${await listening(proxyServer)}`);

    try {
      let proxy: any = newWebSocketRpcSession(proxySocket as any);
      let socket = (await proxy.openEchoViaUpstream()).webSocket;

      let echoed = nextEvent(socket, "message");
      socket.send("through two tunnels");
      expect((await echoed).data).toBe("through two tunnels");

      let closeEvent = nextEvent(socket, "close");
      socket.close(1000, "done");
      expect(await closeEvent).toMatchObject({ code: 1000, reason: "done" });
    } finally {
      proxySocket.close();
      await closeServer(proxyServer);
    }
  });
});

// Prove that a Cap'n Web session over a tunneled WebSocket behaves exactly like one over a
// direct WebSocket: run the same battery of test cases (session-battery.ts) that index.test.ts
// runs over a direct connection, but with the socket obtained through a tunnel. The topology:
//
//   client --[RPC session A]--> gateway.fetch(Request with Upgrade: websocket)
//                                  |
//                                  +--> opens a real WebSocket to the inner server, returns it
//                                       as an upgrade Response over session A
//
//   client then runs a *second* Cap'n Web session through response.webSocket, whose frames all
//   travel over session A's tunnel, and the battery exercises that inner session.
describe("Cap'n Web over a WebSocket obtained via fetch() over Cap'n Web", () => {
  // The inner RPC server -- the one the client ultimately wants to talk to.
  let innerServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  innerServer.on("connection", socket => {
    newWebSocketRpcSession(socket as any, new TestTarget());
  });

  // The gateway: an RPC server whose main target is a fetch handler that performs WebSocket
  // upgrades by connecting to the inner server on the caller's behalf.
  class Gateway extends RpcTarget {
    async fetch(request: Request): Promise<Response> {
      if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
        return new Response("Expected a WebSocket upgrade.", { status: 426 });
      }
      let port = (innerServer.address() as AddressInfo).port;
      return responseWithWebSocket(await openWebSocket(port));
    }
  }

  let gatewayServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  gatewayServer.on("connection", socket => {
    newWebSocketRpcSession(socket as any, new Gateway());
  });

  afterAll(async () => {
    await Promise.all([closeServer(gatewayServer), closeServer(innerServer)]);
  });

  registerSessionTestBattery(async () => {
    let gatewayPort = await listening(gatewayServer);
    await listening(innerServer);

    let gatewaySocket = new NodeWebSocket(`ws://127.0.0.1:${gatewayPort}`);
    let gateway: any = newWebSocketRpcSession(gatewaySocket as any);
    let response = await gateway.fetch(new Request("https://inner.example/rpc", {
      headers: { Upgrade: "websocket" },
    }));

    let stub = newWebSocketRpcSession<TestTarget>(response.webSocket);
    return {
      stub,
      async [Symbol.asyncDispose]() {
        stub[Symbol.dispose]();
        gatewaySocket.close();
      },
    };
  });
});

// The deferUpgradeMaterialization session option delivers an upgrade Response's tunneled socket
// as an opaque framed-byte { readable, writable, init } pair, for infrastructure that needs to
// carry the tunnel across a further hop (one that can serialize byte streams but not sockets)
// before materializeUpgrade() turns it back into a real socket at the final hop.
describe("deferred upgrade materialization", () => {
  let echoServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  echoServer.on("connection", socket => {
    socket.on("message", (data, isBinary) => socket.send(data, { binary: isBinary }));
  });

  // A server that greets in response to any message ("knock"), plus a log of its server-side
  // sockets so tests can observe closure from the provider's side. (The greeting is
  // knock-triggered rather than sent on connect because a `ws` socket's early messages can
  // arrive before the tunnel attaches its listeners; see webSocketToStreams.)
  let greetingSockets: any[] = [];
  let greetingServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  greetingServer.on("connection", socket => {
    greetingSockets.push(socket);
    socket.on("message", () => socket.send("welcome!"));
  });

  class DeferredTestApi extends RpcTarget {
    constructor(private echoPort: number, private greetingPort: number) { super(); }

    async openEcho(): Promise<Response> {
      return responseWithWebSocket(await openWebSocket(this.echoPort));
    }

    async openGreeting(): Promise<Response> {
      return responseWithWebSocket(await openWebSocket(this.greetingPort));
    }

    // An upgrade Response carrying a negotiated subprotocol header, to test that headers
    // survive deferral and re-materialization.
    async openEchoWithProtocol(): Promise<Response> {
      let response = new Response(null, { headers: { "sec-websocket-protocol": "itx" } });
      Object.defineProperty(response, "webSocket",
          { value: await openWebSocket(this.echoPort) });
      return response;
    }

    async openPlain(): Promise<Response> {
      return new Response("plain traffic", { status: 418 });
    }
  }

  // Identity adapter that re-chunks a byte stream into pieces of at most `size` bytes,
  // simulating a transport that splits frames at arbitrary boundaries (as a real workerd RPC
  // byte pipe may).
  function rechunkBytes(readable: ReadableStream, size: number): ReadableStream {
    let reader = readable.getReader();
    let buffer = new Uint8Array(0);
    return new ReadableStream({
      async pull(controller) {
        while (buffer.length === 0) {
          let { done, value } = await reader.read();
          if (done) {
            controller.close();
            return;
          }
          buffer = value;
        }
        controller.enqueue(buffer.subarray(0, Math.min(size, buffer.length)));
        buffer = buffer.length > size ? buffer.subarray(size) : new Uint8Array(0);
      },
      cancel(reason) {
        return reader.cancel(reason);
      },
    });
  }

  function textFrame(text: string): Uint8Array {
    let payload = new TextEncoder().encode(text);
    let frame = new Uint8Array(5 + payload.length);
    frame[0] = 0;
    new DataView(frame.buffer).setUint32(1, payload.length);
    frame.set(payload, 5);
    return frame;
  }

  let rpcServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  let clientSocket: NodeWebSocket;
  let api: any;

  beforeAll(async () => {
    let [echoPort, greetingPort, rpcPort] = await Promise.all([
      listening(echoServer), listening(greetingServer), listening(rpcServer)]);
    rpcServer.on("connection", socket => {
      newWebSocketRpcSession(socket as any, new DeferredTestApi(echoPort, greetingPort));
    });
    clientSocket = new NodeWebSocket(`ws://127.0.0.1:${rpcPort}`);
    api = newWebSocketRpcSession(clientSocket as any, undefined,
        { deferUpgradeMaterialization: true });
  });

  afterAll(async () => {
    clientSocket.close();
    await Promise.all([
      closeServer(rpcServer), closeServer(echoServer), closeServer(greetingServer)]);
  });

  it("delivers the tunnel as an opaque byte-stream pair", async () => {
    let response = await api.openGreeting();
    let webSocket = response.webSocket;
    expect(webSocket.readable).toBeInstanceOf(ReadableStream);
    expect(webSocket.writable).toBeInstanceOf(WritableStream);

    // The pair carries the tunnel's frames as opaque bytes -- so it can cross byte-oriented
    // boundaries like native workerd RPC. Speak the internal framing by hand (1 byte type,
    // 4 bytes big-endian length, payload; type 0 = text) to knock, and decode the reply. This
    // deliberately pins the frame format byte-for-byte: the encoder and decoder ship together,
    // so a drifted format would still round-trip green in every other test here and only fail
    // against an already-deployed peer -- in the worst case as nothing but a hung tunnel.
    let writer = webSocket.writable.getWriter();
    let reader = webSocket.readable.getReader();
    let knock = new TextEncoder().encode("knock");
    let frame = new Uint8Array(5 + knock.length);
    frame[0] = 0;
    new DataView(frame.buffer).setUint32(1, knock.length);
    frame.set(knock, 5);
    await writer.write(frame);

    let { value } = await reader.read();
    expect(value).toBeInstanceOf(Uint8Array);
    expect(value![0]).toBe(0);  // a text frame
    expect(new DataView(value!.buffer, value!.byteOffset).getUint32(1)).toBe(value!.length - 5);
    expect(new TextDecoder().decode(value!.subarray(5))).toBe("welcome!");

    // Closing the writable closes the provider's socket.
    let serverSocket = greetingSockets[greetingSockets.length - 1];
    let closed = new Promise(resolve => serverSocket.once("close", resolve));
    await writer.close();
    await closed;
  });

  it("materializes a real upgrade Response with materializeUpgrade()", async () => {
    let response = await api.openEcho();
    let materialized = materializeUpgrade(response.webSocket);
    let socket: any = (materialized as any).webSocket;
    expect(socket).toBeTruthy();

    let echoed = nextEvent(socket, "message");
    socket.send("hello");
    expect((await echoed).data).toBe("hello");

    let binaryEchoed = nextEvent(socket, "message");
    socket.send(new Uint8Array([4, 5, 6]));
    let bytes = (await binaryEchoed).data;
    expect(typeof bytes).not.toBe("string");
    expect(Array.from(bytes)).toEqual([4, 5, 6]);

    let closeEvent = nextEvent(socket, "close");
    socket.close(1000, "done");
    expect(await closeEvent).toMatchObject({ code: 1000, reason: "done" });
    expect(socket.readyState).toBe(3);  // CLOSED
  });

  it("survives arbitrary re-chunking of the byte pair", async () => {
    // A transport carrying the pair may split frames at any byte boundary; FrameDecoder must
    // reassemble them. Split into 3-byte chunks (every header is split), and also push a large
    // frame through moderate chunks.
    let webSocket = (await api.openEcho()).webSocket;
    let materialized = materializeUpgrade({
      readable: rechunkBytes(webSocket.readable, 3),
      writable: webSocket.writable,
    });
    let socket: any = (materialized as any).webSocket;

    let echoed = nextEvent(socket, "message");
    socket.send("split me");
    expect((await echoed).data).toBe("split me");

    // An empty text frame is a legal frame (zero-length payload).
    let emptyEchoed = nextEvent(socket, "message");
    socket.send("");
    expect((await emptyEchoed).data).toBe("");
    socket.close(1000, "");
  });

  it("survives re-chunking of a large binary frame", async () => {
    let webSocket = (await api.openEcho()).webSocket;
    let materialized = materializeUpgrade({
      readable: rechunkBytes(webSocket.readable, 64 * 1024),
      writable: webSocket.writable,
    });
    let socket: any = (materialized as any).webSocket;

    let big = new Uint8Array(1024 * 1024);
    for (let i = 0; i < big.length; i += 4096) big[i] = i & 0xff;
    let echoed = nextEvent(socket, "message");
    socket.send(big);
    let bytes = (await echoed).data;
    expect(bytes.length).toBe(big.length);
    expect(Array.from(bytes.subarray(0, 16))).toEqual(Array.from(big.subarray(0, 16)));
    expect(bytes[8192]).toBe(big[8192]);
    socket.close(1000, "");
  });

  it("decodes several frames coalesced into one chunk", async () => {
    // The inverse of re-chunking: one byte chunk carrying two whole frames.
    let webSocket = (await api.openGreeting()).webSocket;
    let writer = webSocket.writable.getWriter();
    let reader = webSocket.readable.getReader();

    let knock = textFrame("knock");
    let both = new Uint8Array(knock.length * 2);
    both.set(knock);
    both.set(knock, knock.length);
    await writer.write(both);

    // Two knocks produce two greetings.
    expect((await reader.read()).value![0]).toBe(0);
    expect((await reader.read()).value![0]).toBe(0);
    await writer.close();
  });

  it("materializes headers carried on the pair (e.g. a negotiated subprotocol)", async () => {
    let response = await api.openEchoWithProtocol();
    expect(response.headers.get("sec-websocket-protocol")).toBe("itx");

    // The pair itself carries the init, so infrastructure that forwards the whole pair object
    // across a hop preserves the headers without extra plumbing.
    let webSocket = response.webSocket;
    expect(webSocket.init).toBeTruthy();
    let materialized = materializeUpgrade(webSocket);
    expect(materialized.headers.get("sec-websocket-protocol")).toBe("itx");
    (materialized as any).webSocket.close(1000, "");
  });

  it("refuses to materialize the same pair twice", async () => {
    let webSocket = (await api.openEcho()).webSocket;
    let materialized = materializeUpgrade(webSocket);
    let socket: any = (materialized as any).webSocket;

    // The second call must fail fast (the pair is consumed), leaving the first materialization
    // fully working.
    expect(() => materializeUpgrade(webSocket)).toThrow(TypeError);

    let echoed = nextEvent(socket, "message");
    socket.send("still mine");
    expect((await echoed).data).toBe("still mine");
    socket.close(1000, "");
  });

  it("tears the tunnel down on a malformed frame instead of leaving it half-open", async () => {
    let webSocket = (await api.openGreeting()).webSocket;
    let serverSocket = greetingSockets[greetingSockets.length - 1];
    let closed = new Promise(resolve => serverSocket.once("close", resolve));

    // Frame type 7 does not exist. The write must reject AND the provider's socket must be
    // torn down (fail closed), not linger half-open.
    let writer = webSocket.writable.getWriter();
    await expect(writer.write(new Uint8Array([7, 0, 0, 0, 0]))).rejects.toThrow(/frame type/);
    await closed;

    // Likewise a header declaring an absurd length: fail closed immediately rather than
    // buffering toward 4 GiB.
    let webSocket2 = (await api.openGreeting()).webSocket;
    let serverSocket2 = greetingSockets[greetingSockets.length - 1];
    let closed2 = new Promise(resolve => serverSocket2.once("close", resolve));
    let writer2 = webSocket2.writable.getWriter();
    await expect(writer2.write(new Uint8Array([0, 255, 255, 255, 255])))
        .rejects.toThrow(/maximum/);
    await closed2;
  });

  it("treats a truncated frame at end-of-stream as an error, not a clean close", async () => {
    // Read direction: a byte stream that ends mid-frame means the transport lost data. The
    // materialized socket must fail (1006), not report a normal closure.
    let partial = textFrame("lost message").subarray(0, 7);
    let materialized = materializeUpgrade({
      readable: new ReadableStream({
        start(controller) {
          controller.enqueue(partial);
          controller.close();
        },
      }),
      writable: new WritableStream(),
    });
    let socket: any = (materialized as any).webSocket;
    let error = nextEvent(socket, "error");
    let close = nextEvent(socket, "close");
    socket.accept();
    expect((await error).error.message).toMatch(/truncated/);
    expect(await close).toMatchObject({ code: 1006 });

    // Write direction: closing the pair's writable with a partial frame buffered must reject
    // and tear the provider's socket down.
    let webSocket = (await api.openGreeting()).webSocket;
    let serverSocket = greetingSockets[greetingSockets.length - 1];
    let closed = new Promise(resolve => serverSocket.once("close", resolve));
    let writer = webSocket.writable.getWriter();
    await writer.write(partial);
    await expect(writer.close()).rejects.toThrow(/truncated/);
    await closed;
  });

  it("closes the provider socket when the pair is aborted or canceled", async () => {
    // These are the teardown verbs infrastructure (or a workerd RPC proxy whose far end died)
    // drives on the raw pair. Aborting the writable tears the provider down eagerly.
    {
      let webSocket = (await api.openGreeting()).webSocket;
      let serverSocket = greetingSockets[greetingSockets.length - 1];
      let closed = new Promise(resolve => serverSocket.once("close", resolve));
      await webSocket.writable.abort(new Error("infra teardown"));
      await closed;
    }
    // Canceling the readable propagates lazily, like any received stream's cancel: the sender
    // notices when it next has a message to deliver, then tears down.
    {
      let webSocket = (await api.openGreeting()).webSocket;
      let serverSocket = greetingSockets[greetingSockets.length - 1];
      let closed = new Promise(resolve => serverSocket.once("close", resolve));
      await webSocket.readable.cancel();
      let writer = webSocket.writable.getWriter();
      await writer.write(textFrame("knock"));  // provokes a greeting nobody can receive
      await closed;
    }
  });

  it("releases the tunnel when a deferred Response received in params is ignored", async () => {
    // The deferred mirror of the base feature's ignore() test: a server session with
    // deferUpgradeMaterialization receives an upgrade Response in params, never touches the
    // pair, and returns -- payload disposal must release the tunnel and close the socket.
    class DeferredIgnorer extends RpcTarget {
      async ignore(response: Response): Promise<void> {}
    }
    let server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    server.on("connection", s => {
      newWebSocketRpcSession(s as any, new DeferredIgnorer(),
          { deferUpgradeMaterialization: true });
    });
    let socket = new NodeWebSocket(`ws://127.0.0.1:${await listening(server)}`);
    try {
      let ignorer: any = newWebSocketRpcSession(socket as any);
      let provider = await openWebSocket((echoServer.address() as AddressInfo).port);
      let closed = nextEvent(provider, "close");
      await ignorer.ignore(responseWithWebSocket(provider));
      await closed;
    } finally {
      socket.close();
      await closeServer(server);
    }
  });

  it("leaves non-upgrade Responses untouched (mixed traffic)", async () => {
    // A deferring session (the relay) carries plain HTTP through live capabilities too; only
    // webSocket-bearing Responses are affected by the option.
    let response = await api.openPlain();
    expect(response.status).toBe(418);
    expect(await response.text()).toBe("plain traffic");
    expect((response as any).webSocket ?? null).toBeNull();
  });

  it("throttles the provider to the flow-control window while the pair is unread", async () => {
    // The bounded-memory property the relay leans on: capnweb stream acks fire as the pair is
    // READ, so an unconsumed deferred pair keeps the provider-side sender throttled to the
    // flow-control window instead of letting a firehose pool at the deferring receiver.
    //
    // In-memory transport pair, counting the bytes the provider side emits. Deterministic: with
    // zero reads there are zero acks, so the window never advances no matter how long we wait.
    let providerSent = 0;
    function pipeTransports(): [RpcTransport, RpcTransport] {
      function makeEndpoint(counted: boolean) {
        let queue: string[] = [];
        let waiting: ((m: string) => void) | undefined;
        return {
          deliver(message: string) {
            if (waiting) { let w = waiting; waiting = undefined; w(message); }
            else queue.push(message);
          },
          transport: undefined as unknown as RpcTransport,
          make(peer: () => { deliver(m: string): void }): RpcTransport {
            return {
              send(message: string) {
                if (counted) providerSent += message.length;
                peer().deliver(message);
              },
              receive() {
                if (queue.length > 0) return Promise.resolve(queue.shift()!);
                return new Promise<string>(resolve => { waiting = resolve; });
              },
            };
          },
        };
      }
      let a = makeEndpoint(true), b = makeEndpoint(false);
      return [a.make(() => b), b.make(() => a)];
    }

    // A minimal provider-side socket the test can flood on demand.
    let listeners = new Map<string, ((event: any) => void)[]>();
    let providerSocket = {
      send(data: unknown) {},
      close(code?: number, reason?: string) {
        for (let l of listeners.get("close") ?? []) l({ code: code ?? 1005, reason: reason ?? "" });
      },
      addEventListener(type: string, listener: (event: any) => void) {
        let list = listeners.get(type) ?? [];
        list.push(listener);
        listeners.set(type, list);
      },
    };

    class FloodApi extends RpcTarget {
      async openFlood(): Promise<Response> {
        return responseWithWebSocket(providerSocket);
      }
    }

    let [providerTransport, receiverTransport] = pipeTransports();
    new RpcSession(providerTransport, new FloodApi());
    let receiver = new RpcSession<FloodApi>(receiverTransport, undefined,
        { deferUpgradeMaterialization: true });
    let response: any = await (receiver.getRemoteMain() as any).openFlood();
    let webSocket = response.webSocket;

    let settle = () => new Promise(resolve => setTimeout(resolve, 50));
    let baseline = providerSent;
    let message = "x".repeat(32 * 1024);
    for (let i = 0; i < 1024; i++) {
      for (let l of listeners.get("message") ?? []) l({ data: message });
    }
    await settle();
    let unreadSent = providerSent - baseline;
    // 32 MiB was offered; only about one flow-control window's worth may cross.
    expect(unreadSent).toBeGreaterThan(0);
    expect(unreadSent).toBeLessThan(4 * 1024 * 1024);

    // Reading the pair acks chunks and lets more flow.
    let reader = webSocket.readable.getReader();
    for (let i = 0; i < 8; i++) await reader.read();
    await settle();
    expect(providerSent - baseline).toBeGreaterThan(unreadSent);
    expect(providerSent - baseline).toBeLessThan(16 * 1024 * 1024);
  });

  it("fails closed when the RPC session dies under a materialized socket", async () => {
    // A dedicated session, so terminating the transport doesn't break the shared one.
    let transport = new NodeWebSocket(
        `ws://127.0.0.1:${(rpcServer.address() as AddressInfo).port}`);
    let session: any = newWebSocketRpcSession(transport as any, undefined,
        { deferUpgradeMaterialization: true });
    let materialized = materializeUpgrade((await session.openEcho()).webSocket);
    let socket: any = (materialized as any).webSocket;

    let echoed = nextEvent(socket, "message");
    socket.send("alive");
    expect((await echoed).data).toBe("alive");

    // Kill the RPC transport abnormally: the socket must fail (error and/or close 1006), not
    // hang and not report a clean close.
    let close = nextEvent(socket, "close");
    transport.terminate();
    expect(await close).toMatchObject({ code: 1006 });
    expect(socket.readyState).toBe(3);  // CLOSED
  });
});

// Prove that deferring and re-materializing changes nothing about the socket's behavior: run the
// full session battery over a socket that took the long way around -- tunneled over RPC,
// delivered as the opaque byte pair, then rebuilt by materializeUpgrade() -- mirroring the
// "Cap'n Web over a WebSocket obtained via fetch() over Cap'n Web" battery above.
describe("Cap'n Web over a deferred-then-materialized WebSocket", () => {
  let innerServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  innerServer.on("connection", socket => {
    newWebSocketRpcSession(socket as any, new TestTarget());
  });

  class Gateway extends RpcTarget {
    async fetch(request: Request): Promise<Response> {
      if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
        return new Response("Expected a WebSocket upgrade.", { status: 426 });
      }
      let port = (innerServer.address() as AddressInfo).port;
      return responseWithWebSocket(await openWebSocket(port));
    }
  }

  let gatewayServer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  gatewayServer.on("connection", socket => {
    newWebSocketRpcSession(socket as any, new Gateway());
  });

  afterAll(async () => {
    await Promise.all([closeServer(gatewayServer), closeServer(innerServer)]);
  });

  registerSessionTestBattery(async () => {
    let gatewayPort = await listening(gatewayServer);
    await listening(innerServer);

    let gatewaySocket = new NodeWebSocket(`ws://127.0.0.1:${gatewayPort}`);
    let gateway: any = newWebSocketRpcSession(gatewaySocket as any, undefined,
        { deferUpgradeMaterialization: true });
    let response = await gateway.fetch(new Request("https://inner.example/rpc", {
      headers: { Upgrade: "websocket" },
    }));

    let materialized = materializeUpgrade(response.webSocket);
    let stub = newWebSocketRpcSession<TestTarget>((materialized as any).webSocket);
    return {
      stub,
      async [Symbol.asyncDispose]() {
        stub[Symbol.dispose]();
        gatewaySocket.close();
      },
    };
  });
});
