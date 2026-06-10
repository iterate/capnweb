// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { expect, it, describe, beforeAll, afterAll } from "vitest";
import type { AddressInfo } from "node:net";
import { WebSocket as NodeWebSocket, WebSocketServer } from "ws";
import { newWebSocketRpcSession, RpcTarget } from "../src/index.js";
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
