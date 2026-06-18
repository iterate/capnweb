import { exports } from "cloudflare:workers";
import { expect, it } from "vitest";
import { fallbackCall, newWebSocketRpcSession, RpcTarget } from "capnweb";

async function connect(): Promise<any> {
  let response = await exports.default.fetch("https://example.test/", {
    headers: { Upgrade: "websocket" },
  });
  expect(response.status).toBe(101);

  let socket = response.webSocket;
  expect(socket).toBeTruthy();
  socket!.accept();

  return newWebSocketRpcSession(socket!);
}

it("routes dynamically provided capabilities through the shared Durable Object", async () => {
  using clientA = await connect();
  using clientB = await connect();

  await clientB.provideCapability("runSwiftCode", async (src: string) => {
    return { language: "swift", stdout: `ran: ${src}` };
  });

  expect(await clientA.runSwiftCode("print(1)")).toStrictEqual({
    language: "swift",
    stdout: "ran: print(1)",
  });

  await clientB.revokeCapability("runSwiftCode");
});

it("routes Slack-shaped nested capability paths through the provider", async () => {
  class SlackLikeClient extends RpcTarget {
    [fallbackCall](path: (string | number)[], args: unknown[]) {
      return { path, args };
    }
  }

  using clientA = await connect();
  using clientB = await connect();

  await clientB.provideCapability("slack", new SlackLikeClient());

  expect(await clientA.slack.chat.postMessage({
    channel: "C123",
    text: "hi",
  })).toStrictEqual({
    path: ["chat", "postMessage"],
    args: [{ channel: "C123", text: "hi" }],
  });

  await clientB.revokeCapability("slack");
});

it("replaces and revokes provided capabilities", async () => {
  using clientA = await connect();
  using clientB = await connect();

  await clientB.provideCapability("tool", () => "first");
  expect(await clientA.tool()).toBe("first");

  await clientB.provideCapability("tool", () => "second");
  expect(await clientA.tool()).toBe("second");

  await clientB.revokeCapability("tool");
  await clientB.provideCapability("tool", () => "third");
  expect(await clientA.tool()).toBe("third");
  await clientB.revokeCapability("tool");
});
