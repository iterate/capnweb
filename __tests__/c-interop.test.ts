// Copyright (c) 2026 Iterate
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { spawn, spawnSync } from "node:child_process";
import type { ChildProcessWithoutNullStreams } from "node:child_process";
import { mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createInterface } from "node:readline";
import { beforeAll, describe, expect, it } from "vitest";
import { RpcSession } from "../src/index.js";
import type { RpcTransport } from "../src/index.js";
import knownFailures from "./c-interop-known-failures.js";
import { registerSessionTestBattery } from "./session-battery.js";
import type { SessionFixture } from "./session-battery.js";
import { TestTarget } from "./test-util.js";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const buildDirectory = resolve(repositoryRoot, "c/build");
const peerExecutable = resolve(buildDirectory, "capnweb-native-peer");
const resourceProfilerExecutable =
    resolve(buildDirectory, "capnweb-resource-profile");

class ChildProcessTransport implements RpcTransport {
  readonly #child: ChildProcessWithoutNullStreams;
  readonly #messages: string[] = [];
  readonly #waiters: Array<{
    resolve: (message: string) => void;
    reject: (error: Error) => void;
  }> = [];
  #failure?: Error;
  #closing = false;

  constructor() {
    this.#child = spawn(peerExecutable, [], {
      cwd: repositoryRoot,
      stdio: ["pipe", "pipe", "pipe"],
    });

    createInterface({ input: this.#child.stdout }).on("line", line => {
      if (process.env.CAPNWEB_C_TRACE) console.error(`C -> TS ${line}`);
      let waiter = this.#waiters.shift();
      if (waiter) {
        waiter.resolve(line);
      } else {
        this.#messages.push(line);
      }
    });

    let standardError = "";
    this.#child.stderr.setEncoding("utf8");
    this.#child.stderr.on("data", chunk => {
      standardError += chunk;
    });
    this.#child.once("error", error => this.#rejectAll(error));
    this.#child.once("close", (code, signal) => {
      if (this.#failure || this.#closing) return;
      this.#rejectAll(new Error(
          `C peer exited before transport shutdown (code=${code}, signal=${signal})` +
          (standardError ? `: ${standardError.trim()}` : "")));
    });
  }

  send(message: string): Promise<void> {
    if (process.env.CAPNWEB_C_TRACE) console.error(`TS -> C ${message}`);
    return new Promise((resolveSend, rejectSend) => {
      this.#child.stdin.write(`${message}\n`, error => {
        if (error) {
          rejectSend(error);
        } else {
          resolveSend();
        }
      });
    });
  }

  receive(): Promise<string> {
    let message = this.#messages.shift();
    if (message) return Promise.resolve(message);
    if (this.#failure) return Promise.reject(this.#failure);
    return new Promise((resolveMessage, rejectMessage) => {
      this.#waiters.push({ resolve: resolveMessage, reject: rejectMessage });
    });
  }

  async close(): Promise<void> {
    this.#closing = true;
    if (this.#child.exitCode !== null) return;
    this.#child.stdin.end();
    await new Promise<void>(resolveClose => {
      let killTimer = setTimeout(() => this.#child.kill(), 1_000);
      this.#child.once("close", () => {
        clearTimeout(killTimer);
        resolveClose();
      });
    });
  }

  #rejectAll(error: Error) {
    this.#failure = error;
    for (let waiter of this.#waiters.splice(0)) waiter.reject(error);
  }
}

class NativeSessionFixture implements SessionFixture {
  readonly stub;
  readonly #transport: ChildProcessTransport;

  constructor() {
    this.#transport = new ChildProcessTransport();
    let session = new RpcSession<TestTarget>(this.#transport);
    this.stub = session.getRemoteMain();
  }

  async [Symbol.asyncDispose]() {
    this.stub[Symbol.dispose]();
    await this.#transport.close();
  }
}

beforeAll(() => {
  mkdirSync(buildDirectory, { recursive: true });
  for (let args of [
    ["-S", resolve(repositoryRoot, "c"), "-B", buildDirectory, "-DCAPNWEB_SANITIZE=ON"],
    ["--build", buildDirectory, "--parallel"],
  ]) {
    let result = spawnSync("cmake", args, {
      cwd: repositoryRoot,
      encoding: "utf8",
    });
    if (result.status !== 0) {
      throw new Error(`cmake ${args.join(" ")} failed:\n${result.stdout}\n${result.stderr}`);
    }
  }
  let testResult = spawnSync(
      "ctest",
      ["--test-dir", buildDirectory, "--output-on-failure"],
      { cwd: repositoryRoot, encoding: "utf8" });
  if (testResult.status !== 0) {
    throw new Error(
        `ctest failed:\n${testResult.stdout}\n${testResult.stderr}`);
  }
});

describe("C embedded peer compatibility", () => {
  it("surfaces native session failures through exit status and stderr", () => {
    let result = spawnSync(peerExecutable, [], {
      cwd: repositoryRoot,
      encoding: "utf8",
      input: "{\n",
    });

    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain("CAPNWEB_E_INVALID_MESSAGE");
    expect(result.stdout).toContain(
        "[\"abort\",[\"error\",\"Error\",\"CAPNWEB_E_INVALID_MESSAGE\"]]");
  });

  it("classifies an oversized native transport frame as an input limit", () => {
    let result = spawnSync(peerExecutable, [], {
      cwd: repositoryRoot,
      encoding: "utf8",
      input: `${" ".repeat(65_537)}\n`,
    });

    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain("CAPNWEB_E_INPUT_LIMIT");
    expect(result.stderr).not.toContain("CAPNWEB_E_INVALID_MESSAGE");
    expect(result.stdout).toBe("");
  });

  it("reports the no-allocator policy without claiming process heap telemetry", () => {
    let result = spawnSync(resourceProfilerExecutable, [], {
      cwd: repositoryRoot,
      encoding: "utf8",
    });

    expect(result.status).toBe(0);
    let profile = JSON.parse(result.stdout) as Record<string, unknown>;
    expect(profile.libraryHeapPolicy).toBe("no allocator dependency");
    expect(profile).not.toHaveProperty("heapAllocations");
  });

  registerSessionTestBattery(
      async () => new NativeSessionFixture(),
      { knownFailures });
});
