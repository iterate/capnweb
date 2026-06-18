import { DurableObject } from "cloudflare:workers";
import { fallbackCall, newWorkersWebSocketRpcResponse, RpcTarget } from "capnweb";

type Capability = ((...args: unknown[]) => unknown) & {
  dup(): Capability;
  [Symbol.dispose](): void;
};

class Session extends RpcTarget {
  constructor(private capabilityStore: any) {
    super();
  }

  provideCapability(name: string, cap: unknown) {
    return this.capabilityStore.provideCapability(name, cap);
  }

  revokeCapability(name: string) {
    return this.capabilityStore.revokeCapability(name);
  }

  [fallbackCall](path: (string | number)[], args: unknown[]) {
    return this.capabilityStore.invokeCapability(path, args);
  }
}

export class CapabilityStore extends DurableObject<Env> {
  #caps = new Map<string, Capability>();

  provideCapability(name: string, cap: unknown) {
    if (typeof (<Capability>cap).dup !== "function") {
      throw new TypeError("provided capabilities must be RPC stubs with dup()");
    }

    this.revokeCapability(name);
    this.#caps.set(name, (<Capability>cap).dup());
  }

  revokeCapability(name: string) {
    let cap = this.#caps.get(name);
    if (cap) {
      cap[Symbol.dispose]();
      this.#caps.delete(name);
    }
  }

  invokeCapability(path: (string | number)[], args: unknown[]) {
    let [name, ...rest] = path;
    let cap = this.#caps.get(String(name));
    if (!cap) {
      throw new Error(`no capability named "${String(name)}"`);
    }

    // The provider owns the capability's internal shape. Forward the remaining dynamic path so
    // Slack-shaped calls like `session.slack.chat.postMessage(...)` reach the client-provided
    // fallback as `["chat", "postMessage"]`; otherwise only root callables would work.
    let target = rest.reduce<any>((value, part) => value[part], cap);
    return target(...args);
  }
}

export default {
  fetch(request: Request, env: Env) {
    let store = env.CAP_STORE.getByName("shared");
    return newWorkersWebSocketRpcResponse(request, new Session(store));
  },
} satisfies ExportedHandler<Env>;
