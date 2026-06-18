---
"capnweb": minor
---

Add an opt-in `fallbackCall` for `RpcTarget`s. If a path followed over RPC reaches an `RpcTarget` that doesn't declare the requested property as a method or getter, and the target defines a method under the exported `fallbackCall` symbol, the unmatched remainder of the path and the call's arguments are forwarded to that method (which may be async) instead of resolving to `undefined`. Promise pipelining is preserved — the whole dotted path still arrives in a single call.
