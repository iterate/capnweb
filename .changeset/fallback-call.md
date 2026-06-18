---
"capnweb": minor
---

Add an opt-in `fallbackCall` for `RpcTarget`s. If a path followed over RPC reaches an `RpcTarget` that doesn't declare the requested property as a method or getter, and that name is not an instance property or local stub helper, a target method under the exported `fallbackCall` symbol receives the unmatched remainder of the path and the call's arguments. The handler may be async, and promise pipelining is preserved: the whole dotted path is sent in one call with the same round-trip cost as a declared method. Canonical numeric property indexes such as `stub.items[0]` arrive as numeric path segments; RPC/Promise helper names such as `then`, `map`, and `dup` remain local stub APIs.
