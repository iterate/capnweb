Examples

- batch-pipelining: Node server + client. Shows batching and pipelining to execute a dependent sequence of RPC calls in a single HTTP round trip, with timing vs sequential.
- dynamic-capabilities: Cloudflare Worker + Durable Object. Shows PR #2's runtime capability registry: one client publishes a capability, another calls it through `fallbackCall`.
- worker-react: Cloudflare Worker backend + React frontend. Shows the same pattern from a browser app, served by the Worker.

Notes

- Most examples import from `../../dist/index.js`; run `npm run build` at the repo root first.
  `dynamic-capabilities` is a standalone Workers package that depends on the repo via `file:../..`.
- Requires Node 18+ (built-in `fetch`, `Request`, `Response`).
