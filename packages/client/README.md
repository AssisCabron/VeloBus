# nodara

Experimental bounded request/reply for Node.js microservices. Rust brokers run separately; this MIT-licensed npm package provides the dependency-free JavaScript client, TypeScript declarations, ESM and CommonJS. Requires Node.js 20+.

```sh
npm install nodara
```

## Multi-broker API

```js
import { connectCluster, AmbiguousResultError } from 'nodara';

const bus = await connectCluster({
  brokers: [
    { id: 'a', host: '127.0.0.1', port: 7447 },
    { id: 'b', host: '127.0.0.1', port: 7448 },
  ],
  // token: process.env.NODARA_TOKEN,
  maxPendingRequests: 256,
});

const service = await bus.handleJSON('catalog.get', async ({ id }, context) => {
  // Replace this with your backend. Pass context.signal to cancellable I/O.
  return { id, available: true };
}, { concurrency: 8, queueLimit: 128 });

try {
  const result = await bus.requestJSON('catalog.get', { id: 'A-42' }, { timeoutMs: 1500 });
  console.log(result);
} catch (error) {
  if (error instanceof AmbiguousResultError) {
    console.error('Unknown outcome on broker', error.brokerId);
    // Reconcile by business operation ID. Do not blindly replay writes.
  } else throw error;
} finally {
  await service.close();
  await bus.close();
}
```

Workers and callers normally live in separate processes and reuse their connections. A cluster accepts 1–8 explicit endpoints, chooses the connected broker with the fewest local pending operations, rotates ties, reconnects failed nodes with bounded exponential backoff, and restores service registrations. Startup requires at least one reachable/authenticated node.

`concurrency` is shared across all brokers for a service in this Node process. There may additionally be at most `broker count × concurrency` assigned requests awaiting/running through its bounded local admission gate. The gate preserves capacity held by uncooperative handlers after connection loss. `queueLimit` and broker byte/call budgets are per broker, not cluster-wide.

`bus.nodes()` returns connection status without tokens. `service.nodes()` lists currently registered brokers. See [CLUSTER.md](https://github.com/AssisCabron/VeloBus/blob/main/docs/CLUSTER.md) for defaults, failure semantics and capacity accounting.

## Single broker and binary payloads

```js
import { connect } from 'nodara';
const bus = await connect({ host: '127.0.0.1', port: 7447 });
const worker = await bus.handle('echo', request => request.payload);
const response = await bus.request('echo', Buffer.from([0, 255]));
console.log(response.payload);
await worker.close();
await bus.close();
```

`requestJSON<T>()` and `handleJSON<Input, Output>()` are generic TypeScript wrappers around binary `request` / `handle`. Types do not replace runtime input validation. CommonJS: `const { connectCluster, connect } = require('nodara')`.

## Guarantees and errors

- RPC is volatile. No queue replication, consensus, durable RPC replay or exactly-once external effects.
- A failed broker stops receiving new calls once detected. The first call during an undetected failure may have an unknown outcome.
- Explicit `NO_SERVICE` (8) and `OVERLOADED` (9) rejections can move to another node before dispatch, once per node within the remaining deadline. Local pre-send backpressure may also select another node.
- Transport/protocol failures after a request attempt produce `AmbiguousResultError` (`outcome: 'unknown'`) and are never automatically replayed.
- `ClusterUnavailableError` indicates no node accepted this call; `BackpressureError` is local capacity rejection. Deadline (10), service unavailable (11) and handler errors (12) are not retried.
- Timeout includes broker waiting and service work. Worker cancellation is cooperative; external side effects may continue. Applications need durable business idempotency/reconciliation for retries.
- `close()` stops intake and signals cancellation; it does not wait for a handler that ignores cancellation.
- Tokens authenticate TCP connections but do not encrypt traffic. Protect remote transport externally; no built-in TLS or per-route ACLs.

The cluster API intentionally excludes event publication/fetch/subscription: independent brokers do not share history or sequence numbers. Single-node `connect()` retains `publish`, `publishBatch`, `fetch`, `subscribe`, `stats`, `ping`, and `close`.

## Broker and agent documentation

Build a broker from the [repository](https://github.com/AssisCabron/VeloBus): `cargo build --release`, then run `target/release/nodara --memory --listen 127.0.0.1:7447`. The npm package does not install a native server binary or start infrastructure automatically.

[LLM.txt](https://github.com/AssisCabron/VeloBus/blob/main/LLM.txt) and `llms.txt` are also included in the npm tarball. Historical VeloBus v0.2 benchmarks are not measurements of the Nodara multi-broker client. Version 0.3 remains experimental.
