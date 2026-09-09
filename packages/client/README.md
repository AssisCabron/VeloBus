# @velobus/client (experimental v0.2)

A Node.js 20+ client for backend APIs and microservices communicating through VeloBus. A caller awaits a service response while Node.js continues processing other I/O. The broker dispatches each admitted call to one worker, bounds waiting work, and enforces deadlines. Replicas share a route without broadcasting requests.

Binary TCP protocol, TypeScript declarations, ESM and CommonJS, zero runtime dependencies. The package name is provisional and **has not been published to npm**. The package remains private to prevent accidental publication before naming and release review.

Build from the repository, then install locally in your application:

```sh
npm --prefix packages/client ci
npm --prefix packages/client run build
# In a different application's directory:
npm install /absolute/path/to/velobus/packages/client
```

## Backend worker

```ts
import { connect } from '@velobus/client';

const bus = await connect({ host: '127.0.0.1', port: 7447 });
const service = await bus.handleJSON<{ id: string }, { id: string; name: string }>(
  'users.get',
  async ({ id }, { signal }) => {
    // Pass signal to downstream I/O where supported; validate your input schema.
    signal.throwIfAborted();
    return { id, name: 'Example user' };
  },
  { concurrency: 8, queueLimit: 128, onError: error => console.error(error.message) },
);

// For shutdown:
// await service.close();
// await bus.close();
```

Run multiple backend processes registering `users.get` to provide more workers. All replicas for a route must agree on `queueLimit`. Each `handle()` creates its own authenticated child connection, inheriting the parent's host, port and token.

## Calling from an API

```ts
import { connect, BrokerError } from '@velobus/client';

const bus = await connect({ host: '127.0.0.1', port: 7447 });
try {
  const user = await bus.requestJSON<{ id: string; name: string }>(
    'users.get', { id: '123' }, { timeoutMs: 2000 },
  );
  console.log(user);
} catch (error) {
  if (error instanceof BrokerError && error.code === 9) {
    // Admission rejected: for example, return HTTP 503 from an API gateway.
  } else if (error instanceof BrokerError && error.code === 10) {
    // Broker deadline exceeded: for example, return HTTP 504.
  } else {
    throw error;
  }
} finally {
  await bus.close();
}
```

CommonJS: `const { connect } = require('@velobus/client')`.

`request(route, Uint8Array | string, { timeoutMs? })` returns a response with Buffer `payload`, `text()` and `json<T>()`. `requestJSON<T>(route, input, options)` serializes input and parses the response. Generic types describe expected data; they do not validate application schemas. Inputs are captured before awaiting, so subsequent mutation does not change an already-issued request.

`handle(route, handler, options)` accepts a handler returning `Uint8Array`, string, or a Promise of either. Its request context includes `payload`, `text()`, `json<T>()`, `signal`, and `remainingMs`. `handleJSON(route, (body, context) => result, options)` decodes input JSON and serializes the result.

## Concurrency, deadlines and lifecycle

- Worker concurrency is 1–32 per SDK handle, default 8, matching the broker's per-connection operation cap. Each slot follows `TAKE → await handler → await COMPLETE → TAKE`. No new handler runs in that slot before the previous completion is acknowledged.
- `queueLimit` is 1–4096 waiting calls per route, default 128. It excludes running calls. Broker-wide call and byte budgets additionally bound admission.
- RPC `timeoutMs` is 1–30000 milliseconds, default 5000, including broker queue wait. The transport timeout for REQUEST is at least the RPC deadline plus 1000 milliseconds, allowing the broker's deadline response to arrive without unnecessarily closing a healthy caller connection.
- The handler's `AbortSignal` is triggered when its received remaining deadline expires. Cancellation is cooperative. A handler that ignores the signal can continue executing and producing side effects. The SDK waits for its actual settlement before sending COMPLETE and reusing that slot, including after the caller's deadline.
- Worker exceptions are sent to the caller as `HANDLER_ERROR` with a valid UTF-8 message limited to 4096 bytes. The worker loop continues after completion. Do not include credentials or sensitive data in thrown messages.
- `service.close()` stops taking work, closes the worker connection and aborts active signals. It does not wait indefinitely for uncooperative handlers. Active callers fail through the broker; side effects already started may still finish.
- `client.close()` also closes its child service handles. Await calls and publication receipts before closing when their results matter. An unexpected worker connection/protocol failure stops the handle, aborts active signals and invokes `onError` once; without a callback it emits a Node warning. There is no rejected-loop retry.

Request/reply is volatile: it is neither stored in the event WAL nor replayed. A call is dispatched to at most one worker by the broker. Timeout or disconnect after execution starts can leave the outcome unknown. There is no implicit reconnect, retry, exactly-once execution, or cross-process cancellation guarantee.

## Errors and transport bounds

`connect({ host, port, token, timeoutMs, maxPendingRequests })` defaults to `127.0.0.1:7447`, a 5-second connection/transport timeout and at most 32 pending requests (configurable 1–256). Caller pending counts include frames awaiting TCP backpressure. The application queue is bounded by the configured request count and the 1 MiB maximum frame size; the socket queue pauses when `write()` reports backpressure. A handle has a separate queue bounded to its declared concurrency.

| Error | Meaning |
| --- | --- |
| `BrokerError`, code 8 | `NO_SERVICE`: no worker registered for the route. |
| `BrokerError`, code 9 | `OVERLOADED`: route queue or broker admission budget is full. |
| `BrokerError`, code 10 | `DEADLINE_EXCEEDED`: broker deadline expired. |
| `BrokerError`, code 11 | `SERVICE_UNAVAILABLE`: worker disconnected. |
| `BrokerError`, code 12 | `HANDLER_ERROR`: worker handler failed. |
| `BackpressureError` | Local pending limit reached; await outstanding operations. |
| `RequestTimeoutError` | Transport timeout closes the connection and rejects all pending operations. |
| `ProtocolError` | Malformed, oversized, unmatched or invalid response closes the connection. |
| `ConnectionClosedError` | Peer disconnect or explicit client close. |

Normal broker application errors preserve the caller connection. Original Node connection failures retain codes such as `ECONNREFUSED`. `await bus.ping()` checks a round trip. `await bus.stats()` exposes event storage metrics and, on v0.2 brokers, an `rpc` object with admission totals, queued/running counts, bytes and per-route workers/capacity. `lastSequence` is a decimal string to preserve precision.

Routes and topics accept 1–255 ASCII letters, digits, dots, underscores and hyphens. Request and response payloads are at most 262144 bytes. Transport is plain TCP. Use loopback or an authenticated private tunnel; no browser or TLS transport is included in this release.

## Supporting event APIs

Existing PUBLISH/FETCH APIs remain compatible for applications that need retained events or state updates:

```ts
await bus.publish('service.events', Buffer.from([0, 1, 255]));
await bus.publishJSON('users.updated', { id: '123', enabled: true }, { key: '123' });
await bus.publishBatch([
  { topic: 'service.state', key: 'api-1', payload: '{"healthy":true}' },
  { topic: 'service.state', key: 'api-2', payload: '{"healthy":false}' },
]);

const batch = await bus.fetch('users.updated', {
  after: 0n, mode: 'all', limit: 128, maxBytes: 524288,
});
for (const event of batch.events) {
  console.log(event.sequence, event.key, event.json());
}
```

Publication receipts have bigint `firstSequence` and `lastSequence`, `count` and `durable`. Only a synchronized disk-mode ACK sets `durable: true`. Batches have 1–256 records and must fit one frame; keys accept up to 1024 UTF-8 bytes. Payloads are copied into the outbound frame before returning the Promise.

Fetch returns `{ cursor, events, hasMore }`; topic `'*'` scans all topics. Events have bigint `sequence`, `topic`, `key`, Buffer `payload`, `text()` and `json<T>()`. `mode: 'all'` preserves every matching event. `mode: 'latest'` combines matching nonempty keys **inside the bounded scan/batch**; it does not guarantee the globally newest value across arbitrary backlog and does not delete history. Empty keys never combine.

For durable application checkpoints:

```ts
const batch = await bus.fetch('users.updated', { after: checkpoint });
for (const event of batch.events) await processIdempotently(event);
await saveCheckpoint(batch.cursor.toString()); // only after the entire batch succeeds
```

The cursor includes scanned nonmatching events and may advance for empty batches. Persist `batch.cursor`, not the last returned event sequence. Checkpoints belong to the same broker log lifetime; memory mode resets sequences after restart. Replay after failure may duplicate already processed events. There are no durable server-side consumer groups or application database transactions.

`subscribe(topic, { after?, mode?, limit?, maxBytes?, pollIntervalMs?, signal? })` is a pull-based async iterator for convenience. It holds at most one fetched batch, does not prefetch while yielding, and waits between empty/idle batches (default 100ms). Aborting ends the iterator gracefully without closing the shared client. An already-sent fetch remains observed until response or timeout. Use explicit fetch batches for durable checkpoints.

Run `npm test` to build both module formats and verify binary framing, malformed responses, authentication, request correlation, deadlines, concurrency, worker cleanup, TCP backpressure and event compatibility. Real-broker integration tests live at the repository root.
