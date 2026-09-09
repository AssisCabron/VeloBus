# Request/reply extension (VeloBus v0.2)

Product focus: an intermediary for synchronous request/reply between backend APIs and microservices, with bounded admission and worker concurrency. The caller awaits the response; network/runtime I/O remains nonblocking. This does not mean blocking the Node.js event loop. Existing event PUBLISH/FETCH remain compatible supporting primitives, not the product's main integration path.

Use the existing binary envelope and HELLO version 1; new request opcodes 6..9. Replies opcode|0x80. All scalars big-endian and strings as in PROTOCOL.md. Route names use topic validation. Payload max 262144 bytes. RPC is volatile, not persisted or retried. Every accepted request is delivered to at most one worker. Outcome of started work may be unknown after timeout/disconnection. This is not exactly-once execution.

## REGISTER = 0x06

`route:str | concurrency:u16 (1..256) | queueLimit:u16 (1..4096)`; success empty.

Worker identity is the authenticated TCP connection. One route registration per connection; repeated REGISTER is invalid. Multiple workers for a route must agree on queueLimit. Each worker may execute at most concurrency jobs at once. queueLimit bounds that route's waiting (not running) requests. It is never a broadcast. A route disappears when its last worker disconnects; queued callers are failed immediately. SDK handle() uses a dedicated authenticated child connection so registration doesn't interfere with caller operations.

## REQUEST = 0x07

`route:str | timeoutMs:u32 (1..30000) | payloadLength:u32 | payload`.

Success: `payloadLength:u32 | payload`. Error uses existing 0xff error envelope. The server measures the deadline from request admission, including queue wait and execution. No service => NO_SERVICE; full waiting queue or global budget => OVERLOADED, promptly. An expired queued request must never execute. Expiration after dispatch replies DEADLINE_EXCEEDED, but must not release the worker slot until the worker actually completes or disconnects. A timed-out handler may still perform side effects.

The TCP server must multiplex bounded concurrent operations per authenticated connection (default cap 32) so a slow REQUEST does not prevent PING, another request, worker TAKE, or COMPLETE on the same connection. Replies correlate by requestId; duplicate currently in-flight IDs are protocol errors. Bound output and in-flight requests; no unbounded task spawn or buffers.

## TAKE = 0x08

`waitMs:u32 (1..1000)`; registered workers only.

Success: `present:u8`; if present=1, append `callId:u64 | remainingMs:u32 | payloadLength:u32 | payload`. present=0 means no work before waitMs elapsed. Takes may wait efficiently for work; no busy polling. Worker TAKE concurrency and executing jobs must respect configured concurrency. The SDK may issue one concurrent TAKE loop per worker slot. Only a single worker receives a call. Pending TAKE attempts do not reserve executing slots unless work is assigned, but must themselves be bounded.

## COMPLETE = 0x09

`callId:u64 | status:u8 (0 success, 1 handler error) | payloadLength:u32 | payload`; success empty.

Registered owner connection only. Completion releases the assigned execution slot even if the caller's deadline already elapsed. On status=0, deliver payload to the waiting caller. On status=1, payload is a UTF-8 error message (SDK sends <=4096 bytes) and caller receives HANDLER_ERROR. Unknown job, duplicate completion, wrong owner or invalid status => INVALID/UNAUTHORIZED as appropriate. No transparent retry after a worker failure.

## New errors

8 NO_SERVICE, 9 OVERLOADED, 10 DEADLINE_EXCEEDED, 11 SERVICE_UNAVAILABLE, 12 HANDLER_ERROR. These are normal application errors and do not close a healthy caller connection. Internal SDK socket/request timeout remains transport failure and closes the connection. For REQUEST, SDK transport timeout must allow the RPC deadline plus a small margin (>=1000ms); server deadline is the primary user-visible timeout.

## Resource lifecycle

Default global RPC budget: 1024 waiting/running calls and 16 MiB conservatively accounted request bytes, adjustable via `--max-rpc-calls`, `--max-rpc-bytes`. Maximum registered routes <= connection cap. Entries, payloads, waiters and counters must be cleaned on success, queued timeout, caller disconnect and worker disconnect. Started calls keep a capacity slot until actual worker completion even if the caller disappears, preventing expiry storms from overloading the service. After disconnect of a worker, active callers receive SERVICE_UNAVAILABLE with no replay. Caller cancellation before dispatch removes queued work. Close/drop of an RPC request future must perform this cleanup even when the socket closes. Tokio blocking storage operations must not block the RPC scheduler.

STATS adds `rpc` object: queued, running, retainedBytes, maxCalls, maxBytes, accepted, completed, rejected, timedOut, routes (array). Each route: name, workers, concurrency (sum of configured worker capacities), running, queued, queueLimit. Counts are bounded/current or saturating telemetry. Existing STATS fields stay compatible. No advertised rate limit, circuit breaker, HTTP proxy, HA, adaptive/autoscaling scheduler, or cross-process cancellation guarantee until implemented and tested.

## SDK

`await client.request(route, payload, {timeoutMs?:number})` -> `{payload:Buffer,text():string,json<T>():T}`. `await client.requestJSON<T>(route, input, {timeoutMs?})` -> parsed response T. Default RPC timeout 5000ms. Capture inputs before awaiting.

`await client.handle(route, async (request) => Uint8Array|string, {concurrency?:number,queueLimit?:number})` -> ServiceHandle with `close():Promise<void>` and optional `onError` callback in options. request has payload, text(), json<T>(), signal:AbortSignal, remainingMs:number. Defaults concurrency=8, queueLimit=128. handler JSON result via `handleJSON(route, async (body, context) => responseObject, options)` convenience. Dedicated child client inherits host/port/token and uses enough bounded pending requests for TAKE+COMPLETE. SDK worker concurrency is 1..32 to fit the 32-operation connection cap; each slot strictly awaits COMPLETE before its next TAKE. Don't spawn handlers above the declared concurrency. Handler error is transmitted and releases capacity. Deadline signals abort cooperatively; await actual handler settlement before COMPLETE to avoid freeing a still-running slot. The SDK must continue completing even if work became late. ServiceHandle.close stops taking new work, closes its worker connection (failing active callers), and aborts active handler signals; it must not wait indefinitely for uncooperative handlers. Parent client.close closes its child handles as well. No swallowed internal promise rejections or unbounded rejected-loop retries.

The example application uses an HTTP API gateway -> VeloBus REQUEST -> replicated users.get backend workers -> response. Overload maps to HTTP 503, deadline to 504. Event tests retain their semantics and use generic service/state records.
