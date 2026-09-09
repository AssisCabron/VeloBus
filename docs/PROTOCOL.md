# VeloBus binary protocol v0.1

This document describes the compatible event layer. The v0.2 backend request/reply extension and overload controls are specified in [RPC-PROTOCOL.md](RPC-PROTOCOL.md).

Implementation contract for the first experimental single-node release. All integers are unsigned big-endian. TCP, persistent connections, one response per request; request IDs correlate replies. No native Node addon is necessary. Name is provisional; no npm namespace or trademark is claimed.

## Envelope

`length:u32 | opcode:u8 | requestId:u32 | body:bytes`

Length counts opcode + requestId + body, not the 4-byte prefix. Valid length: 5..1,048,576. Request IDs are nonzero. A response uses the request opcode OR 0x80; errors use 0xff. Unknown opcodes, invalid UTF-8, trailing bytes, invalid lengths, and invalid enum values must be rejected. Disconnect on malformed envelope. Read payloads with a deadline after accepting the length. No compression or TLS in this version; default loopback binding, trusted network/tunnel for remote use.

`str = length:u16 | UTF-8 bytes`. Topics: 1..255 ASCII bytes, only letters, digits, `.`, `_`, `-`. Fetch also accepts `*` for all topics. Keys: valid UTF-8, 0..1024 bytes. Empty key means never coalesce. Payload: arbitrary bytes, at most 262,144 bytes. Counts 1..256 for publish and fetch limit. Max fetch payload budget: 1..524,288 bytes; this budget counts each returned record's complete wire encoding. Store may scan at most 4096 records per fetch.

## 0x01 HELLO

Must be the first request. Body: `version:u16 (=1) | token:str`. Success: `version:u16 (=1)`. Token is configured through VELOBUS_TOKEN; optional on loopback, mandatory for a non-loopback listener. Token must never be logged. A connection authenticates once; no further HELLO. Wrong credentials receive error then disconnect. SDK authenticates during connect.

## 0x02 PUBLISH

Body: `count:u16 | record[count]` where record = `topic:str | key:str | payloadLength:u32 | payload`.

Success: `firstSequence:u64 | lastSequence:u64 | count:u16 | durable:u8 (0 memory, 1 disk synchronized)`.

Batch is validated and capacity checked before any write. Consecutive globally assigned sequence numbers, starting at 1. Memory mode loses all history and resets sequences on restart. Disk mode logs the whole batch with checksum and synchronizes before ACK. A failed or disconnected request can have an unknown outcome; no transparent publication retry, no exactly-once claim. Empty batches are invalid. Total request must fit the envelope limit.

## 0x03 FETCH

Body: `topic:str | after:u64 | limit:u16 | maxBytes:u32 | mode:u8 (0 all, 1 latest)`.

Success: `cursor:u64 | count:u16 | hasMore:u8 | event[count]`; event = `sequence:u64 | topic:str | key:str | payloadLength:u32 | payload`.

Returns records strictly after `after`, in ascending sequence order. Cursor is the last scanned sequence (including nonmatching records), or `after` if none scanned. Cursor must never advance over an eligible event that was neither returned nor explicitly superseded in latest mode. `after` beyond the current high-water mark is an error, not silent waiting. `hasMore` indicates unscanned history, even if the batch is empty for this topic. Empty history accepts after=0.

`all` preserves every matching record. `latest` replaces pending matching records with the same (topic,key) inside this bounded scan/batch, retaining the newest encountered sequence. This is not guaranteed to find the globally newest value in an arbitrarily large backlog. Empty keys never merge. Retained history is unchanged. If adding/replacing a record would exceed limit or maxBytes, stop before that record; if the first eligible record alone exceeds maxBytes, return LIMIT instead of skipping it. Returned events are sorted by sequence.

Fetch does not delete records or store acknowledgements. Consumers own their checkpoints. To obtain at-least-once processing, persist `cursor` only after the whole batch succeeds; replay after failure may duplicate already processed events. SDK subscribe is a pull-based async iterator using bounded batches and idle polling; expose checkpointing explicitly and do not claim durable server-side consumer groups.

## 0x04 STATS

Empty request. Success body is UTF-8 JSON (control plane only). Fields: version (string), mode (`memory` or `disk`), records, retainedBytes, maxRetainedBytes, walBytes, maxWalBytes, lastSequence (decimal string), publishedRecords, fetchedRecords, coalescedRecords, connections. Other counters and budgets are JSON numbers. Counters since process start; records and lastSequence include replayed history. retainedBytes is conservative store accounting, not an RSS limit. Include uptimeSeconds (number) if practical.

## 0x05 PING

Empty request and success body.

## Errors

Body: `code:u16 | message:str`.

1 PROTOCOL, 2 UNAUTHORIZED, 3 INVALID, 4 CAPACITY, 5 LIMIT, 6 STORAGE, 7 CURSOR. Storage errors may leave publication outcome unknown; server must stop accepting new writes after uncertain disk I/O. No automatic retry. SDK error type exposes numeric code and message.

## Resource and storage contract

Default loopback port 7447. Default disk directory `./data/velobus`; `--memory` selects explicit volatile mode. Default retained budget 64 MiB, WAL budget 128 MiB, connections 64. CLI: `--listen`, `--data-dir`, `--memory`, `--max-retained-bytes`, `--max-wal-bytes`, `--max-connections`. Refuse conflicting memory/data-dir flags. Refuse nonpositive/out-of-range budgets. Refuse concurrent processes opening the same WAL via OS file lock. Reject before capacity exhaustion; no silent eviction. A full log currently requires planned export/rotation outside the running process; automatic retention/compaction is future work.

WAL recovery replays bounded, validated batches, truncates only incomplete trailing records, and refuses checksum corruption or invalid record lengths. Writer failures poison the store until restart. Persist first-created file and directory metadata. Do not ACK disk mode until data is synchronized. Checkpoints belong to a particular topic/mode and the same log lifetime: deleting/replacing the WAL or restarting memory mode invalidates them. There is no generation token in v0.1, so applications must not reuse checkpoints across those actions. No replication, high availability, browser SDK, wildcard hierarchy, transactions with application databases, or production-readiness claim in v0.1.

Startup emits a JSON line on stdout: `{"event":"ready","address":"127.0.0.1:7447","mode":"disk"}` with the actual bound address. Port 0 is permitted for local tests. Diagnostics go to stderr.

## SDK public API

Package `@velobus/client`, provisional name, Node >=20, ESM and CJS with declarations, zero runtime dependencies. `connect({host?,port?,token?,timeoutMs?,maxPendingRequests?})` returns a Client. API: `publish(topic, Uint8Array|string, {key?}?)`, `publishJSON(topic, unknown, {key?}?)`, `publishBatch([{topic,payload,key?}])`, `fetch(topic,{after?:bigint,limit?,maxBytes?,mode?:'all'|'latest'})`, `subscribe(topic,{after?,limit?,maxBytes?,mode?,pollIntervalMs?,signal?})` async iterator of events, `stats()`, `ping()`, `close()`.

Publication receipt: `{firstSequence:bigint,lastSequence:bigint,count:number,durable:boolean}`. Event: `{sequence:bigint,topic:string,key:string,payload:Buffer,json<T>():T,text():string}`. Fetch: `{cursor:bigint,events:Event[],hasMore:boolean}`. SDK rejects on socket close/timeouts and uses socket backpressure with bounded pending requests. Timed out requests must close the connection so outcomes aren't silently confused. Subscribe buffers at most one fetch batch; it must not busy-spin on empty batches. No reconnect/retry that can duplicate publications invisibly. `subscribe` is convenience, explicit `fetch` is recommended for durable application checkpointing.
