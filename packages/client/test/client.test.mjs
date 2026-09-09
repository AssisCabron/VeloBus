import assert from 'node:assert/strict';
import { createServer } from 'node:net';
import { setTimeout as delay, setImmediate as immediate } from 'node:timers/promises';
import { createRequire } from 'node:module';
import test from 'node:test';
import {
  connect, BrokerError, BackpressureError, ConnectionClosedError, ProtocolError, RequestTimeoutError,
} from '../dist/esm/index.js';
import { FrameDecoder } from '../dist/esm/protocol.js';

function frame(opcode, id, body = Buffer.alloc(0)) {
  const bytes = Buffer.alloc(9 + body.length);
  bytes.writeUInt32BE(5 + body.length, 0);
  bytes.writeUInt8(opcode, 4);
  bytes.writeUInt32BE(id, 5);
  body.copy(bytes, 9);
  return bytes;
}
function str(value) {
  const bytes = Buffer.from(value);
  const prefix = Buffer.alloc(2);
  prefix.writeUInt16BE(bytes.length);
  return Buffer.concat([prefix, bytes]);
}
function errorBody(code, message) {
  const prefix = Buffer.alloc(2);
  prefix.writeUInt16BE(code);
  return Buffer.concat([prefix, str(message)]);
}
function receipt(first, count = 1, durable = false) {
  const body = Buffer.alloc(19);
  body.writeBigUInt64BE(first, 0);
  body.writeBigUInt64BE(first + BigInt(count - 1), 8);
  body.writeUInt16BE(count, 16);
  body.writeUInt8(durable ? 1 : 0, 18);
  return body;
}
function eventsBody(cursor, entries = [], hasMore = false) {
  const head = Buffer.alloc(11);
  head.writeBigUInt64BE(cursor, 0);
  head.writeUInt16BE(entries.length, 8);
  head.writeUInt8(hasMore ? 1 : 0, 10);
  const entriesBytes = entries.map(({ sequence, topic = 'services', key = '', payload = 'hello' }) => {
    const sequenceBytes = Buffer.alloc(8);
    sequenceBytes.writeBigUInt64BE(sequence);
    const payloadBytes = Buffer.from(payload);
    const payloadLength = Buffer.alloc(4);
    payloadLength.writeUInt32BE(payloadBytes.length);
    return Buffer.concat([sequenceBytes, str(topic), str(key), payloadLength, payloadBytes]);
  });
  return Buffer.concat([head, ...entriesBytes]);
}
async function peer(t, handler, { authenticate = true } = {}) {
  const sockets = new Set();
  const requests = [];
  const server = createServer(socket => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('close', () => sockets.delete(socket));
    socket.setNoDelay(true);
    let buffer = Buffer.alloc(0);
    socket.on('data', chunk => {
      buffer = Buffer.concat([buffer, chunk]);
      while (buffer.length >= 4 && buffer.length >= buffer.readUInt32BE(0) + 4) {
        const size = buffer.readUInt32BE(0) + 4;
        const packet = buffer.subarray(0, size);
        buffer = buffer.subarray(size);
        const request = { opcode: packet[4], id: packet.readUInt32BE(5), body: packet.subarray(9), socket };
        requests.push(request);
        if (request.opcode === 1 && authenticate) socket.write(frame(0x81, request.id, Buffer.from([0, 1])));
        else handler(request);
      }
    });
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => server.close(resolve));
  });
  return { port: server.address().port, requests, sockets };
}
async function clientFor(t, server, options = {}) {
  const client = await connect({ port: server.port, timeoutMs: 2000, ...options });
  t.after(() => client.close());
  return client;
}
async function until(predicate, ms = 1000) {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('Condition did not become true');
    await delay(2);
  }
}

test('ESM and CommonJS entrypoints are usable', () => {
  const require = createRequire(import.meta.url);
  assert.equal(typeof require('../dist/cjs/index.js').connect, 'function');
  assert.equal(typeof connect, 'function');
});

test('incremental decoder handles every-byte fragmentation and concatenated frames', () => {
  const expected = [frame(0x81, 1, Buffer.from([0, 1])), frame(0x85, 2)];
  const actual = [];
  const decoder = new FrameDecoder(packet => actual.push(packet));
  for (const byte of Buffer.concat(expected)) decoder.push(Buffer.from([byte]));
  assert.deepEqual(actual, expected.map(packet => packet.subarray(4)));
  const together = [];
  new FrameDecoder(packet => together.push(packet)).push(Buffer.concat(expected));
  assert.deepEqual(together, actual);
});

test('authenticates, encodes binary publish, and decodes fragmented bigint fetches', async t => {
  const sequence = 9_007_199_254_740_993n;
  const server = await peer(t, request => {
    if (request.opcode === 1) {
      assert.equal(request.body.readUInt16BE(0), 1);
      assert.equal(request.body.readUInt16BE(2), 6);
      assert.equal(request.body.subarray(4).toString(), 'secret');
      request.socket.write(frame(0x81, request.id, Buffer.from([0, 1])));
    } else if (request.opcode === 2) {
      assert.equal(request.body.readUInt16BE(0), 1);
      assert.equal(request.body.readUInt16BE(2), 8);
      assert.equal(request.body.subarray(4, 12).toString(), 'services');
      request.socket.write(frame(0x82, request.id, receipt(sequence, 1, true)));
    } else if (request.opcode === 3) {
      assert.equal(request.body.at(-1), 1);
      const packet = frame(0x83, request.id, eventsBody(sequence, [{ sequence, key: 'id-1', payload: '{"x":1}' }]));
      void (async () => {
        for (const byte of packet) { request.socket.write(Buffer.from([byte])); await immediate(); }
      })();
    }
  }, { authenticate: false });
  const client = await clientFor(t, server, { token: 'secret' });
  const ack = await client.publishJSON('services', { x: 1 }, { key: 'id-1' });
  assert.deepEqual(ack, { firstSequence: sequence, lastSequence: sequence, count: 1, durable: true });
  const batch = await client.fetch('services', { mode: 'latest' });
  assert.equal(batch.cursor, sequence);
  assert.equal(batch.events[0].sequence, sequence);
  assert.deepEqual(batch.events[0].json(), { x: 1 });
  assert.equal(batch.events[0].text(), '{"x":1}');
  assert.equal(batch.events[0].key, 'id-1');
});

test('matches out-of-order responses and wraps request IDs without zero or collisions', async t => {
  const received = [];
  const server = await peer(t, request => {
    received.push(request);
    if (received.length === 3) {
      for (const current of [...received].reverse()) {
        current.socket.write(frame(0x82, current.id, receipt(BigInt(current.id))));
      }
    }
  });
  const client = await clientFor(t, server);
  // Exercise the otherwise billion-request rollover boundary.
  client.nextRequestId = 0xffff_ffff;
  const first = client.publish('a', 'a');
  client.nextRequestId = 0xffff_ffff;
  const second = client.publish('a', 'b');
  const third = client.publish('a', 'c');
  const results = await Promise.all([first, second, third]);
  assert.deepEqual(results.map(item => item.firstSequence), [0xffff_ffffn, 1n, 2n]);
  assert.deepEqual(received.map(request => request.id), [0xffff_ffff, 1, 2]);
});

test('wrong credentials reject connect with structured broker error and close', async t => {
  const server = await peer(t, request => {
    request.socket.end(frame(0xff, request.id, errorBody(2, 'Unauthorized')));
  }, { authenticate: false });
  await assert.rejects(connect({ port: server.port, token: 'wrong' }), error => error instanceof BrokerError && error.code === 2 && error.message === 'Unauthorized');
  assert.equal(server.requests.length, 1);
});

test('broker application errors preserve a healthy shared connection', async t => {
  const server = await peer(t, request => {
    request.socket.write(request.opcode === 2
      ? frame(0xff, request.id, errorBody(4, 'Capacity reached'))
      : frame(0x85, request.id));
  });
  const client = await clientFor(t, server);
  await assert.rejects(client.publish('a', 'x'), error => error instanceof BrokerError && error.code === 4);
  await client.ping();
});

for (const [name, respond] of [
  ['short envelope', request => Buffer.from([0, 0, 0, 4])],
  ['oversized envelope', request => Buffer.from([0, 16, 0, 1])],
  ['unknown request ID', request => frame(0x85, request.id + 100)],
  ['zero request ID', request => frame(0x85, 0)],
  ['wrong opcode', request => frame(0x84, request.id)],
  ['trailing body bytes', request => frame(0x85, request.id, Buffer.from([0]))],
  ['malformed error string length', request => frame(0xff, request.id, Buffer.from([0, 1, 0, 9, 65]))],
  ['invalid UTF-8', request => frame(0xff, request.id, Buffer.from([0, 1, 0, 1, 0xff]))],
]) {
  test(`${name} closes the connection and rejects every pending request`, async t => {
    let sent = false;
    const server = await peer(t, request => {
      if (!sent) { sent = true; request.socket.write(respond(request)); }
    });
    const client = await clientFor(t, server);
    const results = await Promise.allSettled([client.ping(), client.ping()]);
    assert.ok(results.every(result => result.status === 'rejected' && result.reason instanceof ProtocolError));
    await assert.rejects(client.ping(), ProtocolError);
  });
}

test('malformed fetch payload length is rejected without exposing partial events', async t => {
  const server = await peer(t, request => {
    const body = eventsBody(1n, [{ sequence: 1n, topic: 'a', payload: 'x' }]);
    body.writeUInt32BE(262145, body.length - 5);
    request.socket.write(frame(0x83, request.id, body));
  });
  const client = await clientFor(t, server);
  await assert.rejects(client.fetch('a'), ProtocolError);
});

test('validates fetch monotonic cursor and event ordering', async t => {
  const server = await peer(t, request => {
    request.socket.write(frame(0x83, request.id, eventsBody(2n, [{ sequence: 2n }, { sequence: 1n }])));
  });
  const client = await clientFor(t, server);
  await assert.rejects(client.fetch('services'), ProtocolError);
});

test('disconnect rejects all pending requests without replay', async t => {
  const server = await peer(t, request => request.socket.destroy());
  const client = await clientFor(t, server);
  const results = await Promise.allSettled([client.publish('a', 'x'), client.ping()]);
  assert.ok(results.every(result => result.status === 'rejected' && result.reason instanceof ConnectionClosedError));
  assert.equal(server.requests.filter(request => request.opcode === 2).length, 1);
});

test('request timeout closes socket and rejects all pending work', async t => {
  const server = await peer(t, () => {});
  const client = await clientFor(t, server, { timeoutMs: 60 });
  const results = await Promise.allSettled([client.publish('a', 'x'), client.ping()]);
  assert.ok(results.every(result => result.status === 'rejected' && result.reason instanceof RequestTimeoutError));
  await assert.rejects(client.ping(), RequestTimeoutError);
  await until(() => server.sockets.size === 0);
  assert.equal(server.requests.filter(request => request.opcode === 2).length, 1);
});

test('partial response cannot evade request timeout', async t => {
  const server = await peer(t, request => request.socket.write(Buffer.from([0, 0, 0, 5, 0x85])));
  const client = await clientFor(t, server, { timeoutMs: 60 });
  await assert.rejects(client.ping(), RequestTimeoutError);
});

test('maximum pending requests rejects overflow then recovers capacity', async t => {
  const waiting = [];
  const server = await peer(t, request => waiting.push(request));
  const client = await clientFor(t, server, { maxPendingRequests: 2 });
  const one = client.ping();
  const two = client.ping();
  await assert.rejects(client.ping(), BackpressureError);
  await until(() => waiting.length === 2);
  for (const request of waiting) request.socket.write(frame(0x85, request.id));
  await Promise.all([one, two]);
  const three = client.ping();
  await until(() => waiting.length === 3);
  waiting[2].socket.write(frame(0x85, waiting[2].id));
  await three;
});

test('TCP backpressure keeps queued publication count bounded and resumes after drain', async t => {
  let sequence = 0n;
  const server = await peer(t, request => {
    request.socket.write(frame(0x82, request.id, receipt(++sequence)));
  });
  const client = await clientFor(t, server, { maxPendingRequests: 64, timeoutMs: 5000 });
  const socket = [...server.sockets][0];
  socket.pause();
  const payload = Buffer.alloc(262144, 42);
  const pending = Array.from({ length: 64 }, () => client.publish('bytes', payload));
  const completed = Promise.all(pending);
  await assert.rejects(client.publish('bytes', payload), BackpressureError);
  assert.ok(client.writeQueue.length > 0, 'paused TCP receiver should leave frames waiting for drain');
  assert.ok(client.socket.writableLength <= 1_048_580, 'socket queue should contain at most one maximum frame');
  socket.resume();
  const results = await completed;
  assert.equal(results.length, 64);
  assert.equal(results.at(-1).lastSequence, 64n);
});

test('subscribe buffers only one batch, advances using cursor, and aborts gracefully', async t => {
  let fetches = 0;
  const afters = [];
  const server = await peer(t, request => {
    if (request.opcode === 5) { request.socket.write(frame(0x85, request.id)); return; }
    fetches++;
    afters.push(request.body.readBigUInt64BE(2 + request.body.readUInt16BE(0)));
    request.socket.write(frame(0x83, request.id, fetches === 1
      ? eventsBody(3n, [{ sequence: 1n }, { sequence: 2n }], true)
      : eventsBody(3n)));
  });
  const client = await clientFor(t, server);
  const abort = new AbortController();
  const stream = client.subscribe('services', { limit: 2, pollIntervalMs: 5000, signal: abort.signal });
  assert.equal((await stream.next()).value.sequence, 1n);
  await delay(20);
  assert.equal(fetches, 1);
  assert.equal((await stream.next()).value.sequence, 2n);
  assert.equal(fetches, 1);
  const idle = stream.next();
  await until(() => fetches === 2);
  abort.abort();
  assert.deepEqual(await idle, { done: true, value: undefined });
  assert.deepEqual(afters, [0n, 3n]);
  await client.ping();
});

test('already-aborted subscribe does not send FETCH', async t => {
  const server = await peer(t, () => assert.fail('No command expected'));
  const client = await clientFor(t, server);
  const stream = client.subscribe('a', { signal: AbortSignal.abort() });
  assert.deepEqual(await stream.next(), { done: true, value: undefined });
  assert.equal(server.requests.length, 1);
});

test('empty sparse-topic batches with hasMore still obey idle polling interval', async t => {
  let count = 0;
  const server = await peer(t, request => request.socket.write(frame(0x83, request.id, eventsBody(BigInt(++count), [], true))));
  const client = await clientFor(t, server);
  const abort = new AbortController();
  const next = client.subscribe('a', { signal: abort.signal, pollIntervalMs: 100 }).next();
  await until(() => count === 1);
  await delay(35);
  assert.equal(count, 1);
  abort.abort();
  assert.deepEqual(await next, { done: true, value: undefined });
});

test('aborting an in-flight subscribe does not abandon protocol response or close client', async t => {
  let fetchRequest;
  const server = await peer(t, request => {
    if (request.opcode === 3) fetchRequest = request;
    else request.socket.write(frame(0x85, request.id));
  });
  const client = await clientFor(t, server);
  const abort = new AbortController();
  const next = client.subscribe('a', { signal: abort.signal }).next();
  await until(() => fetchRequest);
  abort.abort();
  assert.deepEqual(await next, { done: true, value: undefined });
  fetchRequest.socket.write(frame(0x83, fetchRequest.id, eventsBody(0n)));
  await client.ping();
});

test('validates input locally before sending and copies mutable payloads before queueing', async t => {
  const server = await peer(t, request => {
    assert.equal(request.body.at(-1), 7);
    request.socket.write(frame(0x82, request.id, receipt(1n)));
  });
  const client = await clientFor(t, server);
  await assert.rejects(client.publish('bad/topic', 'x'), TypeError);
  await assert.rejects(client.publish('a', 'x', { key: '\ud800' }), RangeError);
  await assert.rejects(client.publish('a', Buffer.alloc(262145)), RangeError);
  await assert.rejects(client.publishBatch([]), RangeError);
  await assert.rejects(client.publishJSON('a', undefined), TypeError);
  await assert.rejects(client.fetch('a', { after: -1n }), RangeError);
  await assert.rejects(client.fetch('a', { limit: 257 }), RangeError);
  await assert.rejects(client.fetch('a', { mode: 'invalid' }), TypeError);
  assert.equal(server.requests.length, 1);
  const payload = new Uint8Array([7]);
  const published = client.publish('a', payload);
  payload[0] = 9;
  await published;
});

test('stats preserves full-precision sequence string and validates control-plane types', async t => {
  const stats = {
    version: '0.1.0', mode: 'memory', records: 1, retainedBytes: 1, maxRetainedBytes: 64,
    walBytes: 0, maxWalBytes: 128, lastSequence: '9007199254740993', publishedRecords: 1,
    fetchedRecords: 0, coalescedRecords: 0, connections: 1, uptimeSeconds: 0.1,
  };
  const server = await peer(t, request => request.socket.write(frame(0x84, request.id, Buffer.from(JSON.stringify(stats)))));
  const client = await clientFor(t, server);
  assert.deepEqual(await client.stats(), stats);
  stats.lastSequence = 9007199254740993;
  await assert.rejects(client.stats(), ProtocolError);
});

test('preserves initial U+FEFF in opaque UTF-8 keys', async t => {
  const server = await peer(t, request => {
    request.socket.write(frame(0x83, request.id, eventsBody(1n, [{ sequence: 1n, key: '\ufeffidentity' }])));
  });
  const client = await clientFor(t, server);
  const batch = await client.fetch('services');
  assert.equal(batch.events[0].key, '\ufeffidentity');
});

test('caller mutation of batch array cannot invalidate the captured publication receipt', async t => {
  const server = await peer(t, request => request.socket.write(frame(0x82, request.id, receipt(1n))));
  const client = await clientFor(t, server);
  const records = [{ topic: 'a', payload: 'x' }];
  const published = client.publishBatch(records);
  records.push({ topic: 'b', payload: 'y' });
  assert.equal((await published).count, 1);
});

test('explicit close rejects pending work and is idempotent', async t => {
  const server = await peer(t, () => {});
  const client = await clientFor(t, server);
  const rejected = assert.rejects(client.ping(), ConnectionClosedError);
  await client.close();
  await rejected;
  await client.close();
});
