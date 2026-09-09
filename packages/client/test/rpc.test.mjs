import assert from 'node:assert/strict';
import { createServer } from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { connect, BrokerError, ProtocolError, ConnectionClosedError } from '../dist/esm/index.js';

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
function payloadBody(value) {
  const payload = Buffer.from(value);
  const prefix = Buffer.alloc(4);
  prefix.writeUInt32BE(payload.length);
  return Buffer.concat([prefix, payload]);
}
function jobBody(callId, value, remainingMs = 1000) {
  const prefix = Buffer.alloc(13);
  prefix[0] = 1;
  prefix.writeBigUInt64BE(callId, 1);
  prefix.writeUInt32BE(remainingMs, 9);
  return Buffer.concat([prefix, payloadBody(value)]);
}
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
async function until(predicate, ms = 1500) {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('Condition did not become true');
    await delay(2);
  }
}
async function peer(t, handler, onHello) {
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
        if (request.opcode === 1) {
          if (onHello) onHello(request);
          else socket.write(frame(0x81, request.id, Buffer.from([0, 1])));
        }
        else if (request.opcode === 5) socket.write(frame(0x85, request.id));
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

test('REQUEST snapshots inputs and decodes binary/JSON replies', async t => {
  const server = await peer(t, request => {
    assert.equal(request.opcode, 7);
    const routeLength = request.body.readUInt16BE(0);
    assert.equal(request.body.subarray(2, 2 + routeLength).toString(), 'users.get');
    assert.equal(request.body.readUInt32BE(2 + routeLength), 5000);
    const payload = request.body.subarray(10 + routeLength);
    request.socket.write(frame(0x87, request.id, payloadBody(payload)));
  });
  const client = await clientFor(t, server);
  const bytes = new Uint8Array([7]);
  const pending = client.request('users.get', bytes);
  bytes[0] = 9;
  assert.deepEqual((await pending).payload, Buffer.from([7]));
  const input = { id: 123 };
  const result = client.requestJSON('users.get', input);
  input.id = 456;
  assert.deepEqual(await result, { id: 123 });
  assert.equal((await client.request('users.get', 'hello')).text(), 'hello');
});

for (const code of [8, 9, 10, 11, 12]) {
  test(`RPC application error ${code} preserves healthy caller connection`, async t => {
    const server = await peer(t, request => request.socket.write(frame(0xff, request.id, errorBody(code, 'Service error'))));
    const client = await clientFor(t, server);
    await assert.rejects(client.request('users.get', '{}'), error => error instanceof BrokerError && error.code === code);
    await client.ping();
    assert.equal(server.requests.filter(request => request.opcode === 7).length, 1);
  });
}

test('RPC deadline allows server timeout response beyond the default transport timeout and allows concurrent ping', async t => {
  const server = await peer(t, request => {
    void delay(75).then(() => request.socket.write(frame(0xff, request.id, errorBody(10, 'Deadline exceeded'))));
  });
  const client = await clientFor(t, server, { timeoutMs: 35 });
  const pending = client.request('users.get', '{}', { timeoutMs: 60 });
  const rejected = assert.rejects(pending, error => error instanceof BrokerError && error.code === 10);
  await client.ping();
  await rejected;
  await client.ping();
});

test('malformed RPC payload response closes transport', async t => {
  const server = await peer(t, request => request.socket.write(frame(0x87, request.id, Buffer.from([0, 0, 0, 9, 65]))));
  const client = await clientFor(t, server);
  await assert.rejects(client.request('users.get', '{}'), ProtocolError);
  await assert.rejects(client.ping(), ProtocolError);
});

test('validates RPC inputs and SDK worker cap locally', async t => {
  const server = await peer(t, () => assert.fail('No RPC should be sent'));
  const client = await clientFor(t, server);
  await assert.rejects(client.request('bad/route', 'x'), TypeError);
  await assert.rejects(client.request('users.get', 'x', { timeoutMs: 30001 }), RangeError);
  await assert.rejects(client.request('users.get', Buffer.alloc(262145)), RangeError);
  await assert.rejects(client.requestJSON('users.get', undefined), TypeError);
  await assert.rejects(client.handle('users.get', () => '', { concurrency: 33 }), RangeError);
  await assert.rejects(client.handle('users.get', () => '', { queueLimit: 4097 }), RangeError);
  await assert.rejects(client.handleJSON('users.get', undefined), TypeError);
  assert.equal(server.requests.length, 1);
});

test('worker uses a dedicated authenticated connection and bounds real handler concurrency', async t => {
  const takes = [];
  const completions = [];
  const server = await peer(t, request => {
    if (request.opcode === 6) {
      const routeLength = request.body.readUInt16BE(0);
      assert.equal(request.body.subarray(2, 2 + routeLength).toString(), 'users.get');
      assert.equal(request.body.readUInt16BE(2 + routeLength), 2);
      assert.equal(request.body.readUInt16BE(4 + routeLength), 7);
      request.socket.write(frame(0x86, request.id));
    } else if (request.opcode === 8) takes.push(request);
    else if (request.opcode === 9) {
      completions.push(request);
      request.socket.write(frame(0x89, request.id));
    }
  });
  const client = await clientFor(t, server, { token: 'inherited-token', maxPendingRequests: 1 });
  const release = deferred();
  let active = 0, peak = 0;
  const service = await client.handleJSON('users.get', async (body, context) => {
    active++; peak = Math.max(peak, active);
    assert.ok(context.signal instanceof AbortSignal);
    assert.equal(context.remainingMs, 1000);
    await release.promise;
    active--;
    return { id: body.id };
  }, { concurrency: 2, queueLimit: 7 });
  await until(() => takes.length === 2);
  takes[0].socket.write(frame(0x88, takes[0].id, jobBody(1n, '{"id":1}')));
  takes[1].socket.write(frame(0x88, takes[1].id, jobBody(2n, '{"id":2}')));
  await until(() => active === 2);
  await delay(15);
  assert.equal(takes.length, 2, 'No extra TAKE before handlers settle and COMPLETE is acknowledged');
  await client.ping();
  release.resolve();
  await until(() => completions.length === 2 && takes.length === 4);
  assert.equal(peak, 2);
  assert.deepEqual(completions.map(request => JSON.parse(request.body.subarray(13).toString())).sort((a, b) => a.id - b.id), [{ id: 1 }, { id: 2 }]);
  assert.ok(completions.every(request => request.body[8] === 0));
  const hellos = server.requests.filter(request => request.opcode === 1);
  assert.equal(hellos.length, 2);
  assert.ok(hellos.every(request => request.body.subarray(4).toString() === 'inherited-token'));
  assert.notEqual(hellos[0].socket, takes[0].socket);
  await service.close();
  await client.ping();
});

test('deadline aborts cooperatively and does not COMPLETE or reuse a slot before actual settlement', async t => {
  const takes = [];
  const completions = [];
  const server = await peer(t, request => {
    if (request.opcode === 6) request.socket.write(frame(0x86, request.id));
    else if (request.opcode === 8) takes.push(request);
    else if (request.opcode === 9) { completions.push(request); request.socket.write(frame(0x89, request.id)); }
  });
  const client = await clientFor(t, server);
  const release = deferred();
  let context;
  const service = await client.handle('users.get', async request => {
    context = request;
    await release.promise;
    return 'late result';
  }, { concurrency: 1 });
  await until(() => takes.length === 1);
  takes[0].socket.write(frame(0x88, takes[0].id, jobBody(42n, 'input', 20)));
  await until(() => context?.signal.aborted);
  assert.equal(context.signal.reason.code, 10);
  assert.equal(completions.length, 0);
  assert.equal(takes.length, 1);
  release.resolve();
  await until(() => completions.length === 1 && takes.length === 2);
  assert.equal(completions[0].body.readBigUInt64BE(0), 42n);
  assert.equal(completions[0].body.subarray(13).toString(), 'late result');
  await service.close();
});

test('handler errors send bounded valid UTF-8 and release the worker loop', async t => {
  const takes = [];
  let completion;
  const server = await peer(t, request => {
    if (request.opcode === 6) request.socket.write(frame(0x86, request.id));
    else if (request.opcode === 8) takes.push(request);
    else if (request.opcode === 9) { completion = request; request.socket.write(frame(0x89, request.id)); }
  });
  const client = await clientFor(t, server);
  const service = await client.handle('users.get', () => { throw new Error('ç'.repeat(5000)); }, { concurrency: 1 });
  await until(() => takes.length === 1);
  takes[0].socket.write(frame(0x88, takes[0].id, jobBody(1n, 'input')));
  await until(() => completion && takes.length === 2);
  assert.equal(completion.body[8], 1);
  const payload = completion.body.subarray(13);
  assert.ok(payload.length <= 4096);
  assert.equal(new TextDecoder('utf-8', { fatal: true }).decode(payload), 'ç'.repeat(2048));
  await service.close();
});

test('invalid JSON handler output is transmitted as handler error without losing service', async t => {
  const takes = [];
  let completion;
  const server = await peer(t, request => {
    if (request.opcode === 6) request.socket.write(frame(0x86, request.id));
    else if (request.opcode === 8) takes.push(request);
    else if (request.opcode === 9) { completion = request; request.socket.write(frame(0x89, request.id)); }
  });
  const client = await clientFor(t, server);
  const service = await client.handleJSON('users.get', () => undefined, { concurrency: 1 });
  await until(() => takes.length === 1);
  takes[0].socket.write(frame(0x88, takes[0].id, jobBody(1n, '{}')));
  await until(() => completion && takes.length === 2);
  assert.equal(completion.body[8], 1);
  assert.match(completion.body.subarray(13).toString(), /cannot be represented as JSON/);
  await service.close();
});

test('parent close aborts all handlers, closes child sockets and does not await uncooperative work', async t => {
  const takes = [];
  const server = await peer(t, request => {
    if (request.opcode === 6) request.socket.write(frame(0x86, request.id));
    else if (request.opcode === 8) takes.push(request);
    else assert.fail('Unsettled handler must not COMPLETE');
  });
  const client = await clientFor(t, server);
  let context;
  const service = await client.handle('users.get', request => { context = request; return new Promise(() => {}); }, { concurrency: 1 });
  await until(() => takes.length === 1);
  takes[0].socket.write(frame(0x88, takes[0].id, jobBody(1n, '{}')));
  await until(() => context);
  const start = Date.now();
  await client.close();
  assert.ok(Date.now() - start < 500);
  assert.ok(context.signal.aborted);
  await service.close();
  await until(() => server.sockets.size === 0);
});

test('worker disconnect aborts running handlers, reports once and never retries', async t => {
  const takes = [];
  const errors = [];
  const server = await peer(t, request => {
    if (request.opcode === 6) request.socket.write(frame(0x86, request.id));
    else if (request.opcode === 8) takes.push(request);
  });
  const client = await clientFor(t, server);
  let context;
  const service = await client.handle('users.get', request => { context = request; return new Promise(() => {}); }, {
    concurrency: 1, onError: error => errors.push(error),
  });
  await until(() => takes.length === 1);
  takes[0].socket.write(frame(0x88, takes[0].id, jobBody(1n, '{}')));
  await until(() => context);
  takes[0].socket.destroy();
  await until(() => errors.length === 1);
  assert.ok(errors[0] instanceof ConnectionClosedError);
  assert.ok(context.signal.aborted);
  await delay(20);
  assert.equal(server.requests.filter(request => request.opcode === 6).length, 1);
  assert.equal(errors.length, 1);
  await client.ping();
  await service.close();
});

test('failed REGISTER cleans child connection and leaves parent usable', async t => {
  const server = await peer(t, request => request.socket.write(frame(0xff, request.id, errorBody(3, 'Queue mismatch'))));
  const client = await clientFor(t, server);
  await assert.rejects(client.handle('users.get', () => ''), error => error instanceof BrokerError && error.code === 3);
  await until(() => server.sockets.size === 1);
  await client.ping();
});

test('malformed TAKE terminates worker and reports a protocol error without retrying', async t => {
  const errors = [];
  const server = await peer(t, request => {
    if (request.opcode === 6) request.socket.write(frame(0x86, request.id));
    else request.socket.write(frame(0x88, request.id, Buffer.from([2])));
  });
  const client = await clientFor(t, server);
  const service = await client.handle('users.get', () => assert.fail('Malformed work must not execute'), {
    concurrency: 1, onError: error => errors.push(error),
  });
  await until(() => errors.length === 1);
  assert.ok(errors[0] instanceof ProtocolError);
  assert.equal(server.requests.filter(request => request.opcode === 8).length, 1);
  await client.ping();
  await service.close();
});

test('parent close cancels a child still negotiating HELLO without waiting for transport timeout', async t => {
  let hellos = 0;
  const server = await peer(t, () => assert.fail('REGISTER should not be sent'), request => {
    if (++hellos === 1) request.socket.write(frame(0x81, request.id, Buffer.from([0, 1])));
  });
  const client = await clientFor(t, server, { timeoutMs: 2000 });
  const starting = client.handle('users.get', () => 'ok');
  const rejected = assert.rejects(starting, ConnectionClosedError);
  await until(() => hellos === 2);
  const start = Date.now();
  await client.close();
  await rejected;
  assert.ok(Date.now() - start < 500);
  await until(() => server.sockets.size === 0);
});
