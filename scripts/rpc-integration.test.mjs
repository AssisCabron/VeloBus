import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { connect } from '../packages/client/dist/esm/index.js';
import { startBroker } from './harness.mjs';

function deferred() {
  let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve };
}
async function eventually(check, message = 'condition', milliseconds = 1500) {
  const until = Date.now() + milliseconds;
  while (Date.now() < until) { if (await check()) return; await delay(5); }
  assert.fail(`Timed out waiting for ${message}`);
}
async function fixture(t, options = {}) {
  const server = await startBroker(options); const clients = []; const handles = [];
  t.after(async () => {
    await Promise.allSettled(handles.map(handle => handle.close()));
    await Promise.allSettled(clients.map(client => client.close()));
    await server.stop();
  });
  async function client() {
    const bus = await connect({ port: server.port, token: options.token }); clients.push(bus); return bus;
  }
  const bus = await client();
  return { server, bus, client, async handle(route, fn, settings = {}) {
    const handle = await bus.handleJSON(route, fn, settings); handles.push(handle); return handle;
  } };
}

test('RPC: backend JSON responde ao chamador sem gravar chamada no log de eventos', async t => {
  const { bus, handle } = await fixture(t);
  await handle('users.get', async body => ({ id: body.id, name: 'Ada' }));
  assert.deepEqual(await bus.requestJSON('users.get', { id: 42 }), { id: 42, name: 'Ada' });
  const stats = await bus.stats();
  assert.equal(stats.records, 0); assert.equal(stats.rpc.accepted, 1); assert.equal(stats.rpc.completed, 1);
  assert.equal(stats.rpc.queued, 0); assert.equal(stats.rpc.running, 0); assert.equal(stats.rpc.retainedBytes, 0);
});

test('RPC: payload binário preservado, serviço ausente e erro do handler mantêm conexão saudável', async t => {
  const { bus } = await fixture(t);
  await assert.rejects(bus.request('absent', 'x'), e => e.code === 8);
  const binary = Buffer.from([0, 255, 128, 3]);
  const echo = await bus.handle('binary.echo', async request => request.payload);
  t.after(() => echo.close());
  assert.deepEqual((await bus.request('binary.echo', binary)).payload, binary);
  const fail = await bus.handle('operation.fail', async () => { throw new Error('business failure'); });
  t.after(() => fail.close());
  await assert.rejects(bus.request('operation.fail', 'x'), e => e.code === 12);
  await bus.ping();
});

test('RPC: requisição lenta não bloqueia chamada rápida na mesma conexão', async t => {
  const { bus, handle } = await fixture(t); const gate = deferred(); let started = false;
  t.after(() => gate.resolve());
  await handle('operation.slow', async () => { started = true; await gate.promise; return 'slow'; });
  await handle('operation.fast', async () => 'fast');
  const slow = bus.requestJSON('operation.slow', {}, { timeoutMs: 2000 });
  const guardedSlow = slow.catch(error => error);
  await eventually(() => started, 'slow handler to start');
  try {
    const fast = await bus.requestJSON('operation.fast', {}, { timeoutMs: 500 });
    assert.equal(fast, 'fast'); await bus.ping();
  } finally { gate.resolve(); }
  assert.equal(await guardedSlow, 'slow');
});

test('RPC: bombardeio respeita concorrência e fila, excedente recebe OVERLOADED', async t => {
  const { bus, handle } = await fixture(t); const gate = deferred();
  let active = 0; let peak = 0; let started = 0;
  t.after(() => gate.resolve());
  await handle('orders.create', async body => {
    active++; started++; peak = Math.max(peak, active);
    try { await gate.promise; return { id: body.id }; } finally { active--; }
  }, { concurrency: 2, queueLimit: 3 });
  const work = Array.from({ length: 20 }, (_, id) => bus.requestJSON('orders.create', { id }, { timeoutMs: 2000 })
    .then(value => ({ ok: true, value }), error => ({ ok: false, code: error.code })));
  await eventually(() => started >= 2, 'two active workers');
  await eventually(async () => (await bus.stats()).rpc.rejected >= 15, 'overload rejections');
  const stats = await bus.stats();
  assert.equal(stats.rpc.running, 2); assert.ok(stats.rpc.queued <= 3); assert.equal(peak, 2);
  gate.resolve(); const results = await Promise.all(work);
  const rejected = results.filter(result => !result.ok);
  assert.ok(rejected.length >= 15); assert.ok(rejected.every(result => result.code === 9));
  assert.ok(results.filter(result => result.ok).length <= 5); assert.equal(peak, 2);
  await eventually(async () => (await bus.stats()).rpc.retainedBytes === 0, 'released RPC memory');
});

test('RPC: réplicas recebem trabalhos exclusivos, sem broadcast', async t => {
  const { bus, handle } = await fixture(t); const gate = deferred(); const seen = new Map(); const workers = new Set();
  t.after(() => gate.resolve());
  for (const worker of ['A', 'B']) await handle('users.lookup', async body => {
    seen.set(body.id, (seen.get(body.id) ?? 0) + 1); workers.add(worker); await gate.promise;
    return { id: body.id, worker };
  }, { concurrency: 1, queueLimit: 32 });
  const results = Promise.all(Array.from({ length: 10 }, (_, id) => bus.requestJSON('users.lookup', { id }, { timeoutMs: 2000 })));
  await eventually(() => workers.size === 2, 'both replicas to receive work'); gate.resolve();
  assert.equal((await results).length, 10); assert.equal(seen.size, 10);
  assert.ok([...seen.values()].every(count => count === 1));
});

test('RPC: deadline na fila cancela a chamada antes da execução', async t => {
  const { bus, handle } = await fixture(t); const gate = deferred(); const executed = [];
  t.after(() => gate.resolve());
  await handle('billing.run', async body => { executed.push(body.id); await gate.promise; return body.id; }, { concurrency: 1, queueLimit: 8 });
  const first = bus.requestJSON('billing.run', { id: 1 }, { timeoutMs: 2000 });
  const guardedFirst = first.catch(error => error);
  await eventually(() => executed.length === 1);
  await assert.rejects(bus.requestJSON('billing.run', { id: 2 }, { timeoutMs: 30 }), e => e.code === 10);
  assert.deepEqual(executed, [1]); gate.resolve(); assert.equal(await guardedFirst, 1);
  await delay(40); assert.deepEqual(executed, [1]);
  const stats = await bus.stats(); assert.equal(stats.rpc.queued, 0); assert.equal(stats.rpc.timedOut, 1);
});

test('RPC: timeout em execução não libera vaga de handler que ainda está rodando', async t => {
  const { bus, handle } = await fixture(t); const gate = deferred(); const contexts = []; const executed = [];
  t.after(() => gate.resolve());
  await handle('inventory.reserve', async (body, context) => {
    contexts.push(context); executed.push(body.id);
    if (body.id === 1) await gate.promise; // deliberately ignores cancellation until released
    return body.id;
  }, { concurrency: 1, queueLimit: 4 });
  const expired = assert.rejects(bus.requestJSON('inventory.reserve', { id: 1 }, { timeoutMs: 250 }), e => e.code === 10);
  await eventually(() => executed.length === 1, 'first handler to start', 200);
  await expired;
  await eventually(() => contexts[0]?.signal.aborted, 'handler deadline AbortSignal');
  const second = bus.requestJSON('inventory.reserve', { id: 2 }, { timeoutMs: 2000 });
  const guardedSecond = second.catch(error => error);
  await eventually(async () => (await bus.stats()).rpc.queued === 1);
  assert.deepEqual(executed, [1]); assert.equal((await bus.stats()).rpc.running, 1);
  gate.resolve(); assert.equal(await guardedSecond, 2); assert.deepEqual(executed, [1, 2]);
});

test('RPC: desconexão do chamador remove trabalho em fila', async t => {
  const { bus, handle, client } = await fixture(t); const gate = deferred(); const executed = [];
  t.after(() => gate.resolve());
  await handle('jobs.execute', async body => { executed.push(body.id); await gate.promise; return body.id; }, { concurrency: 1, queueLimit: 8 });
  const first = bus.requestJSON('jobs.execute', { id: 1 }, { timeoutMs: 2000 }).catch(error => error);
  await eventually(() => executed.length === 1);
  const other = await client();
  const pending = other.requestJSON('jobs.execute', { id: 2 }, { timeoutMs: 2000 }).catch(error => error);
  await eventually(async () => (await bus.stats()).rpc.queued === 1);
  await other.close(); await pending;
  await eventually(async () => (await bus.stats()).rpc.queued === 0, 'disconnected caller removed');
  gate.resolve(); assert.equal(await first, 1); await delay(30); assert.deepEqual(executed, [1]);
});

test('RPC: queda do worker falha chamada ativa sem reenvio e close não espera handler travado', async t => {
  const { bus, handle } = await fixture(t); const gate = deferred(); let started = false;
  t.after(() => gate.resolve());
  const worker = await handle('backend.unstable', async () => { started = true; await gate.promise; return 'late'; }, { concurrency: 1 });
  const failed = assert.rejects(bus.requestJSON('backend.unstable', {}, { timeoutMs: 2000 }), e => e.code === 11);
  await eventually(() => started);
  await worker.close(); await failed;
  const stats = await bus.stats(); assert.equal(stats.rpc.running, 0); assert.equal(stats.rpc.retainedBytes, 0);
  await assert.rejects(bus.request('backend.unstable', 'x'), e => e.code === 8);
  gate.resolve();
});

test('RPC: orçamento global em bytes rejeita chamada antes do handler', async t => {
  const { bus, handle } = await fixture(t, { args: ['--max-rpc-bytes', '512'] }); let started = false;
  await handle('budget.bytes', async () => { started = true; return 'unexpected'; });
  await assert.rejects(bus.request('budget.bytes', Buffer.alloc(1024)), e => e.code === 9);
  assert.equal(started, false); assert.equal((await bus.stats()).rpc.retainedBytes, 0);
});

test('RPC: orçamento global em chamadas limita fila mais execução', async t => {
  const { bus, handle } = await fixture(t, { args: ['--max-rpc-calls', '2'] }); const gate = deferred(); let started = false;
  t.after(() => gate.resolve());
  await handle('budget.calls', async body => { started = true; await gate.promise; return body; }, { concurrency: 1, queueLimit: 8 });
  const first = bus.requestJSON('budget.calls', 1, { timeoutMs: 2000 }).catch(error => error);
  await eventually(() => started);
  const second = bus.requestJSON('budget.calls', 2, { timeoutMs: 2000 }).catch(error => error);
  await eventually(async () => (await bus.stats()).rpc.queued === 1);
  await assert.rejects(bus.requestJSON('budget.calls', 3), e => e.code === 9);
  gate.resolve(); assert.equal(await first, 1); assert.equal(await second, 2);
});

test('RPC: replicas discordantes não alteram limite existente da rota', async t => {
  const { bus, handle } = await fixture(t);
  await handle('config.route', async body => body, { queueLimit: 4 });
  await assert.rejects(bus.handleJSON('config.route', async body => body, { queueLimit: 5 }), e => e.code === 3);
  assert.deepEqual(await bus.requestJSON('config.route', { still: 'working' }), { still: 'working' });
  const route = (await bus.stats()).rpc.routes.find(route => route.name === 'config.route');
  assert.equal(route.workers, 1); assert.equal(route.queueLimit, 4);
});

test('RPC: 32 slots completam sem deadlock no teto da conexão do worker', async t => {
  const { bus, handle } = await fixture(t); const gate = deferred(); let started = 0;
  t.after(() => gate.resolve());
  await handle('capacity.maximum', async body => { started++; await gate.promise; return body; }, { concurrency: 32, queueLimit: 64 });
  const results = Promise.all(Array.from({ length: 32 }, (_, index) => bus.requestJSON('capacity.maximum', index, { timeoutMs: 3000 })));
  await eventually(() => started === 32, '32 active handlers'); gate.resolve();
  assert.deepEqual(await results, Array.from({ length: 32 }, (_, index) => index));
  await bus.ping();
  await eventually(async () => (await bus.stats()).rpc.retainedBytes === 0, '32 completions to release memory');
});
