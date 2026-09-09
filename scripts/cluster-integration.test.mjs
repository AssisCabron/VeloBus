import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { connect, connectCluster, AmbiguousResultError, ClusterUnavailableError, BackpressureError } from '../packages/client/dist/esm/index.js';
import { startBroker } from './harness.mjs';

function deferred() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }
async function eventually(check, label = 'condition') {
  const until = Date.now() + 5000;
  while (Date.now() < until) { if (await check()) return; await delay(10); }
  assert.fail(`Timeout: ${label}`);
}
async function fixture(t, options = [{}, {}]) {
  const servers = []; const clients = [];
  t.after(async () => { await Promise.allSettled(clients.map(client => client.close())); await Promise.allSettled(servers.map(server => server.stop())); });
  for (const option of options) servers.push(await startBroker(option));
  const brokers = servers.map((server, i) => ({ id: `node-${i}`, host: '127.0.0.1', port: server.port, token: options[i].token }));
  return { servers, brokers,
    async single(i) { const client = await connect(brokers[i]); clients.push(client); return client; },
    async pool(extra = {}) { const client = await connectCluster({ brokers, timeoutMs: 200, healthIntervalMs: 50, reconnectMinMs: 50, reconnectMaxMs: 200, ...extra }); clients.push(client); return client; },
  };
}

test('cluster: distributes calls across independent brokers and preserves binary payloads', async t => {
  const f = await fixture(t); const direct = await Promise.all([f.single(0), f.single(1)]);
  for (const [i, bus] of direct.entries()) await bus.handleJSON('identify', async body => ({ ...body, node: i }));
  const pool = await f.pool(); const seen = new Set();
  for (let i = 0; i < 20; i++) { const result = await pool.requestJSON('identify', { id: i }); assert.equal(result.id, i); seen.add(result.node); }
  assert.equal(seen.size, 2);
  await pool.handle('binary', request => request.payload);
  const payload = Buffer.from([0, 255, 0, 128]);
  assert.deepEqual((await pool.request('binary', payload)).payload, payload);
  assert.equal(pool.nodes().filter(node => node.connected).length, 2);
});

test('cluster: worker concurrency is shared across brokers, not multiplied by node count', async t => {
  const f = await fixture(t); const pool = await f.pool(); let active = 0, peak = 0, completed = 0;
  const service = await pool.handleJSON('bounded', async body => {
    active++; peak = Math.max(active, peak);
    try { await delay(15); completed++; return body; } finally { active--; }
  }, { concurrency: 2, queueLimit: 32 });
  assert.equal(service.nodes().length, 2);
  const results = await Promise.all(Array.from({ length: 12 }, (_, id) => pool.requestJSON('bounded', { id })));
  assert.equal(completed, 12); assert.equal(peak, 2); assert.equal(active, 0);
  assert.deepEqual(results.map(result => result.id), Array.from({ length: 12 }, (_, i) => i));
});

test('cluster: survives a node loss and reconnects/reregisters workers after restart', async t => {
  const f = await fixture(t); const pool = await f.pool();
  const service = await pool.handleJSON('echo', body => body, { concurrency: 2 });
  await f.servers[0].stop('SIGKILL');
  await eventually(() => !pool.nodes()[0].connected, 'failed node removed');
  for (let id = 0; id < 10; id++) assert.equal((await pool.requestJSON('echo', { id })).id, id);
  const restarted = await startBroker({ args: ['--listen', `127.0.0.1:${f.brokers[0].port}`] });
  f.servers.push(restarted);
  await eventually(() => service.nodes().length === 2, 'worker reattached');
  for (let id = 0; id < 10; id++) await pool.requestJSON('echo', { id });
  const direct = await f.single(0); assert.ok((await direct.stats()).rpc.completed > 0);
});

test('cluster: in-flight loss is ambiguous, never replayed, and old handlers keep shared capacity', async t => {
  const f = await fixture(t); const pool = await f.pool(); const controls = await Promise.all([f.single(0), f.single(1)]);
  const release = deferred(); const started = deferred(); let calls = 0, active = 0, peak = 0;
  t.after(() => release.resolve());
  await pool.handleJSON('write', async body => {
    calls++; active++; peak = Math.max(peak, active);
    try { if (body.first) { started.resolve(); await release.promise; } return body; }
    finally { active--; }
  }, { concurrency: 1, queueLimit: 8 });
  const first = pool.requestJSON('write', { first: true }, { timeoutMs: 3000 }).catch(error => error);
  await started.promise;
  const stats = await Promise.all(controls.map(client => client.stats()));
  const index = stats.findIndex(value => value.rpc.running === 1); assert.ok(index >= 0);
  await f.servers[index].stop('SIGKILL');
  const failure = await first;
  assert.ok(failure instanceof AmbiguousResultError); assert.equal(failure.outcome, 'unknown');
  const second = pool.requestJSON('write', { first: false }, { timeoutMs: 2000 });
  await delay(50); assert.equal(calls, 1); assert.equal(active, 1);
  release.resolve(); assert.deepEqual(await second, { first: false });
  assert.equal(calls, 2); assert.equal(peak, 1);
});

test('cluster: safe NO_SERVICE rejection can move a call to another broker', async t => {
  const f = await fixture(t); const a = await f.single(0); const b = await f.single(1);
  let calls = 0; await b.handle('only-b', request => { calls++; return request.payload; });
  const pool = await f.pool(); assert.equal((await pool.request('only-b', 'ok')).text(), 'ok');
  assert.equal(calls, 1); assert.equal((await a.stats()).rpc.rejected, 1);
});

test('cluster: explicit overload retries another broker without duplicating execution', async t => {
  const f = await fixture(t, [{ args: ['--max-rpc-calls', '1'] }, {}]);
  const a = await f.single(0); const b = await f.single(1); const gate = deferred(); const started = deferred();
  t.after(() => gate.resolve());
  await a.handle('hold', async request => { started.resolve(); await gate.promise; return request.payload; });
  await b.handle('hold', request => request.payload);
  const busy = a.request('hold', 'busy'); await started.promise;
  const pool = await f.pool(); assert.equal((await pool.request('hold', 'other')).text(), 'other');
  assert.equal((await b.stats()).rpc.accepted, 1); assert.equal((await a.stats()).rpc.rejected, 1);
  gate.resolve(); await busy;
});

test('cluster: business errors and deadlines are not retried', async t => {
  const f = await fixture(t); const pool = await f.pool(); let failedCalls = 0, slowCalls = 0;
  await pool.handle('fail', () => { failedCalls++; throw new Error('business failure'); });
  await assert.rejects(pool.request('fail', ''), error => error.code === 12); assert.equal(failedCalls, 1);
  await pool.handle('slow', async () => { slowCalls++; await delay(200); return ''; });
  await assert.rejects(pool.request('slow', '', { timeoutMs: 100 }), error => error.code === 10);
  await delay(220); assert.equal(slowCalls, 1);
});

test('cluster: aggregate pending limit rejects locally and releases after completion', async t => {
  const f = await fixture(t); const pool = await f.pool({ maxPendingRequests: 1 }); const gate = deferred(); const started = deferred();
  t.after(() => gate.resolve());
  await pool.handle('hold', async request => { started.resolve(); await gate.promise; return request.payload; });
  const first = pool.request('hold', 'first'); await started.promise;
  await assert.rejects(pool.request('hold', 'second'), BackpressureError);
  gate.resolve(); await first; assert.equal((await pool.request('hold', 'third')).text(), 'third');
});

test('cluster: partial startup works, total outage fails without pretending to accept work', async t => {
  const f = await fixture(t); await f.servers[0].stop();
  const pool = await f.pool(); assert.equal(pool.nodes().filter(node => node.connected).length, 1);
  await pool.handle('echo', request => request.payload); assert.equal((await pool.request('echo', 'ok')).text(), 'ok');
  await f.servers[1].stop(); await eventually(() => pool.nodes().every(node => !node.connected));
  await assert.rejects(pool.request('echo', 'x'), ClusterUnavailableError);
  await assert.rejects(f.pool(), ClusterUnavailableError);
});

test('cluster: per-node credentials stay out of status and invalid nodes remain unavailable', async t => {
  const f = await fixture(t, [{ token: 'alpha' }, { token: 'beta' }]);
  const pool = await f.pool({ brokers: [f.brokers[0], { ...f.brokers[1], token: 'wrong-secret' }] });
  assert.equal(pool.nodes()[0].connected, true); assert.equal(pool.nodes()[1].connected, false);
  assert.doesNotMatch(JSON.stringify(pool.nodes()), /alpha|beta|wrong-secret/);
  await pool.handle('echo', request => request.payload); assert.equal((await pool.request('echo', 'ok')).text(), 'ok');
});

test('cluster: close is idempotent, unregisters every worker and prevents new calls', async t => {
  const f = await fixture(t); const pool = await f.pool(); const a = await f.single(0); const b = await f.single(1);
  const service = await pool.handle('echo', request => request.payload);
  await service.close(); await service.close();
  await eventually(async () => (await a.stats()).rpc.routes.length === 0 && (await b.stats()).rpc.routes.length === 0);
  await pool.close(); await pool.close();
  await assert.rejects(pool.request('echo', ''), ClusterUnavailableError);
  await assert.rejects(pool.handle('echo', () => ''), ClusterUnavailableError);
  assert.ok(pool.nodes().every(node => !node.connected));
});

test('cluster: invalid endpoint/configuration fails before opening connections', async () => {
  await assert.rejects(connectCluster({ brokers: [] }), RangeError);
  await assert.rejects(connectCluster({ brokers: [{ host: 'localhost' }, { host: 'localhost' }] }), TypeError);
  await assert.rejects(connectCluster({ brokers: [{ host: 'localhost' }], maxPendingRequests: 0 }), RangeError);
});

test('cluster: sustained replenishment at 32 operations per broker keeps workers registered', async t => {
  // Reserve the full transport budget for this test; heartbeat operations count
  // against the same limit and are covered by the other lifecycle scenarios.
  const f = await fixture(t); const pool = await f.pool({ healthIntervalMs: 60000 }); let executed = 0;
  const service = await pool.handleJSON('burst', body => { executed++; return body; }, { concurrency: 32, queueLimit: 128 });
  for (let batch = 0; batch < 5; batch++) {
    const results = await Promise.all(Array.from({ length: 64 }, (_, id) => pool.requestJSON('burst', { id, batch })));
    assert.ok(results.every((result, id) => result.id === id && result.batch === batch));
  }
  assert.equal(executed, 320); assert.equal(service.nodes().length, 2);
});

test('cluster: deadline while waiting for shared handler capacity never starts that handler', async t => {
  const f = await fixture(t); const pool = await f.pool(); const gate = deferred(); const started = deferred(); let calls = 0;
  t.after(() => gate.resolve());
  await pool.handleJSON('blocked', async body => {
    calls++; if (body.hold) { started.resolve(); await gate.promise; } return body;
  }, { concurrency: 1, queueLimit: 8 });
  const first = pool.requestJSON('blocked', { hold: true }, { timeoutMs: 2000 });
  await started.promise;
  await assert.rejects(pool.requestJSON('blocked', { hold: false }, { timeoutMs: 60 }), error => error.code === 10);
  gate.resolve(); await first; await delay(30); assert.equal(calls, 1);
});

test('client: disconnect observers notify once and can unsubscribe even after close', async t => {
  const f = await fixture(t, [{}]); const client = await f.single(0); let calls = 0;
  client.onDisconnect(() => { calls++; });
  const unsubscribe = client.onDisconnect(() => { calls += 100; }); unsubscribe();
  await client.close(); assert.equal(calls, 1); assert.equal(client.isClosed, true);
  const cancelLate = client.onDisconnect(() => { calls += 100; }); cancelLate();
  await delay(0); assert.equal(calls, 1);
});
