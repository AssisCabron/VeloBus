import test from 'node:test';
import assert from 'node:assert/strict';
import { startBroker } from './harness.mjs';
import { startApiGateway, startUsersWorker } from '../examples/http-services.mjs';

async function fixture(t, { workerOptions, timeoutMs } = {}) {
  const server = await startBroker(); const broker = { port: server.port }; let worker; let gateway;
  t.after(async () => { if (gateway) await gateway.close(); if (worker) await worker.close(); await server.stop(); });
  if (workerOptions !== false) worker = await startUsersWorker({ broker, ...workerOptions });
  gateway = await startApiGateway({ broker, port: 0, timeoutMs });
  return { worker, gateway };
}

test('HTTP API -> VeloBus -> microserviço -> resposta JSON', async t => {
  const { gateway } = await fixture(t);
  const response = await fetch(`${gateway.url}/users/42`);
  assert.equal(response.status, 200); assert.equal((await response.json()).id, '42');
});

test('HTTP bombardeio retorna 503 sem exceder concorrência do backend', async t => {
  const { gateway, worker } = await fixture(t, { workerOptions: { concurrency: 2, queueLimit: 2, latencyMs: 100 }, timeoutMs: 2000 });
  const responses = await Promise.all(Array.from({ length: 20 }, (_, id) => fetch(`${gateway.url}/users/${id}`)));
  assert.ok(responses.some(response => response.status === 200));
  assert.ok(responses.some(response => response.status === 503));
  assert.ok(responses.every(response => [200, 503].includes(response.status)));
  await Promise.all(responses.map(response => response.arrayBuffer()));
  assert.ok(worker.metrics.peak <= 2);
});

test('HTTP serviço ausente retorna 503 e deadline retorna 504', async t => {
  const unavailable = await fixture(t, { workerOptions: false });
  const absent = await fetch(`${unavailable.gateway.url}/users/42`);
  assert.equal(absent.status, 503); await absent.arrayBuffer();
  const slow = await fixture(t, { workerOptions: { latencyMs: 250 }, timeoutMs: 30 });
  const expired = await fetch(`${slow.gateway.url}/users/42`);
  assert.equal(expired.status, 504); await expired.arrayBuffer();
});
