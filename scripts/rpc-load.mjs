import { cpus, platform, arch } from 'node:os';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { connect } from '../packages/client/dist/esm/index.js';
import { startBroker } from './harness.mjs';

const output = process.argv[2];
const server = await startBroker(); const clients = []; const workers = [];
let active = 0; let peak = 0; let executed = 0;
try {
  const control = await connect({ port: server.port }); clients.push(control);
  for (let replica = 0; replica < 2; replica++) {
    workers.push(await control.handleJSON('users.get', async ({ id }, context) => {
      active++; executed++; peak = Math.max(peak, active);
      try { await delay(50, undefined, { signal: context.signal }); return { id, replica }; }
      finally { active--; }
    }, { concurrency: 2, queueLimit: 8 }));
  }
  const callers = [];
  for (let i = 0; i < 2; i++) { const client = await connect({ port: server.port }); clients.push(client); callers.push(client); }
  const start = performance.now();
  const results = await Promise.all(Array.from({ length: 64 }, async (_, id) => {
    const began = performance.now();
    try {
      const result = await callers[id % callers.length].requestJSON('users.get', { id }, { timeoutMs: 2000 });
      if (result.id !== id) throw new Error('Response correlation mismatch');
      return { ok: true, latencyMs: performance.now() - began };
    } catch (error) { return { ok: false, code: error.code ?? error.name, latencyMs: performance.now() - began }; }
  }));
  const success = results.filter(result => result.ok);
  const overloaded = results.filter(result => result.code === 9);
  const unexpected = results.filter(result => !result.ok && result.code !== 9);
  const rpc = (await control.stats()).rpc;
  if (unexpected.length || peak > 4 || rpc.running || rpc.queued || rpc.retainedBytes || executed !== success.length) {
    throw new Error(`Overload protection verification failed: ${JSON.stringify({ peak, executed, unexpected, rpc })}`);
  }
  const report = {
    timestamp: new Date().toISOString(),
    purpose: 'Short local verification of RPC overload protection, not a throughput benchmark',
    environment: { cpu: cpus()[0]?.model, os: platform(), architecture: arch(), node: process.version },
    workload: { requests: 64, callers: 2, replicas: 2, concurrencyPerReplica: 2, queueLimit: 8, handlerDelayMs: 50, deadlineMs: 2000, transport: 'loopback TCP', binary: process.env.NODARA_BIN ? 'explicit binary from NODARA_BIN' : 'target/debug/nodara' },
    results: { succeeded: success.length, overloaded: overloaded.length, unexpectedErrors: unexpected.length, observedPeakHandlers: peak, executed, elapsedMs: performance.now() - start },
    rpc,
    notes: ['Waiting calls are bounded; overload returns a structured error instead of forwarding to every service.', 'Exact admitted count depends on scheduling.', 'Caller and worker code run on the same Mac, not on a Raspberry Pi.', 'This test does not prove high availability or sustained capacity.'],
  };
  const json = `${JSON.stringify(report, null, 2)}\n`;
  if (output) { const path = resolve(output); await mkdir(dirname(path), { recursive: true }); await writeFile(path, json); }
  process.stdout.write(json);
} finally {
  await Promise.allSettled(workers.map(worker => worker.close()));
  await Promise.allSettled(clients.map(client => client.close()));
  await server.stop();
}
