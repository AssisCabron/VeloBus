import { setTimeout as delay } from 'node:timers/promises';
import { connect } from '../../packages/client/dist/esm/index.js';

const config = JSON.parse(process.argv[2]);
let bus;
let service;
let active = 0;
let peak = 0;
let executed = 0;
let finished = 0;
let handlerErrors = 0;
const errors = [];
const send = message => { if (process.connected) process.send(message); };

async function close() {
  await service?.close();
  await bus?.close();
}

process.on('message', async message => {
  try {
    if (message.type === 'reset') {
      if (active) throw new Error('Cannot reset worker metrics with active handlers');
      peak = 0; executed = 0; finished = 0; handlerErrors = 0;
      // Keep lifecycle/transport errors across warmup; resetting counters must not hide a dead service.
      send({ type: 'reset' });
    } else if (message.type === 'snapshot') {
      send({ type: 'snapshot', result: { active, peak, executed, finished, handlerErrors, errors } });
    } else if (message.type === 'stop') {
      await close();
      process.exit(0);
    }
  } catch (error) {
    send({ type: 'error', message: error.stack ?? String(error) });
    await close();
    process.exit(1);
  }
});
process.on('disconnect', () => { void close().finally(() => process.exit(0)); });

try {
  bus = await connect({ port: config.port });
  service = await bus.handle('benchmark.echo', async request => {
    active++; executed++; peak = Math.max(peak, active);
    try {
      if (config.handlerDelayMs) await delay(config.handlerDelayMs, undefined, { signal: request.signal });
      return request.payload;
    } catch (error) {
      handlerErrors++;
      throw error;
    } finally { active--; finished++; }
  }, {
    concurrency: config.workerConcurrency,
    queueLimit: config.queueLimit,
    onError(error) { if (errors.length < 10) errors.push({ name: error.name, code: error.code, message: error.message }); },
  });
  send({ type: 'ready' });
} catch (error) {
  send({ type: 'error', message: error.stack ?? String(error) });
  await close();
  process.exit(1);
}
