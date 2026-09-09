import { setTimeout as delay } from 'node:timers/promises';
import { connectCluster } from '../packages/client/dist/esm/index.js';
import { startBroker } from '../scripts/harness.mjs';

const servers = []; let pool;
try {
  for (let i = 0; i < 3; i++) servers.push(await startBroker());
  pool = await connectCluster({ brokers: servers.map((server, i) => ({ id: `broker-${i + 1}`, host: '127.0.0.1', port: server.port })), healthIntervalMs: 100 });
  const service = await pool.handleJSON('catalog.get', async ({ id }) => ({ id, name: 'Example product' }), { concurrency: 4, queueLimit: 16 });
  for (let i = 0; i < 6; i++) console.log(await pool.requestJSON('catalog.get', { id: i }));
  console.log('Registered brokers:', service.nodes());
  await servers[0].stop('SIGKILL');
  const limit = Date.now() + 3000;
  while (pool.nodes()[0].connected && Date.now() < limit) await delay(10);
  console.log('After broker-1 failure:', pool.nodes());
  console.log('Response from remaining brokers:', await pool.requestJSON('catalog.get', { id: 42 }));
} finally {
  await pool?.close();
  await Promise.allSettled(servers.map(server => server.stop()));
}
