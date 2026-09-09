import http from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import { connect } from '../packages/client/dist/esm/index.js';

// In a real application these two functions live in separate backend services.
export async function startUsersWorker({ broker = {}, concurrency = 2, queueLimit = 8, latencyMs = 25, name = 'users-1' } = {}) {
  const bus = await connect(broker); let handle;
  const metrics = { active: 0, peak: 0, completed: 0 };
  try {
    handle = await bus.handleJSON('users.get', async ({ id }, context) => {
      metrics.active++; metrics.peak = Math.max(metrics.peak, metrics.active);
      try {
        // Replace with a DB query or fetch(existingApi, {signal: context.signal}).
        await delay(latencyMs, undefined, { signal: context.signal });
        metrics.completed++;
        return { id, name: `User ${id}`, servedBy: name };
      } finally { metrics.active--; }
    }, { concurrency, queueLimit });
    return { metrics, async close() { await handle.close(); await bus.close(); } };
  } catch (error) { await bus.close(); throw error; }
}

export async function startApiGateway({ broker = {}, port = 8080, timeoutMs = 1500, maxHttpRequests = 32 } = {}) {
  const bus = await connect(broker); let active = 0;
  const server = http.createServer(async (request, response) => {
    response.setHeader('content-type', 'application/json; charset=utf-8');
    const route = /^\/users\/([0-9]{1,12})$/.exec(request.url ?? '');
    if (request.method !== 'GET' || !route) {
      response.writeHead(404); response.end(JSON.stringify({ error: 'Use GET /users/42' })); return;
    }
    if (active >= maxHttpRequests) {
      response.writeHead(503, { 'retry-after': '1' }); response.end(JSON.stringify({ error: 'gateway_overloaded' })); return;
    }
    active++;
    try {
      const result = await bus.requestJSON('users.get', { id: route[1] }, { timeoutMs });
      response.writeHead(200); response.end(JSON.stringify(result));
    } catch (error) {
      const status = error.code === 10 ? 504
        : [8, 9, 11].includes(error.code) || error.name === 'BackpressureError' ? 503 : 500;
      response.writeHead(status, status === 503 ? { 'retry-after': '1' } : {});
      response.end(JSON.stringify({ error: status === 504 ? 'upstream_timeout' : status === 503 ? 'service_unavailable' : 'upstream_error' }));
    } finally { active--; }
  });
  server.requestTimeout = 5000; server.headersTimeout = 5000;
  try {
    await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, '127.0.0.1', resolve); });
    return {
      url: `http://127.0.0.1:${server.address().port}`,
      async close() {
        const closed = new Promise(resolve => server.close(resolve));
        server.closeAllConnections(); await bus.close(); await closed;
      },
    };
  } catch (error) { await bus.close(); throw error; }
}
