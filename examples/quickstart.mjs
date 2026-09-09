import { connect } from '../packages/client/dist/esm/index.js';

const bus = await connect({
  host: process.env.NODARA_HOST ?? '127.0.0.1',
  port: Number(process.env.NODARA_PORT ?? 7447),
  token: process.env.NODARA_TOKEN,
});
let users;
try {
  users = await bus.handleJSON('users.get', async ({ id }) => ({ id, name: 'Ada' }), {
    concurrency: 4,
    queueLimit: 32,
  });
  const user = await bus.requestJSON('users.get', { id: 42 }, { timeoutMs: 1000 });
  console.log('Resposta do microserviço:', user);
  console.log('Capacidade e filas:', (await bus.stats()).rpc);
} finally {
  if (users) await users.close();
  await bus.close();
}
