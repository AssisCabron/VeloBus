import { startApiGateway, startUsersWorker } from './http-services.mjs';

const broker = {
  host: process.env.NODARA_HOST ?? '127.0.0.1',
  port: Number(process.env.NODARA_PORT ?? 7447),
  token: process.env.NODARA_TOKEN,
};
const workers = []; let gateway; let closing = false;
async function close() {
  if (closing) return; closing = true;
  if (gateway) await gateway.close();
  await Promise.allSettled(workers.map(worker => worker.close()));
}
try {
  workers.push(await startUsersWorker({ broker, name: 'users-1' }));
  workers.push(await startUsersWorker({ broker, name: 'users-2' }));
  gateway = await startApiGateway({ broker, port: Number(process.env.PORT ?? 8080) });
  console.log(`API disponível em ${gateway.url}/users/42`);
  console.log('2 workers, concorrência 2 por worker e fila de 8 chamadas. Ctrl+C encerra.');
  process.once('SIGINT', () => { close().catch(console.error); });
  process.once('SIGTERM', () => { close().catch(console.error); });
} catch (error) { await close(); throw error; }
