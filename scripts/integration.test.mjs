import test from 'node:test';
import assert from 'node:assert/strict';
import { appendFile, readFile, readdir, stat, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import net from 'node:net';
import { connect } from '../packages/client/dist/esm/index.js';
import { startBroker, temporaryDirectory, removeTemporaryDirectory } from './harness.mjs';

async function fixture(t, options = {}) {
  const server = await startBroker(options);
  t.after(() => server.stop());
  const bus = await connect({ port: server.port, token: options.token });
  t.after(() => bus.close());
  return { server, bus };
}

function wireString(value) {
  const body = Buffer.from(value); const length = Buffer.alloc(2);
  length.writeUInt16BE(body.length); return Buffer.concat([length, body]);
}

function envelope(opcode, id, body = Buffer.alloc(0)) {
  const header = Buffer.alloc(9); header.writeUInt32BE(5 + body.length); header[4] = opcode;
  header.writeUInt32BE(id, 5); return Buffer.concat([header, body]);
}

async function rawConnection(port) {
  const socket = net.createConnection({ host: '127.0.0.1', port });
  await new Promise((resolve, reject) => { socket.once('connect', resolve); socket.once('error', reject); });
  let buffer = Buffer.alloc(0); const waiters = [];
  socket.on('data', chunk => {
    buffer = Buffer.concat([buffer, chunk]);
    while (buffer.length >= 4 && buffer.length >= buffer.readUInt32BE(0) + 4) {
      const size = buffer.readUInt32BE(0); const frame = buffer.subarray(4, size + 4);
      buffer = buffer.subarray(size + 4); waiters.shift()?.resolve(frame);
    }
  });
  const fail = () => { for (const waiter of waiters.splice(0)) waiter.reject(new Error('Socket encerrado')); };
  socket.on('close', fail); socket.on('error', fail);
  return {
    socket,
    request(bytes) {
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => { socket.destroy(); reject(new Error('Raw request timeout')); }, 3000);
        waiters.push({ resolve: value => { clearTimeout(timer); resolve(value); }, reject: err => { clearTimeout(timer); reject(err); } });
        socket.write(bytes);
      });
    },
  };
}

test('autenticação, ping e estatísticas por SDK real', async t => {
  const { server, bus } = await fixture(t, { token: 'integration-only-token' });
  await assert.rejects(connect({ port: server.port, token: 'wrong-token' }), error => error.code === 2);
  await bus.ping();
  const stats = await bus.stats();
  assert.equal(stats.mode, 'memory'); assert.equal(stats.lastSequence, '0'); assert.equal(stats.records, 0);
});

test('publicação binária, JSON e respostas concorrentes preservam conteúdo e sequências', async t => {
  const { bus } = await fixture(t);
  const binary = Buffer.from([0, 255, 128, 1, 0, 42]);
  const receipt = await bus.publish('events.binary', binary, { key: 'ação' });
  assert.equal(receipt.firstSequence, 1n); assert.equal(receipt.durable, false);
  await bus.publishJSON('events.json', { title: 'Olá', n: 42 });
  const receipts = await Promise.all(Array.from({ length: 20 }, (_, i) => bus.publish('events.concurrent', String(i))));
  assert.equal(new Set(receipts.map(r => r.lastSequence)).size, 20);
  const batch = await bus.fetch('*');
  assert.equal(batch.events.length, 22); assert.equal(batch.cursor, 22n);
  assert.deepEqual(batch.events[0].payload, binary); assert.equal(batch.events[0].key, 'ação');
  assert.deepEqual(batch.events[1].json(), { title: 'Olá', n: 42 });
});

test('latest combina por tópico/chave, mantém ordem e preserva histórico all', async t => {
  const { bus } = await fixture(t);
  await bus.publishBatch([
    { topic: 'cache.state', key: 'A', payload: 'A1' },
    { topic: 'cache.state', key: 'B', payload: 'B2' },
    { topic: 'cache.state', key: 'A', payload: 'A3' },
    { topic: 'cache.state', payload: 'independent4' },
    { topic: 'cache.state', payload: 'independent5' },
    { topic: 'other', key: 'A', payload: 'other6' },
  ]);
  const latest = await bus.fetch('*', { mode: 'latest' });
  assert.deepEqual(latest.events.map(e => e.sequence), [2n, 3n, 4n, 5n, 6n]);
  assert.deepEqual(latest.events.map(e => e.text()), ['B2', 'A3', 'independent4', 'independent5', 'other6']);
  assert.equal((await bus.fetch('*', { mode: 'all' })).events.length, 6);
  assert.ok((await bus.stats()).coalescedRecords >= 1);
});

test('orçamento exato de fetch e substituição latest não saltam registros', async t => {
  const { bus } = await fixture(t);
  await bus.publishBatch([
    { topic: 'x', key: 'a', payload: 'one' },
    { topic: 'x', key: 'a', payload: 'x'.repeat(100) },
  ]);
  const exactBytes = 16 + 1 + 1 + 3;
  await assert.rejects(bus.fetch('x', { maxBytes: exactBytes - 1 }), e => e.code === 5);
  const small = await bus.fetch('x', { mode: 'latest', maxBytes: exactBytes });
  assert.equal(small.events[0].text(), 'one'); assert.equal(small.cursor, 1n); assert.equal(small.hasMore, true);
  const next = await bus.fetch('x', { after: small.cursor, maxBytes: 256 });
  assert.equal(next.events[0].sequence, 2n); assert.equal(next.events[0].payload.length, 100);
});

test('paginação por tópico atravessa janela de varredura sem perder evento', async t => {
  const { bus } = await fixture(t);
  const block = Array.from({ length: 256 }, () => ({ topic: 'noise', payload: 'x' }));
  for (let i = 0; i < 17; i++) await bus.publishBatch(block);
  await bus.publish('wanted', 'found');
  const empty = await bus.fetch('wanted');
  assert.equal(empty.events.length, 0); assert.equal(empty.cursor, 4096n); assert.equal(empty.hasMore, true);
  const found = await bus.fetch('wanted', { after: empty.cursor });
  assert.equal(found.events[0].sequence, 4353n); assert.equal(found.events[0].text(), 'found');
  await assert.rejects(bus.fetch('*', { after: 99999n }), e => e.code === 7);
});

test('limite de capacidade rejeita lote inteiro e mantém sequência', async t => {
  const { bus } = await fixture(t, { args: ['--max-retained-bytes', '1024'] });
  await assert.rejects(bus.publishBatch([
    { topic: 'x', payload: Buffer.alloc(800) }, { topic: 'x', payload: Buffer.alloc(800) },
  ]), e => e.code === 4);
  assert.equal((await bus.stats()).records, 0);
  assert.equal((await bus.publish('x', 'ok')).lastSequence, 1n);
});

test('servidor rejeita lote malformado sem publicar a parte válida', async t => {
  const { server, bus } = await fixture(t);
  const raw = await rawConnection(server.port); t.after(() => raw.socket.destroy());
  const version = Buffer.from([0, 1]);
  assert.equal((await raw.request(envelope(1, 1, Buffer.concat([version, wireString('')]))))[0], 0x81);
  const payload = Buffer.from([0, 0, 0, 1, 42]);
  const valid = Buffer.concat([wireString('valid'), wireString(''), payload]);
  const invalid = Buffer.concat([wireString('bad/topic'), wireString(''), payload]);
  const response = await raw.request(envelope(2, 2, Buffer.concat([Buffer.from([0, 2]), valid, invalid])));
  assert.equal(response[0], 0xff);
  assert.equal((await bus.stats()).records, 0);
});

test('envelope fragmentado funciona e comprimento enorme encerra conexão', async t => {
  const { server } = await fixture(t);
  const raw = await rawConnection(server.port); t.after(() => raw.socket.destroy());
  const hello = envelope(1, 1, Buffer.from([0, 1, 0, 0]));
  const result = raw.request(hello.subarray(0, 2));
  await delay(10); raw.socket.write(hello.subarray(2, 7));
  await delay(10); raw.socket.write(hello.subarray(7));
  assert.equal((await result)[0], 0x81);
  const closed = new Promise(resolve => raw.socket.once('close', resolve));
  const oversized = Buffer.alloc(4); oversized.writeUInt32BE(0xffffffff); raw.socket.write(oversized);
  let deadline;
  try {
    await Promise.race([closed, new Promise((_, reject) => {
      deadline = setTimeout(() => reject(new Error('Conexão de frame enorme permaneceu aberta')), 2000);
    })]);
  } finally { clearTimeout(deadline); }
});

test('subscribe acompanha novas mensagens e cancela ociosidade', async t => {
  const { bus } = await fixture(t);
  const controller = new AbortController();
  const iterator = bus.subscribe('live', { signal: controller.signal, pollIntervalMs: 10 })[Symbol.asyncIterator]();
  t.after(() => { controller.abort(); });
  const pending = iterator.next();
  await delay(25); await bus.publishJSON('live', { ok: true });
  const value = await pending;
  assert.equal(value.done, false); assert.deepEqual(value.value.json(), { ok: true });
  const idle = iterator.next(); controller.abort();
  assert.equal((await idle).done, true);
  await bus.ping();
});

test('lote confirmado em disco sobrevive SIGKILL e retoma sequência', async t => {
  const dataDir = await temporaryDirectory(); let server; let bus;
  t.after(async () => { if (bus) await bus.close(); if (server) await server.stop(); await removeTemporaryDirectory(dataDir); });
  server = await startBroker({ dataDir }); bus = await connect({ port: server.port });
  const receipt = await bus.publishBatch([
    { topic: 'durable', key: 'A', payload: 'first' }, { topic: 'durable', key: 'B', payload: Buffer.from([0, 255]) },
  ]);
  assert.equal(receipt.durable, true);
  await server.stop('SIGKILL'); await bus.close();
  server = await startBroker({ dataDir }); bus = await connect({ port: server.port });
  const batch = await bus.fetch('*');
  assert.equal(batch.events.length, 2); assert.deepEqual(batch.events[1].payload, Buffer.from([0, 255]));
  assert.equal((await bus.publish('durable', 'third')).lastSequence, 3n);
});

test('mesmo diretório de dados recusa segundo escritor', async t => {
  const dataDir = await temporaryDirectory();
  const { bus } = await fixture(t, { dataDir });
  t.after(() => removeTemporaryDirectory(dataDir));
  await bus.publish('x', 'kept');
  await assert.rejects(startBroker({ dataDir }), /Broker encerrou/);
  assert.equal((await bus.fetch('*')).events[0].text(), 'kept');
});

async function walFile(dataDir) {
  const files = await readdir(dataDir);
  const name = files.find(f => f.endsWith('.wal'));
  assert.ok(name, `Arquivo .wal esperado; encontrados: ${files.join(', ')}`);
  return join(dataDir, name);
}

test('recuperação remove somente cauda incompleta do WAL', async t => {
  const dataDir = await temporaryDirectory(); let server; let bus;
  t.after(async () => { if (bus) await bus.close(); if (server) await server.stop(); await removeTemporaryDirectory(dataDir); });
  server = await startBroker({ dataDir }); bus = await connect({ port: server.port });
  await bus.publish('x', 'committed'); await bus.close(); await server.stop();
  const path = await walFile(dataDir); const originalSize = (await stat(path)).size;
  await appendFile(path, Buffer.from([0x01, 0x02, 0x03]));
  server = await startBroker({ dataDir }); bus = await connect({ port: server.port });
  assert.equal((await bus.fetch('*')).events[0].text(), 'committed');
  assert.equal((await stat(path)).size, originalSize);
});

test('corrupção de conteúdo no WAL falha sem apagar evidência', async t => {
  const dataDir = await temporaryDirectory(); let server; let bus;
  t.after(async () => { if (bus) await bus.close(); if (server) await server.stop(); await removeTemporaryDirectory(dataDir); });
  server = await startBroker({ dataDir }); bus = await connect({ port: server.port });
  await bus.publish('x', 'committed'); await bus.close(); await server.stop();
  const path = await walFile(dataDir); const bytes = await readFile(path);
  bytes[bytes.length - 1] ^= 0xff; await writeFile(path, bytes);
  await assert.rejects(startBroker({ dataDir }), /Broker encerrou/);
  assert.deepEqual(await readFile(path), bytes);
});
