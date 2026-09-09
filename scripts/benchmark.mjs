import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { cpus, platform, arch, release, totalmem } from 'node:os';
import { mkdir, stat, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { connect } from '../packages/client/dist/esm/index.js';
import { root, startBroker, temporaryDirectory, removeTemporaryDirectory } from './harness.mjs';

const exec = promisify(execFile);
const flags = new Map();
for (let i = 2; i < process.argv.length; i += 2) {
  const flag = process.argv[i]; const value = process.argv[i + 1];
  if (!['--messages', '--payload', '--batch', '--mode', '--keys', '--output'].includes(flag) || value === undefined) {
    throw new Error('Uso: npm run benchmark -- --messages 20000 --payload 256 --batch 128 --mode memory|disk --keys 100 [--output arquivo.json]');
  }
  flags.set(flag, value);
}
function integer(name, defaultValue, min, max) {
  const value = Number(flags.get(name) ?? defaultValue);
  if (!Number.isInteger(value) || value < min || value > max) throw new Error(`${name} deve estar entre ${min} e ${max}`);
  return value;
}
const messages = integer('--messages', 20000, 1, 1000000);
const payloadBytes = integer('--payload', 256, 0, 262144);
const batchSize = integer('--batch', 128, 1, 256);
const keyCount = integer('--keys', 100, 1, 1000000);
const mode = flags.get('--mode') ?? 'memory';
if (!['memory', 'disk'].includes(mode)) throw new Error('--mode deve ser memory ou disk');
if (batchSize * (payloadBytes + 40) > 1048500) throw new Error('Lote excederia o frame de 1 MiB; reduza --batch ou --payload');
const dataDir = await temporaryDirectory();
const binary = process.env.NODARA_BIN ?? join(root, 'target/release/nodara');
let server; let bus; let sampler; let observedRssPeak = 0; let sampling = false;
async function sampleRss() {
  if (!server || sampling) return;
  sampling = true;
  try {
    const { stdout } = await exec('ps', ['-p', String(server.child.pid), '-o', 'rss=']);
    const value = Number(stdout.trim());
    if (Number.isFinite(value)) observedRssPeak = Math.max(observedRssPeak, value * 1024);
  } catch { /* process might have exited between samples */ }
  finally { sampling = false; }
}

try {
  server = await startBroker({ binary, ...(mode === 'disk' ? { dataDir } : {}) });
  bus = await connect({ port: server.port, timeoutMs: 30000 });
  await sampleRss(); sampler = setInterval(sampleRss, 50); sampler.unref();
  const payload = Buffer.alloc(payloadBytes, 0x61);
  const latencies = []; let confirmations = 0; let durable = false;
  const start = performance.now();
  for (let offset = 0; offset < messages; offset += batchSize) {
    const count = Math.min(batchSize, messages - offset);
    const records = Array.from({ length: count }, (_, i) => ({ topic: 'bench.events', key: `k${(offset + i) % keyCount}`, payload }));
    const before = performance.now(); const receipt = await bus.publishBatch(records);
    latencies.push(performance.now() - before); confirmations += receipt.count; durable = receipt.durable;
  }
  const publishMs = performance.now() - start;
  if (confirmations !== messages || durable !== (mode === 'disk')) throw new Error('Contagem ou garantia de confirmação divergiu');
  async function consume(fetchMode) {
    let cursor = 0n; let delivered = 0; let batches = 0;
    const started = performance.now();
    do {
      const result = await bus.fetch('bench.events', { after: cursor, mode: fetchMode, limit: 256, maxBytes: 524288 });
      delivered += result.events.length; batches++;
      if (result.cursor === cursor && result.hasMore) throw new Error('Cursor não avançou com histórico pendente');
      cursor = result.cursor;
      if (!result.hasMore) break;
    } while (true);
    const elapsedMs = performance.now() - started;
    return { delivered, batches, cursor: String(cursor), elapsedMs, deliveriesPerSecond: delivered / (elapsedMs / 1000), payloadBytes: delivered * payloadBytes };
  }
  const all = await consume('all'); const latest = await consume('latest');
  if (all.delivered !== messages) throw new Error('Consumo all perdeu eventos');
  await sampleRss();
  latencies.sort((a, b) => a - b);
  const percentile = p => latencies[Math.min(latencies.length - 1, Math.ceil(latencies.length * p) - 1)];
  const report = {
    timestamp: new Date().toISOString(),
    status: 'local-development-measurement',
    environment: {
      os: platform(), release: release(), architecture: arch(), cpu: cpus()[0]?.model,
      logicalCpus: cpus().length, systemMemoryBytes: totalmem(), node: process.version,
      rust: (await exec('rustc', ['--version'])).stdout.trim(),
      brokerBinary: binary, brokerBinaryBytes: (await stat(binary)).size,
    },
    workload: { mode, messages, payloadBytes, batchSize, keyCount, producers: 1, transport: 'loopback TCP', consumers: 'one per sequential read phase', tls: false, replication: false, warmup: false },
    publish: { confirmedRecords: confirmations, durable, elapsedMs: publishMs, recordsPerSecond: confirmations / (publishMs / 1000), ackBatchLatencyMs: { p50: percentile(0.5), p95: percentile(0.95), p99: percentile(0.99), max: latencies.at(-1) } },
    all, latest,
    latestDeliveryReductionPercent: 100 * (1 - latest.delivered / all.delivered),
    observedBrokerRssPeakBytes: observedRssPeak,
    rssSamplingIntervalMs: 50,
    stats: await bus.stats(),
    caveats: [
      'Not a Raspberry Pi measurement or a comparison with another broker.',
      'Node client and server share this machine; phases run sequentially over loopback.',
      'ACK latency is measured per published batch, not per-message end-to-end processing latency.',
      'RSS is a sampled maximum, not a guaranteed peak or configured process limit.',
      'latest uses synthetic repeated keys; savings depend on application semantics and workload.',
      'Disk mode syncs the local WAL; power-loss behavior depends on actual hardware and storage.',
    ],
  };
  const json = `${JSON.stringify(report, null, 2)}\n`;
  if (flags.has('--output')) {
    const output = resolve(flags.get('--output')); await mkdir(dirname(output), { recursive: true }); await writeFile(output, json);
  }
  process.stdout.write(json);
} finally {
  clearInterval(sampler);
  if (bus) await bus.close();
  if (server) await server.stop();
  await removeTemporaryDirectory(dataDir);
}
