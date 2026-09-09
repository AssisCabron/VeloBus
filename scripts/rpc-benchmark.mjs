import { fork, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { cpus, platform, arch, release, totalmem } from 'node:os';
import { readFile, mkdir, stat, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { createHash } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { connect } from '../packages/client/dist/esm/index.js';
import { root, startBroker } from './harness.mjs';
import { sampleProcesses } from './benchmarks/resources.mjs';

const exec = promisify(execFile);
const common = { payloadBytes: 256, handlerDelayMs: 0, queueLimit: 128, deadlineMs: 1000 };
const slow = { ...common, mode: 'open', handlerDelayMs: 10, workerConcurrency: 8, queueLimit: 32, concurrency: 128, connections: 4 };
export const scenarios = [
  { ...common, name: 'echo-c1', mode: 'closed', replicas: 1, workerConcurrency: 1, concurrency: 1, connections: 1 },
  { ...common, name: 'echo-c32', mode: 'closed', replicas: 1, workerConcurrency: 32, concurrency: 32, connections: 1 },
  { ...common, name: 'echo-c128', mode: 'closed', replicas: 4, workerConcurrency: 32, concurrency: 128, connections: 4, queueLimit: 256 },
  { ...common, name: 'echo-64k', mode: 'closed', replicas: 2, workerConcurrency: 16, concurrency: 32, connections: 1, payloadBytes: 65536 },
  { ...slow, name: 'slow-normal', replicas: 2, ratePerSecond: 800 },
  { ...slow, name: 'slow-overload', replicas: 2, ratePerSecond: 4000 },
  { ...slow, name: 'slow-scale-out', replicas: 4, ratePerSecond: 4000 },
];

class Peer {
  constructor(script, config) {
    this.messages = []; this.waiters = []; this.failure = null;
    this.stderr = '';
    this.child = fork(join(root, 'scripts', 'benchmarks', script), config ? [JSON.stringify(config)] : [], {
      cwd: root, stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
    });
    this.child.stderr.on('data', chunk => { this.stderr = (this.stderr + chunk).slice(-16000); });
    this.exited = new Promise(resolveExit => this.child.once('close', (code, signal) => resolveExit({ code, signal })));
    this.child.on('error', error => this.fail(error));
    this.child.on('exit', (code, signal) => this.fail(new Error(`${script} exited (${code ?? signal}): ${this.stderr}`)));
    this.child.on('message', message => {
      if (message.type === 'error') { this.fail(new Error(message.message)); return; }
      const index = this.waiters.findIndex(waiter => waiter.type === message.type);
      if (index < 0) this.messages.push(message);
      else this.waiters.splice(index, 1)[0].resolve(message);
    });
  }
  fail(error) { this.failure = error; for (const waiter of this.waiters.splice(0)) waiter.reject(error); }
  send(message) { this.child.send(message); }
  next(type, timeoutMs = 20000) {
    const index = this.messages.findIndex(message => message.type === type);
    if (index >= 0) return Promise.resolve(this.messages.splice(index, 1)[0]);
    if (this.failure) return Promise.reject(this.failure);
    return new Promise((resolveNext, rejectNext) => {
      const done = callback => value => { clearTimeout(timer); callback(value); };
      const waiter = { type, resolve: done(resolveNext), reject: done(rejectNext) };
      const timer = setTimeout(() => {
        const pendingIndex = this.waiters.indexOf(waiter);
        if (pendingIndex >= 0) this.waiters.splice(pendingIndex, 1);
        rejectNext(new Error(`Timeout waiting for ${type}: ${this.stderr}`));
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }
  async stop() {
    if (this.child.pid === undefined) return this.exited;
    if (this.child.exitCode !== null || this.child.signalCode !== null) return;
    if (this.child.connected) this.send({ type: 'stop' });
    else this.child.kill('SIGTERM');
    const timer = setTimeout(() => this.child.kill('SIGKILL'), 3000);
    try { await this.exited; } finally { clearTimeout(timer); }
  }
}

export async function runScenario(config, { binary, durationMs, warmupMs }) {
  let server; let bus; let generator; let sampler;
  const workers = [];
  let sampling = Promise.resolve();
  let samplingBusy = false;
  const sampleErrors = [];
  const resources = {};
  const peak = { queued: 0, running: 0, retainedBytes: 0 };
  try {
    server = await startBroker({ binary });
    bus = await connect({ port: server.port });
    for (let i = 0; i < config.replicas; i++) {
      const worker = new Peer('worker.mjs', { ...config, port: server.port });
      workers.push(worker);
      await worker.next('ready');
    }
    generator = new Peer('load.mjs');
    generator.send({ type: 'run', config: { ...config, port: server.port, durationMs, warmupMs } });
    await generator.next('measuring', warmupMs + config.deadlineMs + 20000);
    for (const worker of workers) { worker.send({ type: 'reset' }); await worker.next('reset'); }
    const baseline = (await bus.stats()).rpc;
    if (baseline.running || baseline.queued || baseline.retainedBytes) throw new Error('Warmup did not drain');
    const processes = [
      { role: 'broker', pid: server.child.pid },
      ...workers.map((worker, i) => ({ role: `worker-${i + 1}`, pid: worker.child.pid })),
      { role: 'generator', pid: generator.child.pid },
      { role: 'coordinator', pid: process.pid },
    ];
    const startingBefore = performance.now();
    const starting = await sampleProcesses(processes.map(item => item.pid));
    const startingAfter = performance.now();
    for (const { role, pid } of processes) resources[role] = {
      start: starting[pid] ?? null, samples: starting[pid] ? 1 : 0,
      missingSamples: starting[pid] ? 0 : 1, rssPeakBytes: starting[pid]?.rssBytes ?? null,
    };
    async function sample() {
      const [statsResult, psResult] = await Promise.allSettled([
        bus.stats(), sampleProcesses(processes.map(item => item.pid)),
      ]);
      if (statsResult.status === 'fulfilled') {
        for (const field of Object.keys(peak)) peak[field] = Math.max(peak[field], statsResult.value.rpc[field]);
      } else if (sampleErrors.length < 10) sampleErrors.push(String(statsResult.reason));
      if (psResult.status === 'fulfilled') {
        for (const { role, pid } of processes) {
          const value = psResult.value[pid];
          if (value) {
            const item = resources[role];
            item.samples++;
            item.rssPeakBytes = Math.max(item.rssPeakBytes ?? 0, value.rssBytes);
          } else resources[role].missingSamples++;
        }
      } else if (sampleErrors.length < 10) sampleErrors.push(String(psResult.reason));
    }
    sampler = setInterval(() => {
      if (samplingBusy) return;
      samplingBusy = true;
      sampling = sample().finally(() => { samplingBusy = false; });
    }, 200);
    generator.send({ type: 'measure' });
    const { result: load } = await generator.next('result', durationMs + config.deadlineMs + 20000);
    clearInterval(sampler); await sampling;
    // The last response can reach the caller just before the worker's COMPLETE ACK.
    let final;
    const drainLimit = performance.now() + 3000;
    do {
      final = (await bus.stats()).rpc;
      if (!final.running && !final.queued && !final.retainedBytes) break;
      await delay(5);
    } while (performance.now() < drainLimit);
    await sample();
    const endingBefore = performance.now();
    const ending = await sampleProcesses(processes.map(item => item.pid));
    const endingAfter = performance.now();
    const resourceWindowMs = (endingBefore + endingAfter - startingBefore - startingAfter) / 2;
    const resourceBoundaryUncertaintyMs = (startingAfter - startingBefore + endingAfter - endingBefore) / 2;
    for (const { role, pid } of processes) {
      const item = resources[role]; const end = ending[pid];
      item.cpuMs = item.start && end ? end.cpuMs - item.start.cpuMs : null;
      item.averageCpuPercentOfOneCore = item.cpuMs === null ? null : 100 * item.cpuMs / resourceWindowMs;
      if (end) { item.rssPeakBytes = Math.max(item.rssPeakBytes ?? 0, end.rssBytes); item.samples++; }
      else item.missingSamples++;
      delete item.start;
    }
    const workerStats = [];
    for (const worker of workers) { worker.send({ type: 'snapshot' }); workerStats.push((await worker.next('snapshot')).result); }
    const counters = Object.fromEntries(['accepted', 'completed', 'rejected', 'timedOut'].map(key => [key, final[key] - baseline[key]]));
    const unexpectedErrors = Object.entries(load.errorsByCode).filter(([code]) => code !== '9');
    const violations = [];
    if (load.warmup && (Object.keys(load.warmup.errorsByCode).some(code => code !== '9') || !load.warmup.correctness.valid)) violations.push('Warmup had unexpected failures');
    if (unexpectedErrors.length) violations.push(`Unexpected RPC errors: ${JSON.stringify(unexpectedErrors)}`);
    if (final.running || final.queued || final.retainedBytes) violations.push('Broker did not drain');
    if (peak.queued > config.queueLimit || peak.running > config.replicas * config.workerConcurrency) violations.push('Sampled capacity exceeded');
    if (workerStats.some(worker => worker.active || worker.peak > config.workerConcurrency || worker.handlerErrors || worker.errors.length)) violations.push('Worker error or concurrency exceeded');
    if (workerStats.reduce((sum, worker) => sum + worker.executed, 0) !== load.successes) violations.push('Worker executions differ from successful replies');
    if (counters.completed !== load.successes || counters.accepted !== load.successes || counters.rejected !== (load.errorsByCode['9'] ?? 0)) violations.push('Broker counters differ from client outcomes');
    if (load.attempts !== load.successes + Object.values(load.errorsByCode).reduce((a, b) => a + b, 0)) violations.push('Client outcomes do not account for all attempts');
    if (config.mode === 'open' && load.scheduled !== load.attempts + load.generatorDropped) violations.push('Scheduled arrivals are unaccounted for');
    if (sampleErrors.length) violations.push('Resource/stat sampling failed');
    if (Object.values(resources).some(item => item.missingSamples || item.cpuMs === null || item.cpuMs < 0)) violations.push('Process resource samples missing or invalid');
    const probe = Buffer.from('recovery-probe');
    const probeStart = performance.now();
    let recoveryProbe;
    try {
      const response = await bus.request('benchmark.echo', probe, { timeoutMs: config.deadlineMs });
      recoveryProbe = { passed: response.payload.equals(probe), latencyMs: performance.now() - probeStart };
    } catch (error) {
      recoveryProbe = { passed: false, latencyMs: performance.now() - probeStart, code: error.code ?? error.name, message: error.message };
    }
    if (!recoveryProbe.passed) violations.push('Post-load recovery probe failed');
    return {
      scenario: config.name, config, load, broker: { counterDelta: counters, sampledPeak: peak, final },
      workers: workerStats, resources, resourceWindowMs, resourceBoundaryUncertaintyMs, samplingIntervalMs: 200, sampleErrors, recoveryProbe,
      validation: { passed: violations.length === 0, violations },
    };
  } finally {
    clearInterval(sampler);
    const cleanup = await Promise.allSettled([
      sampling, generator?.stop(), ...workers.map(worker => worker.stop()), bus?.close(), server?.stop(),
    ]);
    const failures = cleanup.filter(result => result.status === 'rejected');
    if (failures.length) throw new AggregateError(failures.map(result => result.reason), 'Benchmark cleanup failed');
  }
}

function parseFlags(argv) {
  const flags = new Map();
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i];
    if (flag === '--smoke') { flags.set(flag, true); continue; }
    if (!['--duration', '--warmup', '--repetitions', '--scenarios', '--output'].includes(flag) || argv[i + 1] === undefined) {
      throw new Error('Usage: npm run benchmark:rpc -- [--duration seconds] [--warmup seconds] [--repetitions n] [--scenarios name,name] [--output file.json] [--smoke]');
    }
    flags.set(flag, argv[++i]);
  }
  return flags;
}

async function main() {
  const flags = parseFlags(process.argv.slice(2));
  const smoke = flags.has('--smoke');
  const durationMs = 1000 * Number(flags.get('--duration') ?? (smoke ? 0.3 : 10));
  const warmupMs = 1000 * Number(flags.get('--warmup') ?? (smoke ? 0.1 : 2));
  const repetitions = Number(flags.get('--repetitions') ?? (smoke ? 1 : 3));
  if (!Number.isFinite(durationMs) || durationMs < 100 || durationMs > 300000 || !Number.isFinite(warmupMs) || warmupMs < 0 || warmupMs > 60000 || !Number.isInteger(repetitions) || repetitions < 1 || repetitions > 10) throw new Error('Invalid duration/warmup/repetitions');
  const selectedNames = flags.has('--scenarios') ? flags.get('--scenarios').split(',') : smoke ? ['echo-c1', 'slow-overload'] : scenarios.map(scenario => scenario.name);
  const selected = selectedNames.map(name => { const scenario = scenarios.find(item => item.name === name); if (!scenario) throw new Error(`Unknown scenario: ${name}`); return scenario; });
  const binary = process.env.VELOBUS_BIN ?? join(root, 'target/release/velobus');
  const report = {
    schemaVersion: 1, timestamp: new Date().toISOString(), purpose: smoke ? 'functional benchmark smoke check' : 'local RPC baseline; not a capacity certification',
    environment: {
      os: platform(), release: release(), architecture: arch(), cpu: cpus()[0]?.model, logicalCpus: cpus().length,
      systemMemoryBytes: totalmem(), node: process.version, rust: (await exec('rustc', ['--version'])).stdout.trim(),
      sourceCommit: (await exec('git', ['rev-parse', 'HEAD'], { cwd: root })).stdout.trim(),
      workingTreeDirty: Boolean((await exec('git', ['status', '--porcelain'], { cwd: root })).stdout.trim()),
      binaryLabel: process.env.VELOBUS_BIN ? 'VELOBUS_BIN override' : 'target/release/velobus',
      binaryBytes: (await stat(binary)).size, binarySha256: createHash('sha256').update(await readFile(binary)).digest('hex'),
    },
    methodology: {
      durationMs, warmupMs, repetitions, repeatOrder: 'rotate scenario order by repetition index',
      transport: 'loopback TCP', mode: 'memory; RPC has no WAL', tls: false, replication: false,
      processes: 'one Rust broker, one Node process per worker, one Node load generator, one Node coordinator on the same machine',
      handler: 'binary echo preserving every byte; optional cooperative timer simulates I/O wait, not CPU work',
      brokerLimits: { maxConnections: 64, maxRpcCalls: 1024, maxRpcBytes: 16777216 },
      cpu: 'ps cumulative CPU delta using midpoint timestamps of boundary samples; 100% equals one logical core; coarse precision (Linux commonly seconds); includes sampling and final drain',
      rss: 'sampled resident memory, not guaranteed peak; shared pages can be counted in more than one process',
      limitations: [
        'Closed-loop clients wait for replies before sending again; latency under a fixed external arrival rate can be worse.',
        'Open-loop reports planned arrivals, generator drops and scheduling lag separately; dropped arrivals have no RPC latency sample.',
        'No subtraction of handler delay from latency percentiles; no broker-only latency claim.',
        'No comparison with Kafka, RabbitMQ, NATS, direct HTTP, or Raspberry Pi hardware.',
        'Queue time and actual service time are not independently instrumented in the wire protocol.',
        'All processes compete on one desktop; measured throughput can be limited by SDK, generator, worker or host.',
      ],
    },
    runs: [],
  };
  for (let repetition = 0; repetition < repetitions; repetition++) {
    const order = [...selected.slice(repetition % selected.length), ...selected.slice(0, repetition % selected.length)];
    for (const scenario of order) {
      process.stderr.write(`[${repetition + 1}/${repetitions}] ${scenario.name}\n`);
      const run = await runScenario(scenario, { binary, durationMs, warmupMs });
      report.runs.push({ repetition: repetition + 1, ...run });
      process.stderr.write(`  success=${run.load.successes} errors=${JSON.stringify(run.load.errorsByCode)} valid=${run.validation.passed}\n`);
    }
  }
  report.validation = { passed: report.runs.every(run => run.validation.passed) };
  const json = `${JSON.stringify(report, null, 2)}\n`;
  if (flags.has('--output')) { const path = resolve(flags.get('--output')); await mkdir(dirname(path), { recursive: true }); await writeFile(path, json); }
  process.stdout.write(json);
  if (!report.validation.passed) process.exitCode = 1;
}

if (process.argv[1] && resolve(process.argv[1]) === join(root, 'scripts/rpc-benchmark.mjs')) {
  main().catch(error => { process.stderr.write(`${error.stack ?? error}\n`); process.exitCode = 1; });
}
