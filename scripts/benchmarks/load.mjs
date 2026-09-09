import { performance, monitorEventLoopDelay } from 'node:perf_hooks';
import { connect, BrokerError } from '../../packages/client/dist/esm/index.js';
import { LatencyHistogram, OpenLoopSchedule, chooseConnection } from './metrics.mjs';

const ROUTE = 'benchmark.echo';
const PER_CONNECTION_LIMIT = 32;
const MAX_CATCH_UP = 64;
const MEMORY_SAMPLE_MS = 100;
let clients = [];
let runStarted = false;
let terminating = false;
let measureResolve;
let stopResolve;
let nextId = 1;

function integer(value, minimum, maximum, label) {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new RangeError(`${label} must be an integer in ${minimum}..${maximum}`);
  }
  return value;
}

function validateConfig(input) {
  if (!input || typeof input !== 'object') throw new TypeError('Expected benchmark config');
  if (input.mode !== 'closed' && input.mode !== 'open') throw new TypeError('mode must be closed or open');
  const config = {
    port: integer(input.port, 1, 65535, 'port'),
    mode: input.mode,
    durationMs: integer(input.durationMs, 1, 3600000, 'durationMs'),
    warmupMs: integer(input.warmupMs, 0, 600000, 'warmupMs'),
    concurrency: integer(input.concurrency, 1, 8192, 'concurrency'),
    connections: integer(input.connections, 1, 256, 'connections'),
    payloadBytes: integer(input.payloadBytes, 4, 262144, 'payloadBytes'),
    deadlineMs: integer(input.deadlineMs, 1, 30000, 'deadlineMs'),
  };
  if (config.concurrency > config.connections * PER_CONNECTION_LIMIT) {
    throw new RangeError('concurrency must be <= connections * 32 (broker per-socket operation cap)');
  }
  if (config.mode === 'open') {
    if (!Number.isFinite(input.ratePerSecond) || input.ratePerSecond <= 0 || input.ratePerSecond > 100000000) {
      throw new RangeError('ratePerSecond must be positive and <= 100000000');
    }
    config.ratePerSecond = input.ratePerSecond;
  }
  return config;
}

function ipc(message) {
  return new Promise((resolve, reject) => {
    if (!process.send || !process.connected) { reject(new Error('Benchmark load process requires an active IPC parent')); return; }
    process.send(message, error => error ? reject(error) : resolve());
  });
}

async function cleanup() {
  await Promise.allSettled(clients.map(client => client.close()));
  clients = [];
}

async function fatal(error) {
  if (terminating) return;
  terminating = true;
  const message = error instanceof Error ? error.message : String(error);
  try { await ipc({ type: 'error', message }); } catch { /* Parent may already have disconnected. */ }
  await cleanup();
  process.exit(1);
}

function finite(value) { return Number.isFinite(value) ? value : null; }
function latencyFromNanoseconds(value) { return finite(value / 1e6); }

async function runPhase(config, durationMs, payload) {
  const successLatency = new LatencyHistogram();
  const errorLatency = new LatencyHistogram();
  const scheduleLag = config.mode === 'open' ? new LatencyHistogram() : null;
  const scheduledSuccessLatency = config.mode === 'open' ? new LatencyHistogram() : null;
  const pendingByConnection = clients.map(() => 0);
  const healthy = clients.map(() => true);
  const errorsByCode = Object.create(null);
  const droppedByReason = { catchUpLimit: 0, pendingLimit: 0, windowExpired: 0, connectionUnavailable: 0 };
  let nextConnection = 0;
  let pending = 0, peakPending = 0;
  let attempts = 0, successes = 0, successesWithinWindow = 0;
  let scheduled = config.mode === 'open' ? 0 : null;
  let generatorDropped = 0, correlationFailures = 0;
  let closed = false, failed = false;
  let observedWindowEnd = 0;
  let windowTimer, tickTimer, tickIsImmediate = false;
  let doneResolve, doneReject;
  const done = new Promise((resolve, reject) => { doneResolve = resolve; doneReject = reject; });
  const memoryBefore = process.memoryUsage();
  let peakRssBytesSampled = memoryBefore.rss;
  let peakHeapUsedBytesSampled = memoryBefore.heapUsed;
  const loopDelay = monitorEventLoopDelay({ resolution: 10 });
  loopDelay.enable();
  const cpuBefore = process.cpuUsage();
  const start = performance.now();
  const cutoff = start + durationMs;
  const schedule = config.mode === 'open'
    ? new OpenLoopSchedule({ startMs: start, durationMs, ratePerSecond: config.ratePerSecond, maxCatchUp: MAX_CATCH_UP })
    : null;
  const memoryTimer = setInterval(() => {
    const sample = process.memoryUsage();
    peakRssBytesSampled = Math.max(peakRssBytesSampled, sample.rss);
    peakHeapUsedBytesSampled = Math.max(peakHeapUsedBytesSampled, sample.heapUsed);
  }, MEMORY_SAMPLE_MS);

  function clearTick() {
    if (tickTimer !== undefined) {
      if (tickIsImmediate) clearImmediate(tickTimer);
      else clearTimeout(tickTimer);
      tickTimer = undefined;
    }
  }

  function applyDrops(plan) {
    scheduled += plan.scheduled;
    for (const [reason, count] of Object.entries(plan.dropped)) {
      droppedByReason[reason] += count;
      generatorDropped += count;
    }
  }

  function finishWindow() {
    if (closed) return;
    observedWindowEnd = performance.now();
    if (schedule) applyDrops(schedule.advance(Math.max(cutoff, observedWindowEnd), { available: 0 }));
    closed = true;
    clearTimeout(windowTimer);
    clearTick();
    if (!pending) doneResolve();
  }

  function failPhase(error) {
    if (failed) return;
    failed = true;
    closed = true;
    clearTimeout(windowTimer);
    clearTick();
    doneReject(error);
  }

  function connectionChoice() {
    if (pending >= config.concurrency) return -1;
    return chooseConnection(pendingByConnection, healthy, nextConnection, PER_CONNECTION_LIMIT);
  }

  function launch(plannedMs = null) {
    if (closed) return false;
    const index = connectionChoice();
    if (index < 0) return false;
    if (nextId > 0xffffffff) throw new RangeError('Unique uint32 request IDs exhausted; use shorter benchmark runs');
    const id = nextId++;
    const sentAt = performance.now();
    if (sentAt >= cutoff) return false;
    payload.writeUInt32BE(id, 0);
    // SDK snapshots inputs synchronously; the immutable tail may be shared for verification.
    const expectedId = id;
    attempts++;
    pending++;
    pendingByConnection[index]++;
    peakPending = Math.max(peakPending, pending);
    nextConnection = (index + 1) % clients.length;
    if (plannedMs !== null) scheduleLag.record(Math.max(0, sentAt - plannedMs));

    const execute = async () => {
      try {
        const response = await clients[index].request(ROUTE, payload, { timeoutMs: config.deadlineMs });
        if (response.payload.length !== payload.length || response.payload.readUInt32BE(0) !== expectedId ||
            !response.payload.subarray(4).equals(payload.subarray(4))) {
          const mismatch = new Error('Echoed payload or unique request ID does not match');
          mismatch.code = 'CORRELATION_MISMATCH';
          correlationFailures++;
          throw mismatch;
        }
        const completedAt = performance.now();
        successes++;
        if (completedAt <= cutoff) successesWithinWindow++;
        successLatency.record(completedAt - sentAt);
        if (plannedMs !== null) scheduledSuccessLatency.record(Math.max(0, completedAt - plannedMs));
      } catch (error) {
        const completedAt = performance.now();
        const code = error instanceof BrokerError ? String(error.code) : String(error?.code ?? error?.name ?? 'UNKNOWN');
        errorsByCode[code] = (errorsByCode[code] ?? 0) + 1;
        errorLatency.record(Math.max(0, completedAt - sentAt));
        if (!(error instanceof BrokerError) && code !== 'CORRELATION_MISMATCH') healthy[index] = false;
      } finally {
        pending--;
        pendingByConnection[index]--;
        if (performance.now() >= cutoff && !closed) finishWindow();
        if (!closed && config.mode === 'closed') launch();
        if (closed && !pending) doneResolve();
      }
    };
    void execute().catch(failPhase);
    return true;
  }

  function tick() {
    tickTimer = undefined;
    if (closed) return;
    try {
      const now = performance.now();
      if (now >= cutoff) { finishWindow(); return; }
      const socketCapacity = pendingByConnection.reduce((sum, count, i) => sum + (healthy[i] ? PER_CONNECTION_LIMIT - count : 0), 0);
      const available = Math.max(0, Math.min(config.concurrency - pending, socketCapacity));
      const plan = schedule.advance(now, { available, unavailable: !healthy.some(Boolean) });
      applyDrops(plan);
      for (const arrival of plan.admitted) {
        if (!launch(arrival.plannedMs)) {
          // Serializing a bounded catch-up batch may itself reach the window cutoff.
          if (performance.now() >= cutoff) { droppedByReason.windowExpired++; generatorDropped++; }
          else throw new Error('Open-loop admission exceeded precomputed pending capacity');
        }
      }
      if (performance.now() >= cutoff) { finishWindow(); return; }
      const waitMs = Math.max(0, Math.min(cutoff, schedule.nextPlannedMs) - performance.now());
      // A bounded catch-up batch handles sub-millisecond arrival intervals without busy spinning.
      // Timer jitter remains visible in scheduleLagMs and scheduledSuccessLatencyMs.
      tickIsImmediate = false;
      tickTimer = setTimeout(tick, Math.max(1, waitMs));
    } catch (error) { failPhase(error); }
  }

  try {
    windowTimer = setTimeout(finishWindow, durationMs);
    if (schedule) tick();
    else {
      for (let slot = 0; slot < config.concurrency; slot++) {
        if (!launch()) break;
      }
    }
    await done;
  } finally {
    clearTimeout(windowTimer);
    clearTick();
    clearInterval(memoryTimer);
    loopDelay.disable();
  }

  const finishedAt = performance.now();
  const cpu = process.cpuUsage(cpuBefore);
  const memoryAfter = process.memoryUsage();
  const measuredDuration = Math.max(0, observedWindowEnd - start);
  const drainMs = Math.max(0, finishedAt - observedWindowEnd);
  const totalMs = measuredDuration + drainMs;
  const errorCount = Object.values(errorsByCode).reduce((sum, count) => sum + count, 0);
  if (attempts !== successes + errorCount || schedule && scheduled !== attempts + generatorDropped || pending !== 0) {
    throw new Error('Benchmark accounting invariant failed');
  }
  return {
    mode: config.mode,
    requestedDurationMs: durationMs,
    attempts, successes, errorsByCode,
    durationMs: measuredDuration,
    drainMs,
    successesPerSecond: totalMs > 0 ? successes * 1000 / totalMs : 0,
    successesWithinWindow,
    successesWithinWindowPerSecond: measuredDuration > 0 ? successesWithinWindow * 1000 / measuredDuration : 0,
    peakPending,
    successLatencyMs: successLatency.summary(),
    errorLatencyMs: errorLatency.summary(),
    scheduleLagMs: scheduleLag?.summary() ?? null,
    scheduledSuccessLatencyMs: scheduledSuccessLatency?.summary() ?? null,
    scheduled,
    generatorDropped,
    generatorDroppedByReason: droppedByReason,
    cpuUsageMs: { user: cpu.user / 1000, system: cpu.system / 1000, total: (cpu.user + cpu.system) / 1000 },
    cpuCoresUsed: totalMs > 0 ? (cpu.user + cpu.system) / 1000 / totalMs : 0,
    eventLoopDelayMs: {
      resolutionMs: 10,
      count: loopDelay.count,
      mean: latencyFromNanoseconds(loopDelay.mean),
      p50: loopDelay.count ? latencyFromNanoseconds(loopDelay.percentile(50)) : null,
      p95: loopDelay.count ? latencyFromNanoseconds(loopDelay.percentile(95)) : null,
      p99: loopDelay.count ? latencyFromNanoseconds(loopDelay.percentile(99)) : null,
      max: loopDelay.count ? latencyFromNanoseconds(loopDelay.max) : null,
    },
    memory: {
      before: memoryBefore, after: memoryAfter,
      peakRssBytesSampled: Math.max(peakRssBytesSampled, memoryAfter.rss),
      peakHeapUsedBytesSampled: Math.max(peakHeapUsedBytesSampled, memoryAfter.heapUsed),
      sampleIntervalMs: MEMORY_SAMPLE_MS,
    },
    correctness: { echoedBytesAndRequestIdsVerified: true, correlationFailures, valid: correlationFailures === 0 },
    scheduler: schedule ? {
      ratePerSecond: config.ratePerSecond,
      maxCatchUpPerTick: MAX_CATCH_UP,
      minimumTimerDelayMs: 1,
      perConnectionPendingLimit: PER_CONNECTION_LIMIT,
      scheduleLagSamples: 'Admitted requests only, from planned arrival to client.request invocation',
      dropPolicy: 'Drop oldest arrivals beyond bounded catch-up; drop remaining arrivals when pending capacity is full; never admit after cutoff',
    } : { perConnectionPendingLimit: PER_CONNECTION_LIMIT },
    latencyOrigin: 'client.request invocation, including local TCP queue; response payload and unique request ID verified',
    closedLoopLimitation: config.mode === 'closed'
      ? 'Closed-loop demand waits for responses and has coordinated omission: latency cannot represent an independent fixed arrival rate.'
      : null,
  };
}

async function run(input) {
  const config = validateConfig(input);
  const results = await Promise.allSettled(Array.from({ length: config.connections }, () => connect({
    host: '127.0.0.1', port: config.port, maxPendingRequests: PER_CONNECTION_LIMIT, timeoutMs: 5000,
  })));
  clients = results.filter(result => result.status === 'fulfilled').map(result => result.value);
  const failed = results.find(result => result.status === 'rejected');
  if (failed) throw new Error(`Benchmark connection setup failed: ${failed.reason?.message ?? failed.reason}`);
  const payload = Buffer.alloc(config.payloadBytes);
  for (let i = 4; i < payload.length; i++) payload[i] = (i * 31 + 17) & 0xff;

  const warmup = config.warmupMs > 0 ? await runPhase(config, config.warmupMs, payload) : null;
  // Do not silently replace dead sockets or benchmark a warmed-up disconnected client.
  await Promise.all(clients.map(client => client.ping()));
  const measure = new Promise((resolve, reject) => {
    const timer = setTimeout(() => { measureResolve = undefined; reject(new Error('Parent did not authorize measurement within 30 seconds')); }, 30000);
    measureResolve = () => { clearTimeout(timer); measureResolve = undefined; resolve(); };
  });
  await ipc({ type: 'measuring' });
  await measure;
  const result = await runPhase(config, config.durationMs, payload);
  result.config = config;
  result.warmup = warmup ? {
    attempts: warmup.attempts,
    successes: warmup.successes,
    errorsByCode: warmup.errorsByCode,
    durationMs: warmup.durationMs,
    drainMs: warmup.drainMs,
    generatorDropped: warmup.generatorDropped,
    generatorDroppedByReason: warmup.generatorDroppedByReason,
    correctness: warmup.correctness,
  } : null;
  const stopped = new Promise((resolve, reject) => {
    const timer = setTimeout(() => { stopResolve = undefined; reject(new Error('Parent did not stop completed load generator within 30 seconds')); }, 30000);
    stopResolve = () => { clearTimeout(timer); stopResolve = undefined; resolve(); };
  });
  await ipc({ type: 'result', result });
  // Keep this process and its idle connections available for the parent's final OS sample.
  await stopped;
  terminating = true;
  await cleanup();
  process.exit(0);
}

if (!process.send) {
  console.error('Run this benchmark load generator using child_process.fork and the documented IPC contract.');
  process.exitCode = 1;
} else {
  process.on('message', message => {
    if (message?.type === 'run') {
      if (runStarted) { void fatal(new Error('Benchmark load process accepts only one run')); return; }
      runStarted = true;
      void run(message.config).catch(fatal);
    } else if (message?.type === 'measure') {
      if (!measureResolve) { void fatal(new Error('Unexpected measurement authorization')); return; }
      measureResolve();
    } else if (message?.type === 'stop') {
      if (!stopResolve) { void fatal(new Error('Unexpected stop before completed measurement')); return; }
      stopResolve();
    }
  });
  process.on('disconnect', () => { if (!terminating) void fatal(new Error('Benchmark parent disconnected')); });
}
