import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

export const CPU_TIME_RESOLUTION_CAVEAT =
  'ps time is cumulative process CPU time, including its threads, rather than instantaneous CPU percent. '
  + 'Its displayed resolution varies: Linux commonly reports whole seconds and macOS commonly reports hundredths of a second. '
  + 'Sampling every 200 ms does not improve that counter resolution; short intervals may show a zero delta or quantization noise. '
  + 'Compute CPU usage from start/end samples of the same surviving PID over the measured wall-clock interval; missing samples are unavailable, not zero.';

/**
 * Parse POSIX ps TIME: D-HH:MM:SS, HH:MM:SS or M:SS, optionally with fractional seconds.
 * The two-field form permits total minutes greater than 59, as seen on macOS.
 * Returns cumulative CPU milliseconds, or null when the value cannot be trusted.
 */
export function parseCpuTime(value) {
  if (typeof value !== 'string') return null;
  const match = /^(?:(\d+)-)?(?:(\d+):)?(\d+):(\d{1,2}(?:\.\d{1,9})?)$/.exec(value.trim());
  if (!match) return null;
  const [, dayText, hourText, minuteText, secondText] = match;
  if (dayText !== undefined && hourText === undefined) return null;

  const days = Number(dayText ?? 0);
  const hours = Number(hourText ?? 0);
  const minutes = Number(minuteText);
  const seconds = Number(secondText);
  if (![days, hours, minutes].every(Number.isSafeInteger)) return null;
  if (seconds >= 60 || (hourText !== undefined && minutes >= 60)) return null;
  if (dayText !== undefined && hours >= 24) return null;

  const milliseconds = (((days * 24 + hours) * 60 + minutes) * 60 + seconds) * 1000;
  if (!Number.isFinite(milliseconds) || milliseconds > Number.MAX_SAFE_INTEGER) return null;
  return milliseconds;
}

function normalizePids(pids) {
  if (!Array.isArray(pids)) throw new TypeError('Process IDs must be an array of positive integers');
  for (const pid of pids) {
    if (!Number.isSafeInteger(pid) || pid <= 0 || pid > 2_147_483_647) {
      throw new TypeError('Process IDs must be positive integers no greater than 2147483647');
    }
  }
  return [...new Set(pids)];
}

/**
 * Parse headerless `ps -o pid=,rss=,time=` output. RSS is KiB on Linux and macOS.
 * Each key maps to { rssBytes, cpuMs } or null for a missing/malformed process row.
 * When requestedPids is supplied, only those PIDs are returned, including missing ones.
 * A row without a usable PID is a parse error because it cannot be attributed safely.
 */
export function parsePsOutput(output, requestedPids) {
  if (typeof output !== 'string') throw new TypeError('ps output must be a string');
  const requested = requestedPids === undefined ? undefined : normalizePids(requestedPids);
  const wanted = requested === undefined ? undefined : new Set(requested);
  const samples = Object.fromEntries((requested ?? []).map((pid) => [pid, null]));
  const seen = new Set();

  for (const [lineIndex, rawLine] of output.split(/\r?\n/).entries()) {
    const line = rawLine.trim();
    if (line === '') continue;
    const fields = line.split(/\s+/);
    const pid = /^\d+$/.test(fields[0]) ? Number(fields[0]) : NaN;
    if (!Number.isSafeInteger(pid) || pid <= 0 || pid > 2_147_483_647) {
      throw new SyntaxError(`ps returned a row without a valid PID at line ${lineIndex + 1}`);
    }
    if (wanted !== undefined && !wanted.has(pid)) continue;
    // ps should have one row per PID. Ambiguous duplicate rows are unavailable.
    if (seen.has(pid)) {
      samples[pid] = null;
      continue;
    }
    seen.add(pid);
    samples[pid] = null;
    if (fields.length !== 3 || !/^\d+$/.test(fields[1])) continue;

    const rssKiB = Number(fields[1]);
    const rssBytes = rssKiB * 1024;
    const cpuMs = parseCpuTime(fields[2]);
    if (!Number.isSafeInteger(rssKiB) || !Number.isSafeInteger(rssBytes) || cpuMs === null) continue;
    samples[pid] = { rssBytes, cpuMs };
  }
  return samples;
}

/**
 * Sample an entire PID group with one ps invocation. No recursive child discovery.
 * A PID that exited between samples is null. Other command/parse failures reject.
 * PID reuse cannot be detected from these three ps columns; the caller owns process lifetimes.
 */
export async function sampleProcesses(pids) {
  const requested = normalizePids(pids);
  if (requested.length === 0) return {};

  let stdout;
  try {
    ({ stdout } = await execFileAsync('ps', ['-p', requested.join(','), '-o', 'pid=,rss=,time='], {
      encoding: 'utf8',
      env: { ...process.env, LC_ALL: 'C' },
      timeout: 3000,
      maxBuffer: 1024 * 1024,
    }));
  } catch (error) {
    // ps conventionally exits 1 when none of the selected PIDs exist. Preserve missing
    // samples, but do not confuse an actual ps diagnostic, timeout or launch failure with that case.
    if (error.code === 1 && !error.killed && !error.signal && typeof error.stdout === 'string'
      && typeof error.stderr === 'string' && error.stderr.trim() === '') {
      return parsePsOutput(error.stdout, requested);
    }
    const reason = error.code ?? error.signal ?? 'unknown execution failure';
    throw new Error(`Unable to sample process resources with ps (${reason})`, { cause: error });
  }
  return parsePsOutput(stdout, requested);
}
