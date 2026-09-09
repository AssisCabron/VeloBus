import assert from 'node:assert/strict';
import test from 'node:test';
import {
  CPU_TIME_RESOLUTION_CAVEAT,
  parseCpuTime,
  parsePsOutput,
  sampleProcesses,
} from './resources.mjs';

test('CPU time parser handles Linux, macOS, long runtimes and fractional seconds', () => {
  assert.equal(parseCpuTime('00:00:00'), 0);
  assert.equal(parseCpuTime('00:01:03'), 63_000);
  assert.equal(parseCpuTime('0:00.03'), 30);
  assert.equal(parseCpuTime(' 12:34.50 '), 754_500);
  assert.equal(parseCpuTime('125:01.25'), 7_501_250);
  assert.equal(parseCpuTime('27:03:04'), 97_384_000);
  assert.equal(parseCpuTime('2-03:04:05.250'), 183_845_250);
  assert.equal(parseCpuTime('0:00.001'), 1);
  assert.ok(Math.abs(parseCpuTime('0:00.000123') - 0.123) < 1e-12);
});

test('invalid or overflowing CPU counters remain unavailable rather than zero', () => {
  for (const value of [
    '', '-', 'N/A', '00', '1:60', '1:99.3', '1:60:00', '1-24:00:00',
    '2-03:04', '-1:02', '1:02.', '1:02.3extra', '1:02:03:04',
    '999999999999999999999:00', '9999999999999-23:59:59', null, undefined, 0,
  ]) {
    assert.equal(parseCpuTime(value), null, `unexpected valid counter: ${String(value)}`);
  }
});

test('ps output converts RSS KiB to bytes and preserves cumulative CPU units', () => {
  const output = '  1425   2048   00:01:02\n  9218  10560    1:02.75\n 772 100 3-01:02:03\n';
  assert.deepEqual(parsePsOutput(output), {
    1425: { rssBytes: 2_097_152, cpuMs: 62_000 },
    9218: { rssBytes: 10_813_440, cpuMs: 62_750 },
    772: { rssBytes: 102_400, cpuMs: 262_923_000 },
  });
});

test('requested PIDs missing from output are explicitly null and extras are excluded', () => {
  assert.deepEqual(parsePsOutput(' 12 1024 0:01.00\n 99 4 00:00:02\n', [12, 14, 12]), {
    12: { rssBytes: 1_048_576, cpuMs: 1000 },
    14: null,
  });
  assert.deepEqual(parsePsOutput('', [12, 14]), { 12: null, 14: null });
  assert.deepEqual(parsePsOutput(' \n\t\r\n'), {});
  assert.deepEqual(parsePsOutput('12 1 0:00\n', []), {});
});

test('malformed rows with identifiable PIDs produce null, including duplicate rows', () => {
  assert.deepEqual(parsePsOutput([
    '11 -1 00:01:00',
    '12 1.5 00:01:00',
    '13 123 unknown',
    '14 123',
    '15 123 0:01 surplus',
    '16 9007199254740991 0:01',
    '17 100 0:01',
    '17 101 0:02',
    '18 0 0:00',
  ].join('\n')), {
    11: null, 12: null, 13: null, 14: null, 15: null, 16: null, 17: null,
    18: { rssBytes: 0, cpuMs: 0 },
  });
});

test('unattributable rows and invalid requested PID lists are explicit errors', () => {
  for (const output of ['not process output', '0 4 0:00', '-12 4 0:00', '999999999999999999999 4 0:00']) {
    assert.throws(() => parsePsOutput(output), SyntaxError);
  }
  assert.throws(() => parsePsOutput(null), TypeError);
  for (const pids of [[0], [-1], [1.5], ['12'], [undefined], [2_147_483_648], null]) {
    assert.throws(() => parsePsOutput('', pids), TypeError);
  }
});

test('empty process groups are explicitly empty and CPU quantization is documented', async () => {
  assert.deepEqual(await sampleProcesses([]), {});
  assert.match(CPU_TIME_RESOLUTION_CAVEAT, /whole seconds/);
  assert.match(CPU_TIME_RESOLUTION_CAVEAT, /200 ms/);
  assert.match(CPU_TIME_RESOLUTION_CAVEAT, /unavailable, not zero/);
  await assert.rejects(sampleProcesses([NaN]), TypeError);
});
