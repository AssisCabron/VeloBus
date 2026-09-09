import assert from 'node:assert/strict';
import test from 'node:test';
import { LatencyHistogram, OpenLoopSchedule, chooseConnection } from './metrics.mjs';

test('histogram percentiles use nearest ranks and bound approximation error', () => {
  const histogram = new LatencyHistogram();
  for (let i = 1; i <= 100; i++) histogram.record(i);
  const result = histogram.summary();
  for (const [key, exact] of [['p50', 50], ['p95', 95], ['p99', 99]]) {
    assert.ok(result[key] >= exact);
    assert.ok(result[key] <= exact * 1.01 + 1e-10);
  }
  assert.equal(result.count, 100);
  assert.equal(result.min, 1);
  assert.equal(result.mean, 50.5);
  assert.equal(result.max, 100);
  assert.equal(result.overflowCount, 0);
  assert.ok(histogram.buckets.length < 2000, 'Histogram allocation must remain compact');
});

test('histogram tracks zeros, sub-floor samples and explicit overflow without clipping percentiles', () => {
  const empty = new LatencyHistogram();
  assert.equal(empty.summary().p99, null);
  assert.equal(empty.summary().max, null);
  empty.record(0);
  assert.equal(empty.percentile(50), 0);
  empty.record(0.005);
  assert.equal(empty.percentile(100), 0.005);
  const overflow = new LatencyHistogram({ maxMs: 10 });
  overflow.record(1); overflow.record(11); overflow.record(1000);
  assert.equal(overflow.summary().count, 3);
  assert.equal(overflow.summary().overflowCount, 2);
  assert.equal(overflow.summary().max, 1000);
  assert.equal(overflow.summary().p50, null);
  assert.equal(overflow.summary().mean, 1012 / 3);
  assert.throws(() => overflow.record(-1), RangeError);
  assert.throws(() => overflow.record(NaN), RangeError);
  assert.throws(() => overflow.percentile(0), RangeError);
});

test('histogram repeated samples are counted without storing samples', () => {
  const histogram = new LatencyHistogram();
  const allocated = histogram.buckets.byteLength;
  for (let i = 0; i < 100000; i++) histogram.record(i % 2 ? 0.5 : 5);
  assert.equal(histogram.buckets.byteLength, allocated);
  assert.equal(histogram.count, 100000);
  assert.equal(histogram.percentile(50) >= 0.5 && histogram.percentile(50) <= 0.505, true);
  assert.equal(histogram.summary().mean, 2.75);
});

test('planned arrivals remain independent of replies and full capacity drops demand explicitly', () => {
  const schedule = new OpenLoopSchedule({ startMs: 100, durationMs: 100, ratePerSecond: 1000, maxCatchUp: 64 });
  assert.equal(schedule.totalScheduled, 100);
  const first = schedule.advance(100, { available: 2 });
  assert.deepEqual(first.admitted, [{ index: 0, plannedMs: 100 }]);
  const full = schedule.advance(105, { available: 0 });
  assert.equal(full.scheduled, 5);
  assert.equal(full.dropped.pendingLimit, 5);
  assert.equal(full.admitted.length, 0);
  const recovered = schedule.advance(107, { available: 1 });
  assert.deepEqual(recovered.admitted, [{ index: 6, plannedMs: 106 }]);
  assert.equal(recovered.dropped.pendingLimit, 1);
  assert.equal(schedule.nextPlannedMs, 108);
});

test('late scheduler bounds catch-up and accounts every dropped arrival', () => {
  const schedule = new OpenLoopSchedule({ startMs: 0, durationMs: 1000, ratePerSecond: 10000, maxCatchUp: 64 });
  const tick = schedule.advance(100, { available: 32 });
  assert.equal(tick.scheduled, 1001);
  assert.equal(tick.dropped.catchUpLimit, 937);
  assert.equal(tick.dropped.pendingLimit, 32);
  assert.equal(tick.admitted.length, 32);
  assert.equal(tick.admitted[0].index, 937);
  assert.equal(tick.admitted.at(-1).index, 968);
  const final = schedule.advance(1001, { available: 128 });
  assert.equal(final.admitted.length, 0, 'Never admit new requests after the window');
  assert.equal(final.dropped.windowExpired, 8999);
  assert.equal(schedule.nextIndex, 10000);
  assert.equal(schedule.advance(2000, { available: 128 }).scheduled, 0);
});

test('unavailable connections and pending limits have distinct drop reasons', () => {
  const schedule = new OpenLoopSchedule({ startMs: 0, durationMs: 1000, ratePerSecond: 10 });
  const tick = schedule.advance(100, { available: 5, unavailable: true });
  assert.equal(tick.dropped.connectionUnavailable, 2);
  assert.equal(tick.dropped.pendingLimit, 0);
  assert.equal(tick.admitted.length, 0);
});

test('irregular ticks preserve scheduled = attempted + dropped and never exceed capacity', () => {
  for (const rate of [1, 7, 999, 100000]) {
    const schedule = new OpenLoopSchedule({ startMs: 12.5, durationMs: 120, ratePerSecond: rate, maxCatchUp: 8 });
    let planned = 0, attempted = 0, dropped = 0;
    for (const [i, elapsed] of [0, 0.1, 1, 20, 21, 50, 50, 100, 120, 140].entries()) {
      const available = i % 5;
      const tick = schedule.advance(12.5 + elapsed, { available });
      assert.ok(tick.admitted.length <= available);
      assert.ok(tick.admitted.length <= 8);
      assert.ok(tick.admitted.every(arrival => arrival.plannedMs < 132.5));
      const lost = Object.values(tick.dropped).reduce((sum, value) => sum + value, 0);
      assert.equal(tick.scheduled, tick.admitted.length + lost);
      planned += tick.scheduled;
      attempted += tick.admitted.length;
      dropped += lost;
    }
    assert.equal(planned, schedule.totalScheduled);
    assert.equal(planned, attempted + dropped);
  }
});

test('fair socket selection never exceeds 32 pending requests per connection', () => {
  const pending = [0, 0, 0, 0];
  const healthy = [true, true, true, true];
  let start = 0;
  for (let i = 0; i < 128; i++) {
    const choice = chooseConnection(pending, healthy, start);
    assert.notEqual(choice, -1);
    pending[choice]++;
    start = (choice + 1) % pending.length;
    assert.ok(Math.max(...pending) - Math.min(...pending) <= 1);
  }
  assert.deepEqual(pending, [32, 32, 32, 32]);
  assert.equal(chooseConnection(pending, healthy, start), -1);
  pending[2]--;
  assert.equal(chooseConnection(pending, healthy, start), 2);
  healthy[2] = false;
  assert.equal(chooseConnection(pending, healthy, start), -1);
});
