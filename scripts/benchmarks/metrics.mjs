/** Fixed-size geometric histogram. Percentiles are nearest-rank bucket upper bounds. */
export class LatencyHistogram {
  constructor({ floorMs = 0.01, relativePrecision = 0.01, maxMs = 60000 } = {}) {
    if (!(floorMs > 0) || !(relativePrecision > 0 && relativePrecision <= 1) || !(maxMs >= floorMs)) {
      throw new RangeError('Invalid histogram precision or range');
    }
    this.floorMs = floorMs;
    this.relativePrecision = relativePrecision;
    this.maxMs = maxMs;
    this.bounds = [0, floorMs];
    while (this.bounds.at(-1) < maxMs) this.bounds.push(Math.min(maxMs, this.bounds.at(-1) * (1 + relativePrecision)));
    this.buckets = new Float64Array(this.bounds.length);
    this.count = 0;
    this.sum = 0;
    this.min = Infinity;
    this.max = 0;
    this.overflowCount = 0;
  }

  record(value) {
    if (!Number.isFinite(value) || value < 0) throw new RangeError('Latency must be finite and nonnegative');
    if (this.count === Number.MAX_SAFE_INTEGER) throw new RangeError('Histogram count exceeds exact integer range');
    this.count++;
    this.sum += value;
    this.min = Math.min(this.min, value);
    this.max = Math.max(this.max, value);
    if (value > this.maxMs) { this.overflowCount++; return; }
    let low = 0, high = this.bounds.length - 1;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if (this.bounds[middle] >= value) high = middle;
      else low = middle + 1;
    }
    this.buckets[low]++;
  }

  percentile(percentile) {
    if (!Number.isFinite(percentile) || percentile <= 0 || percentile > 100) throw new RangeError('Percentile must be in (0,100]');
    if (!this.count) return null;
    const rank = Math.ceil(percentile / 100 * this.count);
    let cumulative = 0;
    for (let i = 0; i < this.buckets.length; i++) {
      cumulative += this.buckets[i];
      if (cumulative >= rank) return Math.min(this.bounds[i], this.max);
    }
    // Do not present a clipped ceiling as an observed high percentile.
    return null;
  }

  summary() {
    return {
      count: this.count,
      min: this.count ? this.min : null,
      mean: this.count ? this.sum / this.count : null,
      p50: this.percentile(50), p95: this.percentile(95), p99: this.percentile(99),
      max: this.count ? this.max : null,
      overflowCount: this.overflowCount,
      trackableMaxMs: this.maxMs,
      precision: {
        absoluteFloorMs: this.floorMs,
        relativeUpperBoundError: this.relativePrecision,
        percentileRepresentation: 'nearest-rank bucket upper bound; null when empty or rank is above tracked range',
      },
    };
  }
}

/** Constant planned arrivals, independent of responses. State and each returned plan are bounded. */
export class OpenLoopSchedule {
  constructor({ startMs, durationMs, ratePerSecond, maxCatchUp = 64 }) {
    if (!Number.isFinite(startMs) || !(durationMs > 0) || !Number.isFinite(durationMs) ||
        !(ratePerSecond > 0) || !Number.isFinite(ratePerSecond) || !Number.isSafeInteger(maxCatchUp) || maxCatchUp < 1) {
      throw new RangeError('Invalid open-loop schedule');
    }
    this.startMs = startMs;
    this.endMs = startMs + durationMs;
    this.intervalMs = 1000 / ratePerSecond;
    this.totalScheduled = Math.ceil(durationMs * ratePerSecond / 1000);
    if (!Number.isSafeInteger(this.totalScheduled)) throw new RangeError('Scheduled arrival count exceeds exact integer range');
    this.maxCatchUp = maxCatchUp;
    this.nextIndex = 0;
  }

  get nextPlannedMs() {
    return this.nextIndex < this.totalScheduled ? this.startMs + this.nextIndex * this.intervalMs : this.endMs;
  }

  advance(nowMs, { available, unavailable = false }) {
    if (!Number.isFinite(nowMs) || !Number.isSafeInteger(available) || available < 0) throw new RangeError('Invalid clock or capacity');
    const dropped = { catchUpLimit: 0, pendingLimit: 0, windowExpired: 0, connectionUnavailable: 0 };
    const dueEnd = nowMs >= this.endMs
      ? this.totalScheduled
      : Math.max(0, Math.min(this.totalScheduled, Math.floor((nowMs - this.startMs) / this.intervalMs) + 1));
    const scheduled = Math.max(0, dueEnd - this.nextIndex);
    const admitted = [];
    if (!scheduled) return { scheduled, admitted, dropped };
    if (nowMs >= this.endMs) {
      dropped.windowExpired = scheduled;
    } else {
      dropped.catchUpLimit = Math.max(0, scheduled - this.maxCatchUp);
      const considered = scheduled - dropped.catchUpLimit;
      const count = Math.min(considered, unavailable ? 0 : available);
      const first = this.nextIndex + dropped.catchUpLimit;
      for (let i = 0; i < count; i++) {
        const index = first + i;
        admitted.push({ index, plannedMs: this.startMs + index * this.intervalMs });
      }
      dropped[unavailable ? 'connectionUnavailable' : 'pendingLimit'] = considered - count;
    }
    this.nextIndex = dueEnd;
    return { scheduled, admitted, dropped };
  }
}

/** Fair least-pending choice, respecting the broker's hard per-socket operation cap. */
export function chooseConnection(pending, healthy, startIndex = 0, perConnectionLimit = 32) {
  if (pending.length !== healthy.length) throw new RangeError('Mismatched connection arrays');
  let chosen = -1, minimum = perConnectionLimit;
  for (let offset = 0; offset < pending.length; offset++) {
    const index = (startIndex + offset) % pending.length;
    if (healthy[index] && pending[index] < minimum) { chosen = index; minimum = pending[index]; }
  }
  return chosen;
}
