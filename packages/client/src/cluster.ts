import { performance } from 'node:perf_hooks';
import { Client, BrokerError, BackpressureError, RpcRequest, type RpcResponse, type RequestOptions, type RequestHandler, type HandleOptions, type ServiceHandle } from './index.js';
import { integer, topicBytes, stringBytes, MAX_PAYLOAD } from './protocol.js';

export interface BrokerEndpoint { id?: string; host: string; port?: number; token?: string }
export interface ClusterOptions {
  /** Explicit independent brokers, maximum 8. No peer discovery or state replication. */
  brokers: BrokerEndpoint[];
  token?: string;
  /** Connect/health transport timeout, default 1000ms. */
  timeoutMs?: number;
  healthIntervalMs?: number;
  reconnectMinMs?: number;
  reconnectMaxMs?: number;
  /** Aggregate user requests across this client; default 256. */
  maxPendingRequests?: number;
}
export interface ClusterNodeStatus {
  id: string; host: string; port: number; connected: boolean; pending: number;
  reconnectFailures: number; lastError: string | null;
}
export interface ClusterServiceHandle extends ServiceHandle {
  /** Registered brokers at this instant, without credentials. */
  nodes(): string[];
}
export class ClusterUnavailableError extends Error {
  readonly outcome = 'not-sent';
  constructor(message = 'No connected broker is available; request was not sent') { super(message); this.name = 'ClusterUnavailableError'; }
}
export class AmbiguousResultError extends Error {
  readonly outcome = 'unknown';
  constructor(public readonly brokerId: string, cause: unknown) {
    super(`Broker ${brokerId} lost the response; the operation may have executed. It was not replayed.`, { cause });
    this.name = 'AmbiguousResultError';
  }
}
interface NodeState {
  endpoint: Required<BrokerEndpoint>; client?: Client; connecting?: Promise<void>;
  pending: number; failures: number; retryAt: number; lastError: string | null;
  unsubscribe?: () => void;
}

// One shared gate survives node reconnects. An uncooperative old handler keeps its
// permit until it actually settles, even when its broker/connection has disappeared.
class HandlerGate {
  active = 0;
  private waiters: Array<{ signal: AbortSignal; start: () => void; abort: () => void }> = [];
  constructor(private readonly limit: number, private readonly maxWaiting: number) {}
  acquire(signal: AbortSignal): Promise<() => void> {
    if (signal.aborted) return Promise.reject(signal.reason);
    if (this.active < this.limit) { this.active++; return Promise.resolve(this.releaseOnce()); }
    if (this.waiters.length >= this.maxWaiting) return Promise.reject(new Error('Cluster worker admission bound exceeded'));
    return new Promise((resolve, reject) => {
      const waiter = {
        signal,
        start: () => { signal.removeEventListener('abort', waiter.abort); this.active++; resolve(this.releaseOnce()); },
        abort: () => { const index = this.waiters.indexOf(waiter); if (index >= 0) this.waiters.splice(index, 1); reject(signal.reason); },
      };
      signal.addEventListener('abort', waiter.abort, { once: true });
      this.waiters.push(waiter);
    });
  }
  private releaseOnce(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true; this.active--;
      while (this.waiters.length && this.active < this.limit) {
        const waiter = this.waiters.shift()!;
        if (waiter.signal.aborted) { waiter.signal.removeEventListener('abort', waiter.abort); waiter.abort(); }
        else waiter.start();
      }
    };
  }
}

class ClusterService implements ClusterServiceHandle {
  private stopped = false;
  private handles = new Map<NodeState, ServiceHandle>();
  private attaching = new Map<NodeState, Promise<void>>();
  private retryAt = new Map<NodeState, number>();
  private epochs = new Map<NodeState, object>();
  private readonly gate: HandlerGate;
  private closing?: Promise<void>;
  constructor(
    readonly route: string, private readonly pool: ClusterClient,
    private readonly handler: RequestHandler, private readonly options: HandleOptions,
  ) { this.gate = new HandlerGate(options.concurrency!, pool.nodeCount * options.concurrency!); }
  nodes(): string[] { return [...this.handles.keys()].filter(node => node.client && !node.client.isClosed).map(node => node.endpoint.id); }
  detach(node: NodeState): void {
    this.epochs.delete(node);
    const handle = this.handles.get(node); this.handles.delete(node);
    if (handle) void handle.close().catch(() => {});
  }
  attach(node: NodeState): Promise<void> {
    if (this.stopped || this.handles.has(node) || performance.now() < (this.retryAt.get(node) ?? 0)) return Promise.resolve();
    const existing = this.attaching.get(node); if (existing) return existing;
    const client = node.client;
    if (!client || client.isClosed) return Promise.resolve();
    const epoch = {}; this.epochs.set(node, epoch);
    const operation = (async () => {
      const handle = await client.handle(this.route, async request => {
        const started = performance.now();
        const release = await this.gate.acquire(request.signal);
        try {
          if (request.signal.aborted) throw request.signal.reason;
          const remaining = Math.max(0, request.remainingMs - Math.ceil(performance.now() - started));
          if (!remaining) throw new Error('Deadline expired before local handler admission');
          return await this.handler(new RpcRequest(request.payload, request.signal, remaining));
        } finally { release(); }
      }, { ...this.options, onError: error => {
        if (this.epochs.get(node) !== epoch || this.stopped) return;
        this.detach(node);
        this.retryAt.set(node, performance.now() + this.pool.healthIntervalMs);
        try { this.options.onError?.(error); } catch (failure) { process.emitWarning(String(failure)); }
      } });
      if (this.stopped || this.epochs.get(node) !== epoch || node.client !== client || client.isClosed) { await handle.close(); return; }
      this.handles.set(node, handle); this.retryAt.delete(node);
    })().catch(error => {
      this.retryAt.set(node, performance.now() + this.pool.healthIntervalMs);
      throw error;
    }).finally(() => { this.attaching.delete(node); });
    this.attaching.set(node, operation);
    return operation;
  }
  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.stopped = true; this.pool.forget(this);
    this.closing = (async () => {
      await Promise.allSettled([...this.handles.values()].map(handle => handle.close()));
      this.handles.clear();
      await Promise.allSettled([...this.attaching.values()]);
    })();
    return this.closing;
  }
}

export class ClusterClient {
  private readonly states: NodeState[];
  private readonly services = new Set<ClusterService>();
  private readonly timeoutMs: number;
  readonly healthIntervalMs: number;
  private readonly reconnectMinMs: number;
  private readonly reconnectMaxMs: number;
  private readonly maxPending: number;
  private pending = 0;
  private cursor = 0;
  private stopped = false;
  private timer?: NodeJS.Timeout;
  private ticking?: Promise<void>;
  private closing?: Promise<void>;
  get nodeCount(): number { return this.states.length; }
  private constructor(options: ClusterOptions) {
    if (!Array.isArray(options?.brokers) || !options.brokers.length || options.brokers.length > 8) throw new RangeError('brokers must contain 1..8 explicit endpoints');
    this.timeoutMs = integer(options.timeoutMs ?? 1000, 50, 30000, 'timeoutMs');
    this.healthIntervalMs = integer(options.healthIntervalMs ?? 500, 50, 60000, 'healthIntervalMs');
    this.reconnectMinMs = integer(options.reconnectMinMs ?? 100, 50, 60000, 'reconnectMinMs');
    this.reconnectMaxMs = integer(options.reconnectMaxMs ?? 5000, this.reconnectMinMs, 60000, 'reconnectMaxMs');
    this.maxPending = integer(options.maxPendingRequests ?? 256, 1, 256, 'maxPendingRequests');
    const ids = new Set<string>(); const addresses = new Set<string>();
    this.states = options.brokers.map(endpoint => {
      if (!endpoint || typeof endpoint.host !== 'string' || !endpoint.host.trim()) throw new TypeError('Every broker requires a host');
      const port = integer(endpoint.port ?? 7447, 1, 65535, 'port');
      const address = `${endpoint.host.toLowerCase()}:${port}`;
      const id = endpoint.id ?? address;
      if (typeof id !== 'string' || !id.length || id.length > 255 || ids.has(id) || addresses.has(address)) throw new TypeError('Broker IDs and addresses must be unique, with IDs of 1..255 characters');
      const token = endpoint.token ?? options.token ?? ''; stringBytes(token, 65535, 'token');
      ids.add(id); addresses.add(address);
      return { endpoint: { ...endpoint, id, port, token }, pending: 0, failures: 0, retryAt: 0, lastError: null };
    });
  }
  static async connect(options: ClusterOptions): Promise<ClusterClient> {
    const pool = new ClusterClient(options);
    await Promise.allSettled(pool.states.map(node => pool.connectNode(node)));
    if (!pool.states.some(node => node.client && !node.client.isClosed)) {
      await pool.close(); throw new ClusterUnavailableError('Could not connect to any configured broker');
    }
    pool.timer = setInterval(() => { if (!pool.ticking) pool.ticking = pool.tick().finally(() => { pool.ticking = undefined; }); }, pool.healthIntervalMs);
    pool.timer.unref();
    return pool;
  }
  nodes(): ClusterNodeStatus[] {
    return this.states.map(node => ({ id: node.endpoint.id, host: node.endpoint.host, port: node.endpoint.port,
      connected: Boolean(node.client && !node.client.isClosed), pending: node.pending, reconnectFailures: node.failures, lastError: node.lastError }));
  }
  private retire(node: NodeState, client: Client, error: unknown): void {
    if (node.client !== client) return;
    node.unsubscribe?.(); node.unsubscribe = undefined; node.client = undefined;
    node.lastError = error instanceof Error ? error.message : String(error);
    node.retryAt = performance.now() + this.reconnectMinMs;
    for (const service of this.services) service.detach(node);
    void client.close().catch(() => {});
  }
  private connectNode(node: NodeState): Promise<void> {
    if (this.stopped || node.client && !node.client.isClosed) return Promise.resolve();
    if (node.connecting) return node.connecting;
    const attempt = (async () => {
      const client = await Client.connect({ ...node.endpoint, timeoutMs: this.timeoutMs, maxPendingRequests: 32 });
      if (this.stopped) { await client.close(); return; }
      node.client = client; node.failures = 0; node.lastError = null;
      node.unsubscribe = client.onDisconnect(error => this.retire(node, client, error));
      await Promise.allSettled([...this.services].map(service => service.attach(node)));
    })().catch(error => {
      node.failures++; node.lastError = error instanceof Error ? error.message : String(error);
      const backoff = Math.min(this.reconnectMaxMs, this.reconnectMinMs * 2 ** Math.min(node.failures - 1, 16));
      node.retryAt = performance.now() + backoff * (0.8 + Math.random() * 0.2);
    }).finally(() => { node.connecting = undefined; });
    node.connecting = attempt;
    return attempt;
  }
  private async tick(): Promise<void> {
    if (this.stopped) return;
    await Promise.allSettled(this.states.map(async node => {
      const client = node.client;
      if (!client || client.isClosed) {
        if (client) this.retire(node, client, new Error('Connection closed'));
        if (performance.now() >= node.retryAt) await this.connectNode(node);
        return;
      }
      // Count the health operation against the same 32-slot transport budget.
      if (!node.pending) {
        node.pending++;
        try { await client.ping(); } catch (error) { this.retire(node, client, error); }
        finally { node.pending--; }
      }
      await Promise.allSettled([...this.services].map(service => service.attach(node)));
    }));
  }
  private choose(tried: Set<NodeState>): NodeState | undefined {
    let best: NodeState | undefined;
    for (let offset = 0; offset < this.states.length; offset++) {
      const node = this.states[(this.cursor + offset) % this.states.length]!;
      if (!tried.has(node) && node.client && !node.client.isClosed && node.pending < 32 && (!best || node.pending < best.pending)) best = node;
    }
    if (best) this.cursor = (this.states.indexOf(best) + 1) % this.states.length;
    return best;
  }
  async request(route: string, payload: Uint8Array | string, options: RequestOptions = {}): Promise<RpcResponse> {
    if (this.stopped) throw new ClusterUnavailableError('Cluster client is closed; request was not sent');
    topicBytes(route);
    if (typeof payload !== 'string' && !(payload instanceof Uint8Array)) throw new TypeError('payload must be a string or Uint8Array');
    if ((typeof payload === 'string' ? Buffer.byteLength(payload) : payload.byteLength) > MAX_PAYLOAD) throw new RangeError('Payload exceeds 262144 bytes');
    const timeoutMs = integer(options.timeoutMs ?? 5000, 1, 30000, 'timeoutMs');
    if (this.pending >= this.maxPending) throw new BackpressureError();
    const snapshot = Buffer.from(payload);
    const deadline = performance.now() + timeoutMs;
    const tried = new Set<NodeState>(); let lastError: Error | undefined;
    this.pending++;
    try {
      while (tried.size < this.states.length) {
        const node = this.choose(tried);
        if (!node) break;
        const remaining = Math.ceil(deadline - performance.now());
        if (remaining < 1) throw new BrokerError(10, 'Cluster admission deadline exceeded; no further attempt was sent');
        tried.add(node);
        const client = node.client!; node.pending++;
        try { return await client.request(route, snapshot, { timeoutMs: remaining }); }
        catch (error) {
          if (error instanceof BrokerError) {
            if (error.code !== 8 && error.code !== 9) throw error;
            lastError = error; // Explicit rejection before dispatch is safe to try elsewhere.
          } else if (error instanceof BackpressureError) lastError = error;
          else {
            this.retire(node, client, error);
            throw new AmbiguousResultError(node.endpoint.id, error);
          }
        } finally { node.pending--; }
      }
      if (lastError) throw lastError;
      if (this.states.some(node => node.client && !node.client.isClosed)) throw new BackpressureError();
      throw new ClusterUnavailableError();
    } finally { this.pending--; }
  }
  async requestJSON<T = unknown>(route: string, body: unknown, options: RequestOptions = {}): Promise<T> {
    const json = JSON.stringify(body); if (json === undefined) throw new TypeError('Value cannot be represented as JSON');
    return (await this.request(route, json, options)).json<T>();
  }
  async handle(route: string, handler: RequestHandler, options: HandleOptions = {}): Promise<ClusterServiceHandle> {
    if (this.stopped) throw new ClusterUnavailableError('Cluster client is closed');
    topicBytes(route);
    if (typeof handler !== 'function') throw new TypeError('handler must be a function');
    if (options.onError !== undefined && typeof options.onError !== 'function') throw new TypeError('onError must be a function');
    if ([...this.services].some(service => service.route === route)) throw new Error('Route already registered on this cluster client');
    if (this.services.size >= 32) throw new RangeError('A cluster client supports at most 32 service registrations');
    const concurrency = integer(options.concurrency ?? 8, 1, 32, 'concurrency');
    const queueLimit = integer(options.queueLimit ?? 128, 1, 4096, 'queueLimit');
    const service = new ClusterService(route, this, handler, { ...options, concurrency, queueLimit });
    this.services.add(service);
    const results = await Promise.allSettled(this.states.map(node => service.attach(node)));
    if (!service.nodes().length) {
      await service.close();
      const failure = results.find(result => result.status === 'rejected');
      if (failure?.status === 'rejected') throw failure.reason;
      throw new ClusterUnavailableError('No broker accepted the worker registration');
    }
    return service;
  }
  handleJSON<TInput = unknown, TOutput = unknown>(route: string, handler: (body: TInput, context: RpcRequest) => TOutput | Promise<TOutput>, options: HandleOptions = {}): Promise<ClusterServiceHandle> {
    if (typeof handler !== 'function') return Promise.reject(new TypeError('handler must be a function'));
    return this.handle(route, async request => {
      const value = JSON.stringify(await handler(request.json<TInput>(), request));
      if (value === undefined) throw new TypeError('Handler result cannot be represented as JSON');
      return value;
    }, options);
  }
  /** Internal service lifecycle hook. */
  forget(service: ClusterService): void { this.services.delete(service); }
  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.stopped = true; clearInterval(this.timer);
    this.closing = (async () => {
      for (const node of this.states) node.unsubscribe?.();
      await Promise.allSettled([...this.services].map(service => service.close()).concat(this.states.map(node => node.client?.close() ?? Promise.resolve())));
      await Promise.allSettled(this.states.map(node => node.connecting));
      await this.ticking;
      for (const node of this.states) node.client = undefined;
    })();
    return this.closing;
  }
}

export function connectCluster(options: ClusterOptions): Promise<ClusterClient> { return ClusterClient.connect(options); }
