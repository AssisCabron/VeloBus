import { createConnection, type Socket } from 'node:net';
import {
  FrameDecoder, Reader, ProtocolError, MAX_FRAME, MAX_PAYLOAD, MAX_FETCH_BYTES, MAX_U64,
  decodeUTF8, envelope, integer, stringBytes, topicBytes,
} from './protocol.js';

export { ProtocolError } from './protocol.js';

export interface ConnectOptions {
  host?: string;
  port?: number;
  token?: string;
  /** Applies to connection establishment and to every queued request. Default 5000. */
  timeoutMs?: number;
  /** Includes requests queued by TCP backpressure. Default 32, maximum 256. */
  maxPendingRequests?: number;
}

export interface PublishOptions { key?: string }
export interface PublishRecord extends PublishOptions { topic: string; payload: Uint8Array | string }
export interface PublishReceipt {
  firstSequence: bigint;
  lastSequence: bigint;
  count: number;
  durable: boolean;
}
export interface FetchOptions {
  after?: bigint;
  limit?: number;
  maxBytes?: number;
  mode?: 'all' | 'latest';
}
export interface SubscribeOptions extends FetchOptions { pollIntervalMs?: number; signal?: AbortSignal }
export interface FetchResult { cursor: bigint; events: Event[]; hasMore: boolean }
export interface RequestOptions {
  /** Broker deadline including queue wait. Default 5000ms, maximum 30000ms. */
  timeoutMs?: number;
}
export interface HandleOptions {
  /** Worker slots on a dedicated connection. Default 8, maximum 32. */
  concurrency?: number;
  /** Per-route waiting requests. Replicas must agree. Default 128, maximum 4096. */
  queueLimit?: number;
  /** Terminal worker transport/protocol errors. Handler exceptions go to the caller. */
  onError?: (error: Error) => void;
}
export interface ServiceHandle {
  readonly route: string;
  /** Stops intake, aborts active signals and disconnects without awaiting uncooperative handlers. */
  close(): Promise<void>;
}
export interface RpcRouteStats {
  name: string;
  workers: number;
  concurrency: number;
  running: number;
  queued: number;
  queueLimit: number;
}
export interface RpcStats {
  queued: number;
  running: number;
  retainedBytes: number;
  maxCalls: number;
  maxBytes: number;
  accepted: number;
  completed: number;
  rejected: number;
  timedOut: number;
  routes: RpcRouteStats[];
}
export interface BrokerStats {
  version: string;
  mode: 'memory' | 'disk';
  records: number;
  retainedBytes: number;
  maxRetainedBytes: number;
  walBytes: number;
  maxWalBytes: number;
  lastSequence: string;
  publishedRecords: number;
  fetchedRecords: number;
  coalescedRecords: number;
  connections: number;
  uptimeSeconds?: number;
  /** Present on brokers supporting request/reply (v0.2+). */
  rpc?: RpcStats;
}

export class RpcResponse {
  constructor(public readonly payload: Buffer) {}
  text(): string { return this.payload.toString('utf8'); }
  json<T = unknown>(): T { return JSON.parse(this.text()) as T; }
}

export class RpcRequest extends RpcResponse {
  constructor(payload: Buffer, public readonly signal: AbortSignal, public readonly remainingMs: number) {
    super(payload);
  }
}

export type RequestHandler = (request: RpcRequest) => Uint8Array | string | Promise<Uint8Array | string>;

export class Event {
  constructor(
    public readonly sequence: bigint,
    public readonly topic: string,
    public readonly key: string,
    public readonly payload: Buffer,
  ) {}
  text(): string { return this.payload.toString('utf8'); }
  json<T = unknown>(): T { return JSON.parse(this.text()) as T; }
}

export class BrokerError extends Error {
  constructor(public readonly code: number, message: string) { super(message); this.name = 'BrokerError'; }
}
export class ConnectionClosedError extends Error {
  constructor(message = 'Nodara connection is closed; unfinished operations may have an unknown outcome') {
    super(message); this.name = 'ConnectionClosedError';
  }
}
export class RequestTimeoutError extends Error {
  constructor(message = 'Nodara transport request timed out; connection closed and operation outcome may be unknown') {
    super(message); this.name = 'RequestTimeoutError';
  }
}
export class BackpressureError extends Error {
  constructor() { super('Maximum pending requests reached; await an outstanding request before sending more'); this.name = 'BackpressureError'; }
}

interface Pending {
  opcode: number;
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
  parse: (body: Buffer) => unknown;
  timer: NodeJS.Timeout;
}

export class Client {
  /** Transport state; false is not a guarantee that the next network operation will succeed. */
  get isClosed(): boolean { return this.closed; }

  onDisconnect(listener: (error: Error) => void): () => void {
    if (typeof listener !== 'function') throw new TypeError('listener must be a function');
    let active = true;
    const observer = (error: Error) => { if (active) { try { listener(error); } catch (failure) { process.emitWarning(asError(failure)); } } };
    if (this.closed) queueMicrotask(() => observer(this.closeReason));
    else this.failureObservers.add(observer);
    return () => { active = false; this.failureObservers.delete(observer); };
  }
  private readonly pending = new Map<number, Pending>();
  private readonly writeQueue: Buffer[] = [];
  private blocked = false;
  private closed = false;
  private nextRequestId = 1;
  private closeReason: Error = new ConnectionClosedError();
  private readonly decoder: FrameDecoder;
  private readonly closePromise: Promise<void>;
  private resolveClose!: () => void;
  private rejectConnect: ((reason: unknown) => void) | undefined;
  private readonly services = new Set<ServiceHandle>();
  private readonly childClients = new Set<Client>();
  private readonly failureObservers = new Set<(reason: Error) => void>();

  private constructor(
    private readonly socket: Socket,
    private readonly timeoutMs: number,
    private readonly maxPendingRequests: number,
    private readonly connectionOptions: Required<ConnectOptions>,
  ) {
    this.closePromise = new Promise(resolve => { this.resolveClose = resolve; });
    this.decoder = new FrameDecoder(frame => this.receive(frame));
    socket.setNoDelay(true);
    socket.on('data', chunk => {
      if (this.closed) return;
      try { this.decoder.push(chunk); }
      catch (error) { this.fail(asError(error)); }
    });
    socket.on('error', error => this.fail(error));
    socket.on('end', () => this.fail(new ConnectionClosedError('Nodara peer ended the connection; publication outcome may be unknown')));
    socket.on('close', () => { this.fail(this.closeReason); this.resolveClose(); });
    socket.on('drain', () => { this.blocked = false; this.flush(); });
  }

  static async connect(options: ConnectOptions = {}): Promise<Client> {
    return Client.open(options);
  }

  private static async open(options: ConnectOptions, onCreated?: (client: Client) => void): Promise<Client> {
    const host = options.host ?? '127.0.0.1';
    if (typeof host !== 'string' || host.length === 0) throw new TypeError('host must be a nonempty string');
    const port = integer(options.port ?? 7447, 1, 65_535, 'port');
    const timeoutMs = integer(options.timeoutMs ?? 5000, 1, 2_147_483_647, 'timeoutMs');
    const maxPending = integer(options.maxPendingRequests ?? 32, 1, 256, 'maxPendingRequests');
    const token = stringBytes(options.token ?? '', 65_535, 'token');
    const socket = createConnection({ host, port });
    const client = new Client(socket, timeoutMs, maxPending, {
      host, port, token: options.token ?? '', timeoutMs, maxPendingRequests: maxPending,
    });
    onCreated?.(client);
    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => client.fail(new RequestTimeoutError('Nodara connection establishment timed out')), timeoutMs);
        client.rejectConnect = reason => { clearTimeout(timer); reject(reason); };
        socket.once('connect', () => {
          clearTimeout(timer);
          client.rejectConnect = undefined;
          resolve();
        });
      });
      const hello = Buffer.allocUnsafe(4 + token.length);
      hello.writeUInt16BE(1, 0);
      hello.writeUInt16BE(token.length, 2);
      token.copy(hello, 4);
      await client.send(1, hello, body => {
        const reader = new Reader(body);
        if (reader.u16() !== 1) throw new ProtocolError('Unsupported server protocol version');
        reader.end();
      });
      return client;
    } catch (error) {
      client.fail(asError(error));
      throw error;
    }
  }

  /** Await a backend response without blocking the Node.js event loop. No implicit retry. */
  async request(route: string, payload: Uint8Array | string, options: RequestOptions = {}): Promise<RpcResponse> {
    this.ensureCapacity();
    const encodedRoute = topicBytes(route);
    const deadlineMs = integer(options.timeoutMs ?? 5000, 1, 30000, 'timeoutMs');
    const encodedPayload = payloadBytes(payload);
    const body = Buffer.allocUnsafe(10 + encodedRoute.length + encodedPayload.length);
    body.writeUInt16BE(encodedRoute.length, 0);
    encodedRoute.copy(body, 2);
    let offset = 2 + encodedRoute.length;
    body.writeUInt32BE(deadlineMs, offset); offset += 4;
    body.writeUInt32BE(encodedPayload.length, offset); offset += 4;
    body.set(encodedPayload, offset);
    return this.send(7, body, response => {
      const reader = new Reader(response);
      const result = new RpcResponse(reader.payload());
      reader.end();
      return result;
    }, Math.max(this.timeoutMs, deadlineMs + 1000));
  }

  async requestJSON<T = unknown>(route: string, input: unknown, options: RequestOptions = {}): Promise<T> {
    const payload = JSON.stringify(input);
    if (payload === undefined) throw new TypeError('Value cannot be represented as JSON');
    return (await this.request(route, payload, options)).json<T>();
  }

  async handle(route: string, handler: RequestHandler, options: HandleOptions = {}): Promise<ServiceHandle> {
    this.ensureOpen();
    const encodedRoute = topicBytes(route);
    if (typeof handler !== 'function') throw new TypeError('handler must be a function');
    const concurrency = integer(options.concurrency ?? 8, 1, 32, 'concurrency');
    const queueLimit = integer(options.queueLimit ?? 128, 1, 4096, 'queueLimit');
    if (options.onError !== undefined && typeof options.onError !== 'function') throw new TypeError('onError must be a function');
    const onError = options.onError;
    const child = await Client.open({ ...this.connectionOptions, maxPendingRequests: concurrency }, created => {
      // Track the socket before HELLO resolves so parent.close also cancels service setup.
      this.childClients.add(created);
      created.failureObservers.add(() => this.childClients.delete(created));
    });
    let stopped = false;
    let closing: Promise<void> | undefined;
    const active = new Map<AbortController, NodeJS.Timeout>();
    const report = (error: Error): void => {
      if (!onError) { process.emitWarning(error); return; }
      try { onError(error); }
      catch (callbackError) { process.emitWarning(asError(callbackError)); }
    };
    const service: ServiceHandle = {
      route,
      close: () => {
        if (stopped) return closing ?? Promise.resolve();
        stopped = true;
        this.services.delete(service);
        child.failureObservers.delete(onFailure);
        for (const [controller, timer] of active) {
          clearTimeout(timer);
          controller.abort(new ConnectionClosedError('Nodara service closed'));
        }
        active.clear();
        closing = child.close();
        return closing;
      },
    };
    const onFailure = (error: Error): void => {
      if (stopped) return;
      void service.close().catch(report);
      report(error);
    };
    try {
      this.ensureOpen();
      this.services.add(service);
      const registration = Buffer.allocUnsafe(6 + encodedRoute.length);
      registration.writeUInt16BE(encodedRoute.length, 0);
      encodedRoute.copy(registration, 2);
      registration.writeUInt16BE(concurrency, 2 + encodedRoute.length);
      registration.writeUInt16BE(queueLimit, 4 + encodedRoute.length);
      await child.send(6, registration, response => { new Reader(response).end(); });
      this.ensureOpen();
      child.ensureOpen();
      child.failureObservers.add(onFailure);
      const run = async (): Promise<void> => {
        while (!stopped) {
          const job = await child.take();
          if (stopped) return;
          if (!job) continue;
          const controller = new AbortController();
          const timer = setTimeout(() => {
            controller.abort(new BrokerError(10, 'Handler deadline exceeded; execution slot remains occupied until completion'));
          }, job.remainingMs);
          active.set(controller, timer);
          if (job.remainingMs === 0) controller.abort(new BrokerError(10, 'Handler deadline exceeded'));
          let status = 0;
          let output: Uint8Array;
          try {
            // Cooperative abort is advisory: retain this slot until actual settlement.
            output = payloadBytes(await handler(new RpcRequest(job.payload, controller.signal, job.remainingMs)));
          } catch (error) {
            status = 1;
            output = handlerErrorBytes(error);
          } finally {
            clearTimeout(timer);
            active.delete(controller);
          }
          if (stopped) return;
          await child.complete(job.callId, status, output);
        }
      };
      for (let slot = 0; slot < concurrency; slot++) void run().catch(onFailure);
      return service;
    } catch (error) {
      await service.close();
      throw error;
    }
  }

  async handleJSON<TInput = unknown, TOutput = unknown>(
    route: string,
    handler: (body: TInput, context: RpcRequest) => TOutput | Promise<TOutput>,
    options: HandleOptions = {},
  ): Promise<ServiceHandle> {
    if (typeof handler !== 'function') throw new TypeError('handler must be a function');
    return this.handle(route, async request => {
      const value = await handler(request.json<TInput>(), request);
      const payload = JSON.stringify(value);
      if (payload === undefined) throw new TypeError('Handler result cannot be represented as JSON');
      return payload;
    }, options);
  }

  private async take(): Promise<{ callId: bigint; remainingMs: number; payload: Buffer } | undefined> {
    const body = Buffer.allocUnsafe(4);
    body.writeUInt32BE(1000);
    return this.send(8, body, response => {
      const reader = new Reader(response);
      if (!reader.bool()) { reader.end(); return undefined; }
      const callId = reader.u64();
      const remainingMs = reader.u32();
      const payload = reader.payload();
      reader.end();
      if (callId === 0n || remainingMs > 30000) throw new ProtocolError('Invalid worker call ID or remaining deadline');
      return { callId, remainingMs, payload };
    }, Math.max(this.timeoutMs, 2000));
  }

  private async complete(callId: bigint, status: number, payload: Uint8Array): Promise<void> {
    const body = Buffer.allocUnsafe(13 + payload.length);
    body.writeBigUInt64BE(callId, 0);
    body.writeUInt8(status, 8);
    body.writeUInt32BE(payload.length, 9);
    body.set(payload, 13);
    return this.send(9, body, response => { new Reader(response).end(); });
  }

  async publish(topic: string, payload: Uint8Array | string, options: PublishOptions = {}): Promise<PublishReceipt> {
    return this.publishBatch([{ topic, payload, key: options.key }]);
  }

  async publishJSON(topic: string, value: unknown, options: PublishOptions = {}): Promise<PublishReceipt> {
    const payload = JSON.stringify(value);
    if (payload === undefined) throw new TypeError('Value cannot be represented as JSON');
    return this.publish(topic, payload, options);
  }

  async publishBatch(records: readonly PublishRecord[]): Promise<PublishReceipt> {
    this.ensureCapacity();
    if (!Array.isArray(records)) throw new TypeError('records must be an array');
    integer(records.length, 1, 256, 'records.length');
    let size = 2;
    const encoded = records.map(record => {
      const topic = topicBytes(record.topic);
      const key = stringBytes(record.key ?? '', 1024, 'key');
      const payload = typeof record.payload === 'string' ? Buffer.from(record.payload) : record.payload;
      if (!(payload instanceof Uint8Array)) throw new TypeError('payload must be a string or Uint8Array');
      if (payload.byteLength > MAX_PAYLOAD) throw new RangeError(`payload exceeds ${MAX_PAYLOAD} bytes`);
      size += 2 + topic.length + 2 + key.length + 4 + payload.byteLength;
      if (size + 5 > MAX_FRAME) throw new RangeError('Publication batch exceeds frame size limit');
      return { topic, key, payload };
    });
    const body = Buffer.allocUnsafe(size);
    const expectedCount = encoded.length;
    body.writeUInt16BE(expectedCount, 0);
    let offset = 2;
    for (const { topic, key, payload } of encoded) {
      body.writeUInt16BE(topic.length, offset); offset += 2;
      topic.copy(body, offset); offset += topic.length;
      body.writeUInt16BE(key.length, offset); offset += 2;
      key.copy(body, offset); offset += key.length;
      body.writeUInt32BE(payload.byteLength, offset); offset += 4;
      body.set(payload, offset); offset += payload.byteLength;
    }
    return this.send(2, body, response => {
      const reader = new Reader(response);
      const firstSequence = reader.u64();
      const lastSequence = reader.u64();
      const count = reader.u16();
      const durable = reader.bool();
      reader.end();
      if (count !== expectedCount || firstSequence === 0n || lastSequence !== firstSequence + BigInt(count) - 1n) {
        throw new ProtocolError('Invalid publication sequence range or record count');
      }
      return { firstSequence, lastSequence, count, durable };
    });
  }

  async fetch(topic: string, options: FetchOptions = {}): Promise<FetchResult> {
    this.ensureCapacity();
    const encodedTopic = topicBytes(topic, true);
    const after = options.after ?? 0n;
    if (typeof after !== 'bigint' || after < 0n || after > MAX_U64) throw new RangeError('after must be an unsigned 64-bit bigint');
    const limit = integer(options.limit ?? 128, 1, 256, 'limit');
    const maxBytes = integer(options.maxBytes ?? MAX_FETCH_BYTES, 1, MAX_FETCH_BYTES, 'maxBytes');
    const mode = options.mode ?? 'all';
    if (mode !== 'all' && mode !== 'latest') throw new TypeError('mode must be all or latest');
    const body = Buffer.allocUnsafe(17 + encodedTopic.length);
    body.writeUInt16BE(encodedTopic.length, 0);
    encodedTopic.copy(body, 2);
    let offset = 2 + encodedTopic.length;
    body.writeBigUInt64BE(after, offset); offset += 8;
    body.writeUInt16BE(limit, offset); offset += 2;
    body.writeUInt32BE(maxBytes, offset); offset += 4;
    body.writeUInt8(mode === 'latest' ? 1 : 0, offset);
    return this.send(3, body, response => {
      const reader = new Reader(response);
      const cursor = reader.u64();
      const count = reader.u16();
      const hasMore = reader.bool();
      if (cursor < after || count > limit || response.length - 11 > maxBytes || hasMore && cursor === after) {
        throw new ProtocolError('Invalid fetch cursor, count, progress or payload budget');
      }
      const events: Event[] = [];
      const keys = new Set<string>();
      let previous = after;
      for (let i = 0; i < count; i++) {
        const sequence = reader.u64();
        const eventTopic = reader.string(255);
        const key = reader.string(1024);
        const payload = reader.payload();
        if (sequence <= previous || sequence > cursor || !/^[A-Za-z0-9._-]{1,255}$/.test(eventTopic) || topic !== '*' && eventTopic !== topic) {
          throw new ProtocolError('Invalid fetch event sequence or topic');
        }
        if (mode === 'latest' && key !== '') {
          const identity = `${eventTopic}\0${key}`;
          if (keys.has(identity)) throw new ProtocolError('Duplicate keyed event in latest response');
          keys.add(identity);
        }
        previous = sequence;
        events.push(new Event(sequence, eventTopic, key, payload));
      }
      reader.end();
      return { cursor, events, hasMore };
    });
  }

  /** Pulls one bounded batch at a time. Use fetch() for durable whole-batch checkpoints. */
  async *subscribe(topic: string, options: SubscribeOptions = {}): AsyncGenerator<Event, void, unknown> {
    const pollInterval = integer(options.pollIntervalMs ?? 100, 1, 2_147_483_647, 'pollIntervalMs');
    let cursor = options.after ?? 0n;
    try {
      while (true) {
        throwIfAborted(options.signal);
        const batch = await abortable(this.fetch(topic, { ...options, after: cursor }), options.signal);
        for (const event of batch.events) {
          throwIfAborted(options.signal);
          yield event;
        }
        cursor = batch.cursor;
        // Empty topic scans also sleep: sparse topics must not become a busy polling loop.
        if (!batch.hasMore || batch.events.length === 0) await pause(pollInterval, options.signal);
      }
    } catch (error) {
      if (!options.signal?.aborted) throw error;
    }
  }

  async stats(): Promise<BrokerStats> {
    return this.send(4, Buffer.alloc(0), response => {
      let stats: unknown;
      try { stats = JSON.parse(decodeUTF8(response)); }
      catch { throw new ProtocolError('Invalid stats JSON'); }
      if (!stats || typeof stats !== 'object' || Array.isArray(stats)) throw new ProtocolError('Invalid stats object');
      const object = stats as Record<string, unknown>;
      if (typeof object.version !== 'string' || object.mode !== 'memory' && object.mode !== 'disk' ||
          typeof object.lastSequence !== 'string' || !/^(0|[1-9][0-9]*)$/.test(object.lastSequence) || BigInt(object.lastSequence) > MAX_U64) {
        throw new ProtocolError('Invalid stats version, mode or lastSequence');
      }
      for (const field of ['records', 'retainedBytes', 'maxRetainedBytes', 'walBytes', 'maxWalBytes', 'publishedRecords', 'fetchedRecords', 'coalescedRecords', 'connections']) {
        if (typeof object[field] !== 'number' || !Number.isSafeInteger(object[field]) || object[field] < 0) {
          throw new ProtocolError(`Invalid stats field: ${field}`);
        }
      }
      if (object.uptimeSeconds !== undefined && (typeof object.uptimeSeconds !== 'number' || !Number.isFinite(object.uptimeSeconds) || object.uptimeSeconds < 0)) {
        throw new ProtocolError('Invalid stats uptimeSeconds');
      }
      if (object.rpc !== undefined) validateRpcStats(object.rpc);
      return stats as BrokerStats;
    });
  }

  async ping(): Promise<void> {
    return this.send(5, Buffer.alloc(0), response => { new Reader(response).end(); });
  }

  /** Closes immediately and rejects pending work. Await publication receipts before close(). */
  async close(): Promise<void> {
    const children = [...this.services];
    const childClients = [...this.childClients];
    this.fail(new ConnectionClosedError());
    await Promise.all([this.closePromise, ...children.map(service => service.close()), ...childClients.map(child => child.close())]);
  }

  private ensureOpen(): void { if (this.closed) throw this.closeReason; }

  private ensureCapacity(): void {
    this.ensureOpen();
    if (this.pending.size >= this.maxPendingRequests) throw new BackpressureError();
  }

  private send<T>(opcode: number, body: Buffer, parse: (body: Buffer) => T, timeoutMs = this.timeoutMs): Promise<T> {
    try { this.ensureCapacity(); } catch (error) { return Promise.reject(error); }
    let id = this.nextRequestId;
    while (this.pending.has(id)) id = id === 0xffff_ffff ? 1 : id + 1;
    this.nextRequestId = id === 0xffff_ffff ? 1 : id + 1;
    let frame: Buffer;
    try { frame = envelope(opcode, id, body); } catch (error) { return Promise.reject(error); }
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => this.fail(new RequestTimeoutError()), timeoutMs);
      this.pending.set(id, { opcode, resolve: value => resolve(value as T), reject, parse, timer });
      this.writeQueue.push(frame);
      this.flush();
    });
  }

  private flush(): void {
    while (!this.closed && !this.blocked && this.writeQueue.length > 0) {
      const frame = this.writeQueue.shift()!;
      try { this.blocked = !this.socket.write(frame); }
      catch (error) { this.fail(asError(error)); }
    }
  }

  private receive(frame: Buffer): void {
    if (this.closed) return;
    const opcode = frame.readUInt8(0);
    const id = frame.readUInt32BE(1);
    const pending = this.pending.get(id);
    if (id === 0 || !pending) throw new ProtocolError('Response has an unknown request ID');
    if (opcode !== 0xff && opcode !== (pending.opcode | 0x80)) throw new ProtocolError('Response opcode does not match request');
    const body = frame.subarray(5);
    if (opcode === 0xff) {
      const reader = new Reader(body);
      const code = reader.u16();
      const message = reader.string();
      reader.end();
      if (code < 1 || code > 12) throw new ProtocolError('Unknown broker error code');
      this.pending.delete(id);
      clearTimeout(pending.timer);
      pending.reject(new BrokerError(code, message));
      return;
    }
    // Keep the request registered until parsing succeeds, so malformed replies reject it too.
    const value = pending.parse(body);
    this.pending.delete(id);
    clearTimeout(pending.timer);
    pending.resolve(value);
  }

  private fail(reason: Error): void {
    if (this.closed) return;
    this.closed = true;
    this.closeReason = reason;
    this.rejectConnect?.(reason);
    this.rejectConnect = undefined;
    for (const pending of this.pending.values()) { clearTimeout(pending.timer); pending.reject(reason); }
    this.pending.clear();
    this.writeQueue.length = 0;
    this.socket.destroy();
    for (const service of this.services) void service.close().catch(error => process.emitWarning(asError(error)));
    for (const child of this.childClients) void child.close().catch(error => process.emitWarning(asError(error)));
    for (const observer of this.failureObservers) observer(reason);
    this.failureObservers.clear();
  }
}

export async function connect(options: ConnectOptions = {}): Promise<Client> { return Client.connect(options); }

function asError(error: unknown): Error { return error instanceof Error ? error : new Error(String(error)); }
function payloadBytes(payload: Uint8Array | string): Uint8Array {
  const bytes = typeof payload === 'string' ? Buffer.from(payload, 'utf8') : payload;
  if (!(bytes instanceof Uint8Array)) throw new TypeError('payload must be a string or Uint8Array');
  if (bytes.length > MAX_PAYLOAD) throw new RangeError(`payload exceeds ${MAX_PAYLOAD} bytes`);
  return bytes;
}
function handlerErrorBytes(error: unknown): Buffer {
  const message = error instanceof Error ? error.message : String(error);
  const bytes = Buffer.from(message.slice(0, 4096), 'utf8');
  if (bytes.length <= 4096) return bytes;
  let end = 4096;
  // Keep the handler error valid UTF-8 when truncating a multibyte character.
  while ((bytes[end]! & 0xc0) === 0x80) end--;
  return bytes.subarray(0, end);
}
function validateRpcStats(value: unknown): asserts value is RpcStats {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new ProtocolError('Invalid rpc stats object');
  const stats = value as Record<string, unknown>;
  const checkCount = (object: Record<string, unknown>, field: string): void => {
    if (typeof object[field] !== 'number' || !Number.isSafeInteger(object[field]) || object[field] < 0) {
      throw new ProtocolError(`Invalid rpc stats field: ${field}`);
    }
  };
  for (const field of ['queued', 'running', 'retainedBytes', 'maxCalls', 'maxBytes', 'accepted', 'completed', 'rejected', 'timedOut']) checkCount(stats, field);
  if (!Array.isArray(stats.routes)) throw new ProtocolError('Invalid rpc routes');
  for (const route of stats.routes) {
    if (!route || typeof route !== 'object' || Array.isArray(route) || typeof route.name !== 'string' || !/^[A-Za-z0-9._-]{1,255}$/.test(route.name)) {
      throw new ProtocolError('Invalid rpc route');
    }
    for (const field of ['workers', 'concurrency', 'running', 'queued', 'queueLimit']) checkCount(route, field);
  }
}
function abortReason(signal: AbortSignal): unknown { return signal.reason ?? new DOMException('Operation aborted', 'AbortError'); }
function throwIfAborted(signal?: AbortSignal): void { if (signal?.aborted) throw abortReason(signal); }
function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return promise;
  // Always observe the in-flight request, including an abort before registration.
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(abortReason(signal));
    if (signal.aborted) onAbort();
    else signal.addEventListener('abort', onAbort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', onAbort));
  });
}

export { connectCluster, ClusterClient, ClusterUnavailableError, AmbiguousResultError } from './cluster.js';
export type { BrokerEndpoint, ClusterOptions, ClusterNodeStatus, ClusterServiceHandle } from './cluster.js';
function pause(ms: number, signal?: AbortSignal): Promise<void> {
  throwIfAborted(signal);
  return new Promise((resolve, reject) => {
    const done = () => { signal?.removeEventListener('abort', onAbort); resolve(); };
    const timer = setTimeout(done, ms);
    const onAbort = () => { clearTimeout(timer); reject(abortReason(signal!)); };
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}
