import { TextDecoder } from 'node:util';

export const MAX_FRAME = 1_048_576;
export const MAX_PAYLOAD = 262_144;
export const MAX_FETCH_BYTES = 524_288;
export const MAX_U64 = 0xffff_ffff_ffff_ffffn;
// Preserve an initial U+FEFF: keys are opaque UTF-8 strings, not BOM-prefixed files.
const utf8 = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });

export class ProtocolError extends Error {
  constructor(message: string) { super(message); this.name = 'ProtocolError'; }
}

export function decodeUTF8(bytes: Uint8Array): string {
  try { return utf8.decode(bytes); }
  catch { throw new ProtocolError('Response contains invalid UTF-8'); }
}

export function integer(value: number, min: number, max: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new RangeError(`${name} must be an integer between ${min} and ${max}`);
  }
  return value;
}

export function topicBytes(topic: string, wildcard = false): Buffer {
  if (typeof topic !== 'string' || !(wildcard && topic === '*') && !/^[A-Za-z0-9._-]{1,255}$/.test(topic)) {
    throw new TypeError('topic must contain 1..255 ASCII letters, digits, dots, underscores or hyphens');
  }
  return Buffer.from(topic);
}

export function stringBytes(value: string, max: number, name: string): Buffer {
  if (typeof value !== 'string') throw new TypeError(`${name} must be a string`);
  const bytes = Buffer.from(value, 'utf8');
  if (bytes.length > max || bytes.toString('utf8') !== value) {
    throw new RangeError(`${name} must be valid UTF-8 and at most ${max} bytes`);
  }
  return bytes;
}

export class Reader {
  private offset = 0;
  constructor(private readonly bytes: Buffer) {}
  private take(count: number): Buffer {
    if (count > this.bytes.length - this.offset) throw new ProtocolError('Truncated response body');
    const slice = this.bytes.subarray(this.offset, this.offset + count);
    this.offset += count;
    return slice;
  }
  u8(): number { return this.take(1).readUInt8(0); }
  u16(): number { return this.take(2).readUInt16BE(0); }
  u32(): number { return this.take(4).readUInt32BE(0); }
  u64(): bigint { return this.take(8).readBigUInt64BE(0); }
  string(max = 65_535): string {
    const count = this.u16();
    if (count > max) throw new ProtocolError('Response string exceeds its size limit');
    return decodeUTF8(this.take(count));
  }
  payload(): Buffer {
    const count = this.u32();
    if (count > MAX_PAYLOAD) throw new ProtocolError('Response payload exceeds its size limit');
    return this.take(count);
  }
  bool(): boolean {
    const value = this.u8();
    if (value > 1) throw new ProtocolError('Invalid boolean in response');
    return value === 1;
  }
  end(): void {
    if (this.offset !== this.bytes.length) throw new ProtocolError('Trailing bytes in response');
  }
}

/** Bounded incremental decoder: copy fragments once into a single frame allocation. */
export class FrameDecoder {
  private readonly prefix = Buffer.allocUnsafe(4);
  private prefixUsed = 0;
  private frame: Buffer | undefined;
  private frameUsed = 0;
  constructor(private readonly onFrame: (frame: Buffer) => void) {}
  push(chunk: Buffer): void {
    let offset = 0;
    while (offset < chunk.length) {
      if (!this.frame) {
        const count = Math.min(4 - this.prefixUsed, chunk.length - offset);
        chunk.copy(this.prefix, this.prefixUsed, offset, offset + count);
        offset += count;
        this.prefixUsed += count;
        if (this.prefixUsed !== 4) continue;
        const length = this.prefix.readUInt32BE(0);
        if (length < 5 || length > MAX_FRAME) throw new ProtocolError('Invalid response frame length');
        this.frame = Buffer.allocUnsafe(length);
        this.frameUsed = 0;
        this.prefixUsed = 0;
      }
      const count = Math.min(this.frame.length - this.frameUsed, chunk.length - offset);
      chunk.copy(this.frame, this.frameUsed, offset, offset + count);
      offset += count;
      this.frameUsed += count;
      if (this.frameUsed === this.frame.length) {
        const complete = this.frame;
        this.frame = undefined;
        this.frameUsed = 0;
        this.onFrame(complete);
      }
    }
  }
}

export function envelope(opcode: number, requestId: number, body: Buffer): Buffer {
  if (body.length + 5 > MAX_FRAME) throw new RangeError(`Request exceeds ${MAX_FRAME} byte frame limit`);
  const frame = Buffer.allocUnsafe(body.length + 9);
  frame.writeUInt32BE(body.length + 5, 0);
  frame.writeUInt8(opcode, 4);
  frame.writeUInt32BE(requestId, 5);
  body.copy(frame, 9);
  return frame;
}
