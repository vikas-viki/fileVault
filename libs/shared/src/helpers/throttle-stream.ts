import { Transform, TransformCallback } from 'stream';

// Rate-limits a stream by pacing each pushed slice: after pushing a chunk it
// delays the callback by (chunk.length / bytesPerSec), so upstream is read no
// faster than the target rate. Downstream backpressure is handled by the base
// Transform. Granularity follows the source's chunk size (~64KB for fs reads).
export class ThrottleStream extends Transform {
  constructor(private readonly bytesPerSec: number) {
    super();
  }

  _transform(chunk: Buffer, _encoding: BufferEncoding, callback: TransformCallback): void {
    this.push(chunk);
    const delayMs = (chunk.length / this.bytesPerSec) * 1000;
    setTimeout(callback, delayMs);
  }
}
