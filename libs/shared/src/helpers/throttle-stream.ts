import { Transform, TransformCallback } from 'stream';

export class ThrottleStream extends Transform {
  private timer: NodeJS.Timeout | null = null;

  constructor(private readonly bytesPerSec: number) {
    super();
  }

  _transform(
    chunk: Buffer,
    _encoding: BufferEncoding,
    callback: TransformCallback,
  ): void {
    if (!this.bytesPerSec || this.bytesPerSec <= 0) {
      this.push(chunk);
      return callback();
    }

    this.push(chunk);

    const delayMs = (chunk.length / this.bytesPerSec) * 1000;

    // Track the timer reference
    this.timer = setTimeout(() => {
      this.timer = null;
      if (!this.destroyed) {
        callback();
      }
    }, delayMs);
  }

  // Clear pending timers immediately if the client disconnects or destroys the stream
  _destroy(error: Error | null, callback: (error?: Error | null) => void): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    callback(error);
  }
}