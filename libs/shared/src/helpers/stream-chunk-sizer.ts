import { Transform, TransformCallback, TransformOptions } from 'stream';
import { STREAM_CHUNK_SIZE } from './constants';

export class StreamChunkSizerService extends Transform {
  private buffer: Buffer;
  private writtenBytes = 0;

  constructor(
    private readonly targetChunkSizeInBytes: number = STREAM_CHUNK_SIZE,
  ) {
    super({
      highWaterMark: targetChunkSizeInBytes
    });
    this.buffer = Buffer.allocUnsafe(this.targetChunkSizeInBytes);
  }

  _transform(
    chunk: any,
    encoding: BufferEncoding,
    callback: TransformCallback,
  ): void {
    const inputBuffer = Buffer.isBuffer(chunk)
      ? chunk
      : Buffer.from(chunk, encoding);
    let inputOffset = 0;
    const inputLength = inputBuffer.length;

    while (inputOffset < inputLength) {
      const sizeToWrite = Math.min(
        inputLength - inputOffset,
        this.targetChunkSizeInBytes - this.writtenBytes
      );

      inputBuffer.copy(
        this.buffer,
        this.writtenBytes,
        inputOffset,
        inputOffset + sizeToWrite
      );
      this.writtenBytes += sizeToWrite;
      inputOffset += sizeToWrite;

      if (this.writtenBytes == this.targetChunkSizeInBytes) {
        const canWriteMore = this.flushGatheredBytes();
        if (!canWriteMore) {
          break;
        }
      }
    }

    callback();
  }

  flushGatheredBytes(): boolean {
    if (!this.writtenBytes) return true;

    const slice = this.buffer.subarray(0, this.writtenBytes);
    const isWritable = this.push(Buffer.from(slice));
    this.writtenBytes = 0;

    return isWritable;
  }

  _flush(callback: TransformCallback): void {
    this.flushGatheredBytes();
    callback();
  }
}
