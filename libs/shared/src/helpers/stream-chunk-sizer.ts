import { Transform, TransformCallback } from 'stream';
import { STREAM_CHUNK_SIZE } from './constants';

export class StreamChunkSizerService extends Transform {
  private bufferPool: Buffer[] = [];
  private currentSize = 0;

  constructor(
    private readonly targetChunkSizeInBytes: number = STREAM_CHUNK_SIZE,
  ) {
    super();
  }

  _transform(
    chunk: any,
    encoding: BufferEncoding,
    callback: TransformCallback,
  ): void {
    const binaryBuffer = Buffer.isBuffer(chunk)
      ? chunk
      : Buffer.from(chunk, encoding);

    this.bufferPool.push(binaryBuffer);
    this.currentSize += binaryBuffer.length;

    while (this.currentSize >= this.targetChunkSizeInBytes) {
      const totalBuffer = Buffer.concat(this.bufferPool);

      const chunkToRelease = totalBuffer.subarray(
        0,
        this.targetChunkSizeInBytes,
      );
      const remainingBuffer = totalBuffer.subarray(this.targetChunkSizeInBytes);

      this.push(chunkToRelease);

      this.bufferPool = remainingBuffer.length > 0 ? [remainingBuffer] : [];
      this.currentSize = remainingBuffer.length;
    }

    callback();
  }

  _flush(callback: TransformCallback): void {
    if (this.bufferPool.length > 0) {
      this.push(Buffer.concat(this.bufferPool));
    }
    callback();
  }
}
