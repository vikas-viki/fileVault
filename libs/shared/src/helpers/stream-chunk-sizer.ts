import { Transform, TransformCallback } from 'stream';
import { STREAM_CHUNK_SIZE } from './constants';

export class StreamChunkSizerService extends Transform {

  constructor(
    private readonly targetChunkSizeInBytes: number = STREAM_CHUNK_SIZE,
  ) {
    super({
      highWaterMark: targetChunkSizeInBytes
    });
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

    if(inputLength <= this.targetChunkSizeInBytes){
      this.push(inputBuffer);
      callback();
      return;
    }
    
    while(inputOffset < inputLength){
      const bytesToRead = Math.min(
        this.targetChunkSizeInBytes, 
        inputLength - inputOffset
      );
      this.push(
        inputBuffer.subarray(inputOffset, inputOffset + bytesToRead)
      );
      inputOffset += bytesToRead;
    }
    callback();
  }
}
