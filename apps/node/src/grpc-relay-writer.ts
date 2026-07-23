import { ClientWritableStream, Metadata, ServiceError } from '@grpc/grpc-js';
import {
  StreamRequest,
  StreamResponse,
} from '@app/shared/protos/interfaces/node';

// Nest's ClientGrpcProxy bridges an Observable/Subject to a client-streaming
// call by calling `call.write(val)` and discarding the return value, so it
// never waits on backpressure. Talking to the raw grpc-js client stub
// directly gives us the real ClientWritableStream, whose write() callback we
// can await to throttle to what the downstream node can actually keep up with.
export interface RawNodeServiceClient {
  streamChunk(
    metadata: Metadata,
    callback: (error: ServiceError | null, response: StreamResponse) => void,
  ): ClientWritableStream<StreamRequest>;
}

export class GrpcRelayWriter {
  private call!: ClientWritableStream<StreamRequest>;
  private readonly response: Promise<StreamResponse>;

  constructor(client: RawNodeServiceClient, metadata: Metadata) {
    this.response = new Promise<StreamResponse>((resolve, reject) => {
      this.call = client.streamChunk(metadata, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    // Swallow here so an abort before `end()` is ever awaited doesn't surface
    // as an unhandled rejection; the real error still reaches the caller via
    // whichever write()/end() call is in flight when the call fails.
    this.response.catch(() => {});
  }

  write(chunk: StreamRequest): Promise<void> {
    return new Promise((resolve, reject) => {
      this.call.write(chunk, (err?: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  end(): Promise<StreamResponse> {
    this.call.end();
    return this.response;
  }

  cancel(): void {
    this.call.cancel();
  }
}
