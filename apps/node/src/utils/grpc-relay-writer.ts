import { ClientWritableStream, Metadata, ServiceError } from '@grpc/grpc-js';
import {
  StreamRequest,
  StreamResponse,
} from '@app/shared/protos/interfaces/node';

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
    // Avoid unhandled rejection if the call fails before end() is awaited.
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
