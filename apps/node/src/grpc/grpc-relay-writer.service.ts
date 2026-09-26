import { ClientWritableStream, Metadata, ServiceError } from '@grpc/grpc-js';
import {
  NodeStreamRequest,
  NodeStreamResponse,
} from '@app/shared/protos/interfaces/node';

export interface RawNodeServiceClient {
  streamChunk(
    metadata: Metadata,
    callback: (
      error: ServiceError | null,
      response: NodeStreamResponse,
    ) => void,
  ): ClientWritableStream<NodeStreamRequest>;
}

export class GrpcRelayWriterService {
  private call!: ClientWritableStream<NodeStreamRequest>;
  private readonly response: Promise<NodeStreamResponse>;
  public readonly nodeId: string = '';

  constructor(
    client: RawNodeServiceClient,
    metadata: Metadata,
    _nodeId: string,
  ) {
    this.response = new Promise<NodeStreamResponse>((resolve, reject) => {
      this.call = client.streamChunk(metadata, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    // Avoid unhandled rejection if the call fails before end() is awaited.
    this.response.catch(() => {});
    this.nodeId = _nodeId;
  }

  write(chunk: NodeStreamRequest): Promise<void> {
    return new Promise((resolve, reject) => {
      this.call.write(chunk, (err?: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  end(): Promise<NodeStreamResponse> {
    this.call.end();
    return this.response;
  }

  cancel(): void {
    this.call.cancel();
  }
}
