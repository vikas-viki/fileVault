import { Controller } from '@nestjs/common';
import { GrpcStreamCall } from '@nestjs/microservices';
import type { ServerReadableStream, ServiceError } from '@grpc/grpc-js';
import { NODE } from '@app/shared/helpers/constants';
import type {
  StreamRequest,
  StreamResponse,
} from '@app/shared/protos/interfaces/node';
import { NodeService } from './node.service';

@Controller()
export class NodeStreamController {
  constructor(private readonly nodeService: NodeService) {}

  // Leaf of the fan-out: the entry node streams replica chunks here; store each
  // one and ack when the stream ends.
  @GrpcStreamCall('NodeService', 'streamChunk')
  async streamChunk(
    call: ServerReadableStream<StreamRequest, StreamResponse>,
    callback: (error: ServiceError | null, value?: StreamResponse) => void,
  ) {
    try {
      for await (const chunk of call) {
        await this.nodeService.storeChunk(chunk.chunk, chunk.chunkHash);
      }
      console.log(`${NODE} stored replica chunks successfully`);
      callback(null, { success: true });
    } catch (err) {
      console.error(`${NODE} error storing replica chunk stream: `, err);
      if (!call.destroyed) {
        call.destroy(err instanceof Error ? err : new Error(String(err)));
      }
      callback(err as ServiceError, { success: false });
    }
  }
}
