import { Controller } from '@nestjs/common';
import { GrpcStreamCall } from '@nestjs/microservices';
import type { ServerReadableStream, ServiceError } from '@grpc/grpc-js';
import { NODE } from '@app/shared/helpers/constants';
import type {
  NodeStreamRequest,
  NodeStreamResponse,
} from '@app/shared/protos/interfaces/node';
import { NodeService } from './node.service';

@Controller()
export class NodeStreamController {
  constructor(
    private readonly nodeService: NodeService,
  ) { }

  // handles stream fanout
  @GrpcStreamCall('NodeService', 'streamChunk')
  async streamChunk(
    call: ServerReadableStream<NodeStreamRequest, NodeStreamResponse>,
    callback: (error: ServiceError | null, value?: NodeStreamResponse) => void,
  ) {
    try {
      const fileSize = Number(call.metadata.get('file-size') ?? 0);
      if (!fileSize) {
        throw new Error('filesize not proveded in metadata');
      }

      await this.nodeService.handleNodeFileStream(call, fileSize);

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
