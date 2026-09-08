import {
  HttpException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
} from '@nestjs/common';
import {
  HealthCheckResponse,
  UploadRequestDTO,
  UploadResponseDTO,
} from './coordinator.dto';
import {
  COORDINATOR,
  CURRENT_NODE_INDEX,
  REPLICATION_COUNT,
} from '@app/shared/helpers/constants';
import { HeartbeatService } from './heartbeat/heartbeat.service';
import { RedisService } from '@app/shared/redis.service';

// Atomically round-robins over the alive nodes, reserving space on the first
// REPLICATION_COUNT that fit; reserves nothing unless the full set is found.

@Injectable()
export class CoordinatorService {
  // bufferStorageSpace in bytes
  private bufferStorageSpace = BigInt(50 * 1024 * 1024);

  constructor(
    private readonly heartbeatService: HeartbeatService,
    private readonly redis: RedisService,
  ) {
  }

  getHealth(): HealthCheckResponse {
    return {
      status: 'active',
    };
  }

  async downloadRequest(fileId: string, userId: string) {
    //
  }

  async uploadRequest(
    uploadRequest: UploadRequestDTO,
    userId: string,
  ): Promise<UploadResponseDTO> {
    try {
      const aliveNodes = await this.heartbeatService.getAvailabeNodes();
      const fileSize = BigInt(uploadRequest.fileSize);

      if (aliveNodes.length == 0) {
        console.log(`${COORDINATOR} no alive nodes to upload`);
        throw new NotFoundException(
          'Nodes are currently unavailable, please try again later',
        );
      }

      const nodesToStream = await this.redis.selectAndReserve(
        aliveNodes.length,
        ...aliveNodes,
        fileSize.toString(),
        this.bufferStorageSpace.toString(),
        REPLICATION_COUNT.toString(),
        CURRENT_NODE_INDEX,
      );

      if (nodesToStream.length < REPLICATION_COUNT) {
        console.log(`${COORDINATOR} not enough streamable nodes to upload`);
        throw new NotFoundException(
          'Nodes are currently filled, please try again later',
        );
      }

      return {
        nodesToStream,
      };
    } catch (err) {
      console.error(`${COORDINATOR} error getting available nodes: `, err);
      if (err instanceof HttpException) {
        throw err;
      }
      throw new InternalServerErrorException(
        'Unable to process the upload request, please try again later',
      );
    }
  }
}
