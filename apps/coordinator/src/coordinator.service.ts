import { HttpException, Inject, Injectable, InternalServerErrorException, NotFoundException } from '@nestjs/common';
import { HealthCheckResponse, UploadRequestDTO, UploadResponseDTO } from './coordinator.interface';
import { COORDINATOR, CURRENT_NODE_INDEX, REDIS_CLIENT, REPLICATION_COUNT } from '@app/shared/constants';
import { HeartbeatService } from './heartbeat/heartbeat.service';
import Redis from 'ioredis';

@Injectable()
export class CoordinatorService {

  // bufferStorageSpace in bytes
  private bufferStorageSpace = BigInt(50 * 1024 * 1024);

  constructor(
    private readonly heartbeatService: HeartbeatService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis
  ) { }

  getHealth(): HealthCheckResponse {
    return {
      status: 'active'
    };
  }

  async upload(uploadRequest: UploadRequestDTO): Promise<UploadResponseDTO> {
    try {
      const aliveNodes = await this.heartbeatService.getAvailabeNodes();
      const fileSize = BigInt(uploadRequest.fileSize);

      if (aliveNodes.length == 0) {
        console.log(`${COORDINATOR} no alive nodes to upload`);
        throw new NotFoundException('Nodes are currently unavailable, please try again later');
      }

      const pipeline = this.redis.multi();
      aliveNodes.forEach(nodeKey => {
        pipeline.hmget(nodeKey, 'spaceAvailableInBytes', 'allocatedSpaceInBytes')
      });
      const nodeStatsResults = await pipeline.exec();

      if (!nodeStatsResults) {
        console.log(`${COORDINATOR} no alive nodes to upload`);
        throw new NotFoundException('Nodes are currently unavailable, please try again later');
      }

      const startIndex = await this.redis.incr(CURRENT_NODE_INDEX);
      const nodesToStream: string[] = [];

      for(let i = 0; i < aliveNodes.length; i++){
        const targetIndex = (startIndex + i) % aliveNodes.length;
        const candidateNode = aliveNodes[targetIndex];
        const nodeStats = nodeStatsResults[targetIndex];

        if(nodeStats && !nodeStats[0]){
          const [spaceAvailableInBytes, allocatedSpaceInBytes] = nodeStats[1] as [string | null, string | null];

          const spaceAvailable = BigInt(spaceAvailableInBytes ??'0') - BigInt(allocatedSpaceInBytes ?? '0');
          const spaceRequired = fileSize + this.bufferStorageSpace;

          if(spaceAvailable > spaceRequired){
            nodesToStream.push(candidateNode);
          }
        }

        if(nodesToStream.length === REPLICATION_COUNT){
          break;
        }
      }

      if (nodesToStream.length < REPLICATION_COUNT) {
        console.log(`${COORDINATOR} not enough streamable nodes to upload`);
        throw new NotFoundException('Nodes are currently filled, please try again later');
      }

      const lockPipeline = this.redis.multi();
      nodesToStream.forEach(nodeKey => {
        lockPipeline.hincrby(
          nodeKey,
          'allocatedSpaceInBytes',
          uploadRequest.fileSize
        );
      })
      await lockPipeline.exec();

      return {
        nodesToStream
      };
    } catch (err) {
      console.error(`${COORDINATOR} error getting available nodes: `, err);
      if (err instanceof HttpException) {
        throw err;
      }
      throw new InternalServerErrorException('Unable to process the upload request, please try again later');
    }
  }
}
