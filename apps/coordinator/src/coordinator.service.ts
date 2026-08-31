import {
  HttpException,
  Inject,
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
  DOWNLOAD_NODE_INDEX,
  REDIS_CLIENT,
  REPLICATION_COUNT,
} from '@app/shared/helpers/constants';
import { HeartbeatService } from './heartbeat/heartbeat.service';
import { FilesService } from './files/files.service';
import Redis from 'ioredis';

// Atomically round-robins over the alive nodes, reserving space on the first
// REPLICATION_COUNT that fit; reserves nothing unless the full set is found.
const SELECT_AND_RESERVE = `
local n = #KEYS
if n == 0 then return {} end
local required = tonumber(ARGV[1]) + tonumber(ARGV[2])
local repCount = tonumber(ARGV[3])
local start = redis.call('INCR', ARGV[4])
local selected = {}
for i = 0, n - 1 do
  local key = KEYS[(start + i) % n + 1]
  local avail = tonumber(redis.call('HGET', key, 'spaceAvailableInBytes') or '0')
  local alloc = tonumber(redis.call('HGET', key, 'allocatedSpaceInBytes') or '0')
  if (avail - alloc) > required then
    selected[#selected + 1] = key
    if #selected == repCount then break end
  end
end
if #selected < repCount then return {} end
for i = 1, #selected do
  redis.call('HINCRBY', selected[i], 'allocatedSpaceInBytes', ARGV[1])
end
return selected
`;

interface RedisWithCommands extends Redis {
  selectAndReserve(...args: (string | number)[]): Promise<string[]>;
}

@Injectable()
export class CoordinatorService {
  // bufferStorageSpace in bytes
  private bufferStorageSpace = BigInt(50 * 1024 * 1024);

  constructor(
    private readonly heartbeatService: HeartbeatService,
    private readonly filesService: FilesService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {
    // Registers the script once so calls send only its SHA (EVALSHA), with
    // ioredis falling back to EVAL if Redis has dropped it.
    this.redis.defineCommand('selectAndReserve', { lua: SELECT_AND_RESERVE });
  }

  getHealth(): HealthCheckResponse {
    return {
      status: 'active',
    };
  }

  async downloadRequest(fileId: string, userId: string) {
    try {
      const file = await this.filesService.getFileForDownload(fileId, userId);

      // Resolve the current HTTP address of every live replica holding the file.
      const nodes: string[] = [];
      for (const grpcAddr of file.nodes) {
        const httpPort = await this.redis.hget(grpcAddr, 'httpPort');
        if (httpPort) {
          nodes.push(`${grpcAddr.split(':')[0]}:${httpPort}`);
        }
      }

      if (nodes.length === 0) {
        throw new NotFoundException('No live replica available for this file');
      }

      // Round-robin the live replicas so downloads spread across the nodes that
      // hold the file; the client uses nodes[0] and can fail over to the rest.
      const start = await this.redis.incr(DOWNLOAD_NODE_INDEX);
      const balanced = nodes.map(
        (_, i) => nodes[(start + i) % nodes.length],
      );

      return {
        fileName: file.fileName,
        chunkHashes: file.chunkHashes,
        nodes: balanced,
      };
    } catch (err) {
      if (err instanceof HttpException) {
        throw err;
      }
      console.error(`${COORDINATOR} error handling download request: `, err);
      throw new InternalServerErrorException(
        'Unable to process the download request, please try again later',
      );
    }
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

      const nodesToStream = await (this.redis as RedisWithCommands).selectAndReserve(
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

      const file = await this.filesService.createPendingFile(
        userId,
        uploadRequest.fileName,
        uploadRequest.fileSize,
        nodesToStream,
      );

      return {
        fileId: file.id,
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
