import { COORDINATOR_GRPC_CLIENT, NODE, STREAM_CHUNK_SIZE } from '@app/shared/helpers/constants';
import { HEARTBEAT_SERVICE_NAME } from '@app/shared/protos/interfaces/coordinator';
import type { HeartbeatResponse, HeartbeatServiceController } from '@app/shared/protos/interfaces/coordinator';
import { HttpException, HttpStatus, Inject, Injectable, InternalServerErrorException, Req, Res } from '@nestjs/common';
import type { ClientGrpc } from '@nestjs/microservices';
import { statfs } from 'fs/promises';
import { firstValueFrom, Observable } from 'rxjs';
import { StreamRequest } from './node.type';
import * as BusBoy from 'busboy';
import { StreamChunkSizerService } from '@app/shared/helpers/stream-chunk-sizer';

@Injectable()
export class NodeService {

  constructor(@Inject(COORDINATOR_GRPC_CLIENT) private readonly client: ClientGrpc) { }
  private heartbeatService!: HeartbeatServiceController;
  private allocatedSpaceSinceLastHeartbeat: number = 0;

  onModuleInit() {
    this.heartbeatService = this.client.getService<HeartbeatServiceController>(HEARTBEAT_SERVICE_NAME);
  }

  async onApplicationBootstrap() {
    await this.heartbeat();
  }

  async heartbeat() {
    while (true) {
      try {
        const availableSpaceInBytes = await this.getAvailableSpaceInBytes();

        const response = await firstValueFrom(
          this.heartbeatService.heartbeat({
            spaceAvailableInBytes: Number(availableSpaceInBytes),
            ip: 'localhost',
            port: 4001,
            allocatedSpaceSinceLastHeartbeat: this.allocatedSpaceSinceLastHeartbeat
          }) as Observable<HeartbeatResponse>
        );

        console.log(`${NODE} got response from coordinator: `, response);
      } catch (err) {
        console.error(`${NODE} error communicating heartbeat: `, err);
      }

      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  async getAvailableSpaceInBytes(): Promise<Number> {
    try {
      const stats = await statfs('/');
      const availableSpace = stats.bavail * stats.bsize;

      return stats.ffree > 0 ? availableSpace : 0;
    } catch (err) {
      console.error(`${NODE} error getting available space: `, err);
      return 0;
    }
  }

  async streamFile(@Req() request, @Res() response, data: StreamRequest) {
    try {
      // you will get available nodes details, put the chunk in your system, then pass on to the other nodes likewise
      const busboy = BusBoy({ header: request.headers });

      busboy.on('file', (name, fileStream, info) => {
        console.log(name, info);
        const chunkSizer = new StreamChunkSizerService(STREAM_CHUNK_SIZE);
        const controlledStream = fileStream.pipe(chunkSizer);

        controlledStream.on('data', (controlledChunk: Buffer) => {
          this.allocatedSpaceSinceLastHeartbeat += controlledChunk.length;
        });

        controlledStream.on('end', () => {
          console.log(`${NODE} streamed chunks to nodes successfully`);
        });
      });

      busboy.on('end', () => {
        response.status(HttpStatus.CREATED).json({ message: 'File uploaded successfully' });
      });

      request.pipe(busboy);
    } catch (err) {
      console.error(`${NODE} error uploading the file: `, err);
      if (err instanceof HttpException) {
        throw err;
      }
      throw new InternalServerErrorException('Error uploading the file');
    }
  }

}
