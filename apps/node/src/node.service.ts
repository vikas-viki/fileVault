import {
  BUFFER_STREAM_SIZE,
  COORDINATOR_GRPC_CLIENT,
  NODE,
  REPLICATION_COUNT,
  STREAM_CHUNK_SIZE,
} from '@app/shared/helpers/constants';
import { HEARTBEAT_SERVICE_NAME } from '@app/shared/protos/interfaces/coordinator';
import type {
  HeartbeatResponse,
  HeartbeatServiceController,
} from '@app/shared/protos/interfaces/coordinator';
import {
  BadRequestException,
  HttpException,
  HttpStatus,
  Inject,
  Injectable,
  InternalServerErrorException,
  Req,
  Res,
} from '@nestjs/common';
import type { ClientGrpc } from '@nestjs/microservices';
import { statfs } from 'fs/promises';
import { firstValueFrom, Observable, Subject, Subscription } from 'rxjs';
import { StreamRequest } from './node.type';
import { StreamRequest as NodeStreamRequest } from '@app/shared/protos/interfaces/node';
import * as BusBoy from 'busboy';
import { StreamChunkSizerService } from '@app/shared/helpers/stream-chunk-sizer';
import { GrpcClientsPoolService } from './grpc-clients-pool/grpc-clients-pool.service';
import {
  NODE_SERVICE_NAME,
  NodeServiceClient,
} from '@app/shared/protos/interfaces/node';
import { createHash } from 'crypto';

@Injectable()
export class NodeService {
  constructor(
    @Inject(COORDINATOR_GRPC_CLIENT) private readonly client: ClientGrpc,
    private readonly grpcClientsPoolService: GrpcClientsPoolService,
  ) {}

  private heartbeatService!: HeartbeatServiceController;
  private allocatedSpaceSinceLastHeartbeat: number = 0;

  onModuleInit() {
    this.heartbeatService = this.client.getService<HeartbeatServiceController>(
      HEARTBEAT_SERVICE_NAME,
    );
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
            allocatedSpaceSinceLastHeartbeat:
              this.allocatedSpaceSinceLastHeartbeat,
          }) as Observable<HeartbeatResponse>,
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

  async clientStreamFile(@Req() request, @Res() response, data: StreamRequest) {
    try {
      // you will get available nodes details, put the chunk in your system, then pass on to the other nodes likewise
      const busboy = BusBoy({ headers: request.headers });
      const { nodesToStream } = data;
      let fileSize = BigInt(data.fileSize);
      let isUploadAborted = false;
      let grpcFinishedPromise = Promise.resolve();

      if (nodesToStream.length !== REPLICATION_COUNT) {
        console.error(`${NODE} replication factor not met, aborting upload`);
        throw new BadRequestException('Replication factor not met');
      }

      // pop the fist node, which is the current one
      nodesToStream.shift();
      const grpcClient = await this.grpcClientsPoolService.getClient(
        nodesToStream[0],
      );
      if (!grpcClient) {
        console.log(`${NODE} error connecting to downstream node`);
        throw new InternalServerErrorException(
          'Error connecting to downstream node, aborting upload',
        );
      }
      const nodeService =
        grpcClient.getService<NodeServiceClient>(NODE_SERVICE_NAME);

      busboy.on('file', (name, fileStream, info) => {
        console.log('busybody data ', { name, info });
        const chunkSizer = new StreamChunkSizerService(STREAM_CHUNK_SIZE);
        const controlledStream = fileStream.pipe(chunkSizer);
        const upstream$ = new Subject<NodeStreamRequest>();
        const downstream$ = nodeService.streamChunk(upstream$);
        let grpcSubscription: Subscription;

        grpcFinishedPromise = new Promise<void>((resolve, reject) => {
          grpcSubscription = downstream$.subscribe({
            next: (grpcResponse) => {
              console.log(
                `${NODE} Received confirmation from downstream node:`,
                grpcResponse,
              );
            },
            error: (err) => {
              console.error(`${NODE} Downstream gRPC storage node error:`, err);
              isUploadAborted = true;
              if (!controlledStream.destroyed) {
                controlledStream.destroy(err);
              }
              reject(err);
            },
            complete: () => {
              resolve();
            },
          });
        });

        controlledStream.on('data', (controlledChunk: Buffer) => {
          if (isUploadAborted) return true;
          const hash = createHash('sha256')
            .update(controlledChunk)
            .digest('hex');
          this.allocatedSpaceSinceLastHeartbeat += controlledChunk.length;
          fileSize -= BigInt(controlledChunk.length);

          if (fileSize + BUFFER_STREAM_SIZE < 0) {
            isUploadAborted = true;
            console.error(
              `${NODE} file size exceeded expected number of bytes`,
            );
            upstream$.error(
              new BadRequestException(
                'File size exceeded expected number of bytes',
              ),
            );
            grpcSubscription.unsubscribe();
            controlledStream.destroy(
              new BadRequestException(
                'File size exceeded expected number of bytes',
              ),
            );
          } else {
            upstream$.next({
              chunk: controlledChunk,
              chunkHash: hash,
            });
          }
        });

        controlledStream.on('error', (err) => {
          console.error('Stream error encountered:', err.message);
          isUploadAborted = true;

          upstream$.error(err);
          grpcSubscription.unsubscribe();

          if (!response.headersSent) {
            const statusCode =
              err instanceof BadRequestException
                ? HttpStatus.BAD_REQUEST
                : HttpStatus.INTERNAL_SERVER_ERROR;
            response.status(statusCode).json({ message: err.message });
          }
        });

        controlledStream.on('end', () => {
          if (!isUploadAborted) {
            upstream$.complete();
            console.log(`${NODE} streamed chunks to nodes successfully`);
          }
        });
      });

      busboy.on('end', async () => {
        try {
          if (isUploadAborted) return;

          await grpcFinishedPromise;
          console.log(`${NODE} file uploaded successfully`);
          if (!response.headersSent) {
            response
              .status(HttpStatus.CREATED)
              .json({ message: 'File uploaded successfully' });
          }
        } catch (grpcErr) {
          if (!response.headersSent) {
            response
              .status(HttpStatus.INTERNAL_SERVER_ERROR)
              .json({ message: 'Downstream replication sync failed' });
          }
        }
      });

      busboy.on('end', () => {
        if (!response.headersSent) {
          response
            .status(HttpStatus.CREATED)
            .json({ message: 'File uploaded successfully' });
        }
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
