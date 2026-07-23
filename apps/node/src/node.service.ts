import {
  BUFFER_STREAM_SIZE,
  COORDINATOR_GRPC_CLIENT,
  GRPC_PORT,
  NODE,
  NODE_FILES_WRITE_PATH,
  NODE_IDENTIFIER,
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
import { GrpcStreamCall, type ClientGrpc } from '@nestjs/microservices';
import { statfs } from 'fs/promises';
import { firstValueFrom, Observable } from 'rxjs';
import { StreamRequest } from './node.type';
import {
  StreamRequest as NodeStreamRequest,
  StreamResponse,
} from '@app/shared/protos/interfaces/node';
import * as BusBoy from 'busboy';
import { StreamChunkSizerService } from '@app/shared/helpers/stream-chunk-sizer';
import { GrpcClientsPoolService } from './grpc-clients-pool/grpc-clients-pool.service';
import { NODE_SERVICE_NAME } from '@app/shared/protos/interfaces/node';
import { createHash } from 'crypto';
import { Metadata } from '@grpc/grpc-js';
import type { ServerReadableStream, ServiceError } from '@grpc/grpc-js';
import { GrpcRelayWriter, RawNodeServiceClient } from './grpc-relay-writer';
import fs from 'fs';
import path from 'path';

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
    fs.mkdirSync(NODE_FILES_WRITE_PATH, { recursive: true });
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
            port: Number(GRPC_PORT),
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

  // `remainingNodes` is the chain still left to replicate to, self already
  // removed and starting with the next hop (the convention every hop, and
  // the coordinator's initial response, follows). Returns null at the last
  // node in the chain.
  private async connectToNextHop(
    remainingNodes: string[],
  ): Promise<GrpcRelayWriter | null> {
    if (remainingNodes.length === 0) {
      return null;
    }

    const grpcClient = await this.grpcClientsPoolService.getClient(
      remainingNodes[0],
    );
    if (!grpcClient) {
      console.log(`${NODE} error connecting to downstream node`);
      throw new InternalServerErrorException(
        'Error connecting to downstream node, aborting upload',
      );
    }

    const forwardMetadata = new Metadata();
    forwardMetadata.add('nodesToStream', remainingNodes.join(','));

    const rawClient = grpcClient.getClientByServiceName<RawNodeServiceClient>(
      NODE_SERVICE_NAME,
    );
    return new GrpcRelayWriter(rawClient, forwardMetadata);
  }

  async clientStreamFile(@Req() request, @Res() response, data: StreamRequest) {
    try {
      const busboy = BusBoy({ headers: request.headers });
      const { nodesToStream } = data;
      let fileSize = BigInt(data.fileSize);
      let isUploadAborted = false;
      let responseSent = false;

      if (nodesToStream.length !== REPLICATION_COUNT) {
        console.error(`${NODE} replication factor not met, aborting upload`);
        throw new BadRequestException('Replication factor not met');
      }

      // Pop the first node, which is the current one
      nodesToStream.shift();

      const sendResponse = (statusCode: number, message: string) => {
        if (responseSent) return;
        responseSent = true;
        if (!response.headersSent) {
          response.status(statusCode).json({ message });
        }
      };

      const sendError = (err: any) => {
        console.error(`${NODE} error uploading/downstreaming file: `, err);
        const statusCode =
          err instanceof HttpException
            ? err.getStatus()
            : HttpStatus.INTERNAL_SERVER_ERROR;
        const message =
          err instanceof HttpException ? err.message : 'Error uploading the file';
        sendResponse(statusCode, message);
      };

      busboy.on('file', (name, fileStream, info) => {
        console.log('busboy data ', { name, info });
        const chunkSizer = new StreamChunkSizerService(STREAM_CHUNK_SIZE);
        const controlledStream = fileStream.pipe(chunkSizer);
        let relay: GrpcRelayWriter | null = null;

        const abort = (err: any) => {
          if (isUploadAborted) return;
          isUploadAborted = true;
          relay?.cancel();
          if (!controlledStream.destroyed) controlledStream.destroy(err);
          sendError(err);
        };

        fileStream.on('error', abort);

        (async () => {
          relay = await this.connectToNextHop(nodesToStream);

          // Using async iterator for inherent backpressure without manual pause/resume
          for await (const controlledChunk of controlledStream) {
            if (isUploadAborted) return;

            const chunk = controlledChunk as Buffer;
            const hash = createHash('sha256').update(chunk).digest('hex');
            fileSize -= BigInt(chunk.length);

            if (fileSize + BigInt(BUFFER_STREAM_SIZE) < 0n) {
              throw new BadRequestException(
                'File size exceeded expected number of bytes',
              );
            }

            await this.writeChunkToDisk(chunk, [
              NODE_FILES_WRITE_PATH,
              NODE_IDENTIFIER,
              hash,
            ]);
            this.allocatedSpaceSinceLastHeartbeat += chunk.length;

            if (relay) {
              await relay.write({ chunk, chunkHash: hash });
            }
          }

          if (isUploadAborted) return;

          if (relay) {
            await relay.end();
          }

          console.log(`${NODE} streamed chunks to nodes successfully`);
          sendResponse(HttpStatus.CREATED, 'File uploaded successfully');
        })().catch(abort);
      });

      busboy.on('error', sendError);
      request.pipe(busboy);
    } catch (err) {
      console.error(`${NODE} error uploading the file: `, err);
      if (err instanceof HttpException) {
        throw err;
      }
      throw new InternalServerErrorException('Error uploading the file');
    }
  }

  @GrpcStreamCall('NodeService', 'streamChunk')
  async nodeStreamChunk(
    call: ServerReadableStream<NodeStreamRequest, StreamResponse>,
    callback: (error: ServiceError | null, value?: StreamResponse) => void,
  ) {
    let relay: GrpcRelayWriter | null = null;

    try {
      const nodesToStream = (call.metadata.get('nodesToStream')[0]?.toString() ?? '')
        .split(',')
        .filter(Boolean);

      relay = await this.connectToNextHop(nodesToStream.slice(1));

      for await (const chunk of call) {
        await this.writeChunkToDisk(chunk.chunk, [
          NODE_FILES_WRITE_PATH,
          NODE_IDENTIFIER,
          chunk.chunkHash,
        ]);
        this.allocatedSpaceSinceLastHeartbeat += chunk.chunk.length;

        if (relay) {
          await relay.write(chunk);
        }
      }

      const response = relay ? await relay.end() : { success: true };
      console.log(`${NODE} file stored and sent downstream successfully`);
      callback(null, response);
    } catch (err) {
      console.error(`${NODE} error receiving/relaying chunk stream: `, err);
      relay?.cancel();
      if (!call.destroyed) {
        call.destroy(err instanceof Error ? err : new Error(String(err)));
      }
      callback(err as ServiceError, { success: false });
    }
  }

  async writeChunkToDisk(chunk: Uint8Array, _path: string[]) {
    await new Promise((resolve, reject) => {
      const filePath = path.join(..._path);
      fs.writeFile(filePath, chunk, (err) => {
        if (err) {
          console.error(`${NODE} error writing file: `, err);
          reject(err);
        } else {
          resolve(true);
        }
      });
    });
  }
}
