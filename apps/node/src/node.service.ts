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

  // Opens a client-stream to a single replica node. Replicas are leaves in the
  // fan-out topology (they only store, never forward), so no routing metadata
  // is passed along.
  private async connectToReplica(node: string): Promise<GrpcRelayWriter> {
    const grpcClient = await this.grpcClientsPoolService.getClient(node);
    if (!grpcClient) {
      console.log(`${NODE} error connecting to replica node ${node}`);
      throw new InternalServerErrorException(
        'Error connecting to replica node, aborting upload',
      );
    }

    const rawClient = grpcClient.getClientByServiceName<RawNodeServiceClient>(
      NODE_SERVICE_NAME,
    );
    return new GrpcRelayWriter(rawClient, new Metadata());
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

      // Pop the first node, which is this (entry) node. The rest are the
      // replica targets we fan out to.
      nodesToStream.shift();
      const replicaNodes = nodesToStream;

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
        let relays: GrpcRelayWriter[] = [];

        const abort = (err: any) => {
          if (isUploadAborted) return;
          isUploadAborted = true;
          relays.forEach((r) => r.cancel());
          if (!controlledStream.destroyed) controlledStream.destroy(err);
          sendError(err);
        };

        fileStream.on('error', abort);

        (async () => {
          relays = await Promise.all(
            replicaNodes.map((node) => this.connectToReplica(node)),
          );

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

            await Promise.all([
              this.writeChunkToDisk(chunk, [
                NODE_FILES_WRITE_PATH,
                NODE_IDENTIFIER,
                hash,
              ]),
              ...relays.map((r) => r.write({ chunk, chunkHash: hash })),
            ]);
            this.allocatedSpaceSinceLastHeartbeat += chunk.length;
          }

          if (isUploadAborted) return;

          // Wait for every replica to finish storing and ack. If any one fails,
          // this rejects and the whole upload is failed.
          await Promise.all(relays.map((r) => r.end()));

          console.log(`${NODE} fanned out chunks to all replicas successfully`);
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

  // Leaf replica in the fan-out topology: the entry node streams chunks here,
  // we store each one and never forward further. The for-await pulls the next
  // chunk only once the current disk write resolves, so backpressure flows
  // straight back to the entry node's relay write.
  @GrpcStreamCall('NodeService', 'streamChunk')
  async nodeStreamChunk(
    call: ServerReadableStream<NodeStreamRequest, StreamResponse>,
    callback: (error: ServiceError | null, value?: StreamResponse) => void,
  ) {
    try {
      for await (const chunk of call) {
        await this.writeChunkToDisk(chunk.chunk, [
          NODE_FILES_WRITE_PATH,
          NODE_IDENTIFIER,
          chunk.chunkHash,
        ]);
        this.allocatedSpaceSinceLastHeartbeat += chunk.chunk.length;
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
