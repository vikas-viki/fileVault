import {
  BUFFER_STREAM_SIZE,
  COORDINATOR_GRPC_CLIENT,
  GRPC_PORT,
  HTTP_PORT,
  NODE,
  NODE_FILES_WRITE_PATH,
  NODE_IDENTIFIER,
  REPLICATION_COUNT,
  STREAM_CHUNK_SIZE,
} from '@app/shared/helpers/constants';
import {
  HEARTBEAT_SERVICE_NAME,
  UPLOAD_SERVICE_NAME,
} from '@app/shared/protos/interfaces/coordinator';
import type {
  CommitUploadResponse,
  HeartbeatResponse,
  HeartbeatServiceController,
  UploadServiceClient,
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
import { type ClientGrpc } from '@nestjs/microservices';
import { statfs } from 'fs/promises';
import { firstValueFrom, Observable } from 'rxjs';
import { StreamRequest } from './node.type';
import Busboy from 'busboy';
import { StreamChunkSizerService } from '@app/shared/helpers/stream-chunk-sizer';
import { ThrottleStream } from '@app/shared/helpers/throttle-stream';
import { DOWNLOAD_RATE_LIMIT_BYTES_PER_SEC } from '@app/shared/helpers/constants';
import { GrpcClientsPoolService } from './grpc-clients-pool/grpc-clients-pool.service';
import { NODE_SERVICE_NAME } from '@app/shared/protos/interfaces/node';
import { createHash } from 'crypto';
import { Metadata } from '@grpc/grpc-js';
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
  private uploadService!: UploadServiceClient;
  private allocatedSpaceSinceLastHeartbeat: number = 0;

  onModuleInit() {
    this.heartbeatService = this.client.getService<HeartbeatServiceController>(
      HEARTBEAT_SERVICE_NAME,
    );
    this.uploadService =
      this.client.getService<UploadServiceClient>(UPLOAD_SERVICE_NAME);
    fs.mkdirSync(path.join(NODE_FILES_WRITE_PATH, NODE_IDENTIFIER), {
      recursive: true,
    });
  }

  private async commitUpload(
    fileId: string,
    chunkHashes: string[],
    success: boolean,
  ): Promise<boolean> {
    try {
      const res = await firstValueFrom(
        this.uploadService.commitUpload({
          fileId,
          chunkHashes,
          success,
        }) as Observable<CommitUploadResponse>,
      );
      return res.ok;
    } catch (err) {
      console.error(`${NODE} error committing upload ${fileId}: `, err);
      return false;
    }
  }

  onApplicationBootstrap() {
    // Fire-and-forget: the loop runs forever, so awaiting it would block
    // bootstrap and the HTTP server would never start.
    void this.heartbeat();
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
            httpPort: Number(HTTP_PORT),
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
      const busboy = Busboy({ headers: request.headers });
      const { nodesToStream } = data;

      if (!data.fileId || !data.fileSize) {
        throw new BadRequestException('Missing upload metadata');
      }

      if (nodesToStream.length !== REPLICATION_COUNT) {
        console.error(`${NODE} replication factor not met, aborting upload`);
        throw new BadRequestException('Replication factor not met');
      }

      let fileSize = BigInt(data.fileSize);
      let isUploadAborted = false;
      let responseSent = false;

      // First node is self; the rest are the replica targets we fan out to.
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

        const chunkHashes: string[] = [];

        const abort = (err: any) => {
          if (isUploadAborted) return;
          isUploadAborted = true;
          relays.forEach((r) => r.cancel());
          if (!controlledStream.destroyed) controlledStream.destroy(err);
          void this.commitUpload(data.fileId, [], false);
          sendError(err);
        };

        fileStream.on('error', abort);

        (async () => {
          relays = await Promise.all(
            replicaNodes.map((node) => this.connectToReplica(node)),
          );

          // Awaiting each chunk's writes drives backpressure back to the client.
          for await (const controlledChunk of controlledStream) {
            if (isUploadAborted) return;

            const chunk = controlledChunk as Buffer;
            const hash = createHash('sha256').update(chunk).digest('hex');
            chunkHashes.push(hash);
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

          await Promise.all(relays.map((r) => r.end()));

          const committed = await this.commitUpload(
            data.fileId,
            chunkHashes,
            true,
          );
          if (!committed) {
            throw new InternalServerErrorException(
              'Failed to commit upload metadata',
            );
          }

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

  async streamFileToClient(response, chunkHashes: string[]) {
    const throttle = new ThrottleStream(DOWNLOAD_RATE_LIMIT_BYTES_PER_SEC);
    try {
      if (!chunkHashes || chunkHashes.length === 0) {
        throw new BadRequestException('No chunks requested');
      }

      response.setHeader('Content-Type', 'application/octet-stream');
      throttle.pipe(response, { end: false });

      // Feed each chunk into the throttle in order; the throttle paces the whole
      // response to the rate limit regardless of chunk boundaries.
      for (const hash of chunkHashes) {
        const filePath = path.join(NODE_FILES_WRITE_PATH, NODE_IDENTIFIER, hash);
        await new Promise<void>((resolve, reject) => {
          const readStream = fs.createReadStream(filePath);
          readStream.on('error', reject);
          readStream.on('end', resolve);
          readStream.pipe(throttle, { end: false });
        });
      }

      throttle.end();
      await new Promise<void>((resolve) => throttle.on('end', resolve));
      response.end();
    } catch (err: any) {
      console.error(`${NODE} error streaming file to client: `, err);
      if (!response.headersSent) {
        const status =
          err instanceof HttpException
            ? err.getStatus()
            : HttpStatus.INTERNAL_SERVER_ERROR;
        response.status(status).json({
          message:
            err instanceof HttpException ? err.message : 'Error streaming file',
        });
      } else if (!response.destroyed) {
        // Headers already sent (mid-stream failure), so the download truncates.
        throttle.destroy();
        response.destroy();
      }
    }
  }

  async storeChunk(chunk: Uint8Array, chunkHash: string): Promise<void> {
    await this.writeChunkToDisk(chunk, [
      NODE_FILES_WRITE_PATH,
      NODE_IDENTIFIER,
      chunkHash,
    ]);
    this.allocatedSpaceSinceLastHeartbeat += chunk.length;
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
