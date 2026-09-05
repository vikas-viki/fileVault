import {
  COORDINATOR_GRPC_CLIENT,
  GRPC_PORT,
  HTTP_PORT,
  NODE,
  NODE_FILES_WRITE_PATH,
  NODE_IDENTIFIER,
  REPLICATION_COUNT,
  STREAM_CHUNK_SIZE,
  DOWNLOAD_RATE_LIMIT_BYTES_PER_SEC,
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
import { ThrottleStream } from '@app/shared/helpers/throttle-stream';
import { GrpcClientsPoolService } from './utils/grpc-clients-pool.service';
import { NODE_SERVICE_NAME } from '@app/shared/protos/interfaces/node';
import { Metadata } from '@grpc/grpc-js';
import { GrpcRelayWriter, RawNodeServiceClient } from './utils/grpc-relay-writer';
import fs from 'fs';
import path from 'path';
import express from 'express';
import { UploadStreamSession } from './utils/UploadStreamSession';

@Injectable()
export class NodeService {
  constructor(
    @Inject(COORDINATOR_GRPC_CLIENT) private readonly client: ClientGrpc,
    private readonly grpcClientsPoolService: GrpcClientsPoolService,
  ) {}

  private heartbeatService!: HeartbeatServiceController;
  private uploadService!: UploadServiceClient;
  public allocatedSpaceSinceLastHeartbeat: number = 0;

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

  onApplicationBootstrap() {
    void this.heartbeat();
  }

  public async commitUpload(
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

  async getAvailableSpaceInBytes(): Promise<number> {
    try {
      const stats = await statfs('/');
      const availableSpace = stats.bavail * stats.bsize;
      return stats.ffree > 0 ? Number(availableSpace) : 0;
    } catch (err) {
      console.error(`${NODE} error getting available space: `, err);
      return 0;
    }
  }

  public async connectToReplica(node: string): Promise<GrpcRelayWriter> {
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

  private validateUploadMetadata(data: StreamRequest) {
    if (!data.fileId || !data.fileSize) {
      throw new BadRequestException('Missing upload metadata');
    }
    if (data.nodesToStream?.length !== REPLICATION_COUNT) {
      console.error(`${NODE} replication factor not met, aborting upload`);
      throw new BadRequestException('Replication factor not met');
    }
  }

  async clientStreamFile(@Req() request: any, @Res() response: express.Response, data: StreamRequest) {
    try {
      this.validateUploadMetadata(data);

      const session = new UploadStreamSession(this, response, data);
      const busboy = Busboy({
        headers: request.headers,
        highWaterMark: STREAM_CHUNK_SIZE,
        limits: { fileSize: data.fileSize },
      });

      // Target node list (exclude self at index 0)
      const replicaNodes = [...data.nodesToStream];
      replicaNodes.shift();

      busboy.on('file', (_, fileStream) => {
        void session.handleFileStream(fileStream, replicaNodes);
      });

      busboy.on('error', (err) => session.sendError(err));
      request.pipe(busboy);
    } catch (err) {
      console.error(`${NODE} error uploading the file: `, err);
      if (err instanceof HttpException) throw err;
      throw new InternalServerErrorException('Error uploading the file');
    }
  }

  async streamFileToClient(response: express.Response, chunkHashes: string[]) {
    const throttle = new ThrottleStream(DOWNLOAD_RATE_LIMIT_BYTES_PER_SEC);
    try {
      if (!chunkHashes?.length) {
        throw new BadRequestException('No chunks requested');
      }

      response.setHeader('Content-Type', 'application/octet-stream');
      throttle.pipe(response, { end: false });

      for (const hash of chunkHashes) {
        await this.pipeChunkToThrottle(hash, throttle);
      }

      throttle.end();
      await new Promise<void>((resolve) => throttle.on('end', resolve));
      response.end();
    } catch (err: any) {
      this.handleDownloadError(err, response, throttle);
    }
  }

  private async pipeChunkToThrottle(hash: string, throttle: ThrottleStream): Promise<void> {
    const filePath = path.join(NODE_FILES_WRITE_PATH, NODE_IDENTIFIER, hash);
    return new Promise((resolve, reject) => {
      const readStream = fs.createReadStream(filePath);
      readStream.on('error', reject);
      readStream.on('end', resolve);
      readStream.pipe(throttle, { end: false });
    });
  }

  private handleDownloadError(err: any, response: express.Response, throttle: ThrottleStream) {
    console.error(`${NODE} error streaming file to client: `, err);
    if (!response.headersSent) {
      const status = err instanceof HttpException ? err.getStatus() : HttpStatus.INTERNAL_SERVER_ERROR;
      response.status(status).json({
        message: err instanceof HttpException ? err.message : 'Error streaming file',
      });
    } else if (!response.destroyed) {
      throttle.destroy();
      response.destroy();
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

  public async writeChunkToDisk(chunk: Uint8Array, pathSegments: string[]) {
    await new Promise((resolve, reject) => {
      const filePath = path.join(...pathSegments);
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
