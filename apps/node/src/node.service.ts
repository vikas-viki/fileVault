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
} from '@app/shared/protos/interfaces/coordinator';
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
import { type ClientGrpc } from '@nestjs/microservices';
import { statfs } from 'fs/promises';
import { firstValueFrom, Observable } from 'rxjs';
import { StreamRequest } from './node.dto';
import Busboy from 'busboy';
import { ThrottleStream } from '@app/shared/helpers/throttle-stream';
import { GrpcClientsPoolService } from './grpc/grpc-clients-pool.service';
import fs from 'fs';
import path from 'path';
import express from 'express';
import { UploadStreamSessionService } from './utils/upload-stream-session.service';
import { BinFileStorageService } from './bin-file-storage/bin-file-storage.service';
import { ObjectRepository } from '@app/shared/repository/object.repository';
import { ServerReadableStream } from '@grpc/grpc-js';
import { NodeStreamRequest, NodeStreamResponse } from '@app/shared/protos/interfaces/node';

@Injectable()
export class NodeService {
  constructor(
    @Inject(COORDINATOR_GRPC_CLIENT) private readonly client: ClientGrpc,
    private readonly grpcClientPoolService: GrpcClientsPoolService,
    private readonly binFileStorageService: BinFileStorageService,
    private readonly objectRepository: ObjectRepository
  ) {

  }

  private heartbeatService!: HeartbeatServiceController;
  private allocatedSpaceSinceLastHeartbeat: number = 0;

  onModuleInit() {
    this.heartbeatService = this.client.getService<HeartbeatServiceController>(
      HEARTBEAT_SERVICE_NAME,
    );
    fs.mkdirSync(path.join(NODE_FILES_WRITE_PATH, NODE_IDENTIFIER), {
      recursive: true,
    });
  }

  onApplicationBootstrap() {
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

  increaseAllocatedSpace(allocatedSpace: number) {
    this.allocatedSpaceSinceLastHeartbeat += allocatedSpace;
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

  private validateUploadMetadata(data: StreamRequest) {
    if (!data.fileId || !data.fileSize) {
      throw new BadRequestException('Missing upload metadata');
    }
    if (data.nodesToStream?.length !== REPLICATION_COUNT) {
      console.error(`${NODE} replication factor not met, aborting upload`);
      throw new BadRequestException('Replication factor not met');
    }
  }

  async handleClientFileStream(@Req() request: any, @Res() response: express.Response, data: StreamRequest) {
    try {
      this.validateUploadMetadata(data);

      const object = await this.objectRepository.create({
        userId: request.user.id,
        fileName: data.fileId,
        fileSize: data.fileSize
      });

      const session = new UploadStreamSessionService(
        this,
        this.grpcClientPoolService,
        this.binFileStorageService,
        data.fileSize,
        response
      );
      const busboy = Busboy({
        headers: request.headers,
        highWaterMark: STREAM_CHUNK_SIZE,
        limits: { fileSize: data.fileSize },
      });

      // Target node list (exclude self at index 0)
      const replicaNodes = [...data.nodesToStream];
      replicaNodes.shift();

      busboy.on('file', (_, fileStream) => {
        void session.handleClientFileStream(fileStream, replicaNodes, object.id);
      });

      busboy.on('error', (err) => session.sendError(err));
      request.pipe(busboy);
    } catch (err) {
      console.error(`${NODE} error uploading the file: `, err);
      if (err instanceof HttpException) throw err;
      throw new InternalServerErrorException('Error uploading the file');
    }
  }

  async handleNodeFileStream(
    stream: ServerReadableStream<NodeStreamRequest, NodeStreamResponse>,
    fileSize: number
  ): Promise<void> {
    const streamSession = new UploadStreamSessionService(
      this,
      this.grpcClientPoolService,
      this.binFileStorageService,
      fileSize
    );

    await streamSession.processNodeStream(stream);
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
}
