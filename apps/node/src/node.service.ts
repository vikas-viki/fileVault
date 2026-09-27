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
  CURRENT_NODE_ID,
  CURRENT_NODE_ID_INDEX,
  STORAGE_CHUNK_SIZE,
} from '@app/shared/helpers/constants';
import { HEARTBEAT_SERVICE_NAME } from '@app/shared/protos/interfaces/coordinator';
import type {
  GetIpResponse,
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
  NotFoundException,
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
import {
  NodeStreamRequest,
  NodeStreamResponse,
} from '@app/shared/protos/interfaces/node';
import { NodeRepository } from '@app/shared/repository/node.repository';
import { ObjectStatus } from '@app/shared/models/object.model';
import { DownloadRquestDTO } from './node.types';
import { ChunkRepository } from '@app/shared/repository/chunk.repository';
import { ChunkReplicaModel } from '@app/shared/models/chunk-replica.model';

@Injectable()
export class NodeService {
  private nodeIp: string;
  constructor(
    @Inject(COORDINATOR_GRPC_CLIENT) private readonly client: ClientGrpc,
    private readonly grpcClientPoolService: GrpcClientsPoolService,
    private readonly binFileStorageService: BinFileStorageService,
    private readonly objectRepository: ObjectRepository,
    private readonly nodeRepository: NodeRepository,
    private readonly chunkRepository: ChunkRepository
  ) { }

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
    const ipResponse = await firstValueFrom(this.heartbeatService.getIp({}) as Observable<GetIpResponse>);
    this.nodeIp = ipResponse.ip;

    // register node
    await this.nodeRepository.upsert({
      nodeId: CURRENT_NODE_ID,
      ipAddress: this.nodeIp,
      name: `NODE-${CURRENT_NODE_ID_INDEX}`,
      port: Number(HTTP_PORT)
    });

    while (true) {
      try {
        const availableSpaceInBytes = await this.getAvailableSpaceInBytes();
        const response = await firstValueFrom(
          this.heartbeatService.heartbeat({
            spaceAvailableInBytes: Number(availableSpaceInBytes),
            ip: this.nodeIp,
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

  async updateObjectStatus(id: string, status: ObjectStatus) {
    await this.objectRepository.updateStatus({ id, status });
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

  async handleClientFileStream(
    @Req() request: any,
    @Res() response: express.Response,
    data: StreamRequest,
  ) {
    let object;
    try {
      this.validateUploadMetadata(data);

      object = await this.objectRepository.create({
        userId: request.user.id,
        fileName: data.fileId,
        fileSize: data.fileSize,
      });

      const session = new UploadStreamSessionService(
        this,
        this.grpcClientPoolService,
        this.binFileStorageService,
        data.fileSize,
        response,
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
        void session.handleClientFileStream(
          fileStream,
          replicaNodes,
          object.id,
        );
      });

      busboy.on('error', (err) => session.sendError(err));
      request.pipe(busboy);
    } catch (err) {
      console.error(`${NODE} error uploading the file: `, err, object.id);
      this.updateObjectStatus(object.id, ObjectStatus.ABORTED);
      if (err instanceof HttpException) throw err;
      throw new InternalServerErrorException('Error uploading the file');
    }
  }

  async handleNodeFileStream(
    stream: ServerReadableStream<NodeStreamRequest, NodeStreamResponse>,
    fileSize: number,
    objectId: string,
  ): Promise<void> {
    const streamSession = new UploadStreamSessionService(
      this,
      this.grpcClientPoolService,
      this.binFileStorageService,
      fileSize,
    );

    await streamSession.processNodeStream(stream, objectId);
  }

  async streamFileToClient(data: DownloadRquestDTO, response: express.Response) {
    const throttle = new ThrottleStream(DOWNLOAD_RATE_LIMIT_BYTES_PER_SEC);
    let isAborted = false;

    response.on('close', () => {
      isAborted = true;
      throttle.destroy();
    });
    try {
      const object = await this.objectRepository.findObjectByUserId(data.objectId, data.userId);

      if (!object) {
        throw new NotFoundException('File not found');
      }

      response.setHeader('Content-Type', 'application/octet-stream');
      throttle.pipe(response, { end: false });

      const startChunkIndex = Math.floor(Number(data.byteOffset) / STORAGE_CHUNK_SIZE);
      const requestedStartByteOffset = data.byteOffset;
      const chunkRelativeOffset = Number(requestedStartByteOffset) % STORAGE_CHUNK_SIZE;

      // get the chunks and chunk replicas
      let totalChunks = await this.chunkRepository.getChunksCountByObjectId({
        objectId: data.objectId,
        chunkIndex: startChunkIndex
      });
      let startOffset = 0;
      let endOffset = 0;

      for (let i = 0; i < totalChunks && !isAborted; i += 10) {
        const chunksData = await this.chunkRepository.getChunksByObjectIdAndChunkIndex({
          objectId: data.objectId,
          chunkIndex: startChunkIndex,
          limit: 10,
          offset: i,
          nodeId: CURRENT_NODE_ID
        });

        for (let j = 0; j < chunksData.length && !isAborted; j++) {
          const chunk = chunksData[j];
          const chunkReplica = chunk.chunkReplica[0];
          let isFirstChunk = i === 0 && j === 0;

          startOffset = chunkReplica.byteOffset + (isFirstChunk ? chunkRelativeOffset : 0);
          endOffset = chunkReplica.byteOffset + chunk.chunkSize;

          await this.binFileStorageService.performOffsetRead(
            chunkReplica.binFileId,
            startOffset,
            endOffset,
            throttle
          );
        }
      }

      if (!isAborted) {
        throttle.end();
        await new Promise<void>((resolve) => throttle.on('end', resolve));
        response.end();
      }
    } catch (err: any) {
      if (!isAborted) {
        this.handleDownloadError(err, response, throttle);
      }
    }
  }


  private handleDownloadError(
    err: any,
    response: express.Response,
    throttle: ThrottleStream,
  ) {
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
      throttle.destroy();
      response.destroy();
    }
  }
}
