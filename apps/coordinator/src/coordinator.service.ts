import {
  BadRequestException,
  HttpException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
} from '@nestjs/common';
import {
  HealthCheckResponse,
  UploadRequestDTO,
} from './coordinator.dto';
import {
  COORDINATOR,
  REPLICATION_COUNT,
  UPLOAD_ROUND_ROBIN_NODE_INDEX_KEY,
} from '@app/shared/helpers/constants';
import { HeartbeatService } from './heartbeat/heartbeat.service';
import { RedisService } from '@app/shared/redis.service';
import { AuthService } from './auth/auth.service';
import { Response } from './coordinator.dto';
import { v7 } from "uuid";
import { ObjectRepository } from '@app/shared/database/repository/object.repository';

@Injectable()
export class CoordinatorService {
  // bufferStorageSpace in bytes
  private bufferStorageSpace = BigInt(50 * 1024 * 1024);

  constructor(
    private readonly heartbeatService: HeartbeatService,
    private readonly redis: RedisService,
    private readonly authService: AuthService,
    private readonly objectRepository: ObjectRepository
  ) { }

  getHealth(): HealthCheckResponse {
    return {
      status: 'active',
    };
  }

  async downloadRequest(objectId: string, userId: string, byteOffset: string): Promise<Response> {
    try{
      const object = await this.objectRepository.findObjectByUserId(objectId, userId);

      if(!object){
        throw new NotFoundException('File not found');
      }

      if(BigInt(byteOffset) > BigInt(object.fileSize)){
        throw new BadRequestException('byteOffset exceeds file size');
      }

      const preSignedUrl = await this.generatePresignedUrl({
        type: "DOWNLOAD",
        objectId,
        userId,
        byteOffset
      });

      return {
        token: preSignedUrl
      }
    }catch(err){
      console.error(`${COORDINATOR} error processing download request: `, err);
      if (err instanceof HttpException) {
        throw err;
      }
      throw new InternalServerErrorException(
        'Unable to process the download request, please try again later',
      );
    }
  }

  async uploadRequest(
    uploadRequest: UploadRequestDTO,
    userId: string,
  ): Promise<Response> {
    try {
      const aliveNodes = await this.heartbeatService.getAvailabeNodes();
      const fileSize = BigInt(uploadRequest.fileSize);

      if (aliveNodes.length == 0) {
        console.log(`${COORDINATOR} no alive nodes to upload`);
        throw new NotFoundException(
          'Nodes are currently unavailable, please try again later',
        );
      }

      const nodesToStream = await this.redis.selectAndReserve(
        aliveNodes.length,
        ...aliveNodes,
        fileSize.toString(),
        this.bufferStorageSpace.toString(),
        REPLICATION_COUNT.toString(),
        UPLOAD_ROUND_ROBIN_NODE_INDEX_KEY,
      );

      if (nodesToStream.length < REPLICATION_COUNT) {
        console.log(`${COORDINATOR} not enough streamable nodes to upload`);
        throw new NotFoundException(
          'Nodes are currently filled, please try again later',
        );
      }

      const preSignedUrl = await this.generatePresignedUrl({
        type: "UPLOAD",
        userId,
        fileName: uploadRequest.fileName,
        fileSize: uploadRequest.fileSize,
        nodesToStream
      });

      return {
        token: preSignedUrl
      };
    } catch (err) {
      console.error(`${COORDINATOR} error  processing upload request: `, err);
      if (err instanceof HttpException) {
        throw err;
      }
      throw new InternalServerErrorException(
        'Unable to process the upload request, please try again later',
      );
    }
  }

  async generatePresignedUrl(data: {
    type: "UPLOAD" | "DOWNLOAD",
    [key: string]: any
  }): Promise<string> {
    const requestId = v7();
    const payload = {
      ...data,
      requestId
    };

    return this.authService.signToken(payload, '5m');
  }
}
