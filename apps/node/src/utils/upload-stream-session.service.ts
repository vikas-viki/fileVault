import {
  STREAM_CHUNK_SIZE,
  NODE,
  BUFFER_STREAM_SIZE,
  STORAGE_CHUNK_SIZE,
  CURRENT_BIN_FILE_KEY,
  CURRENT_BIN_FILE_OFFSET_KEY,
  BIN_FILE_SIZE,
  MIN_FILE_REPLICATION,
  REPLICATION_COUNT,
} from '@app/shared/helpers/constants';
import { StreamChunkSizerService } from '@app/shared/helpers/stream-chunk-sizer';
import {
  HttpStatus,
  BadRequestException,
  HttpException,
  Inject,
} from '@nestjs/common';
import { createHash } from 'crypto';
import { Readable } from 'stream';
import { GrpcRelayWriterService } from '../grpc/grpc-relay-writer.service';
import { NodeService } from '../node.service';
import { StreamRequest } from '../node.dto';
import express from 'express';
import { GrpcClientsPoolService } from '../grpc/grpc-clients-pool.service';
import { BinFileStorageService } from '../bin-file-storage/bin-file-storage.service';
import { v7 } from 'uuid';
import { Metadata, ServerReadableStream } from '@grpc/grpc-js';
import {
  NodeStreamRequest,
  NodeStreamResponse,
} from '@app/shared/protos/interfaces/node';
import { ObjectStatus } from '@app/shared/database/models/object.model';
export class UploadStreamSessionService {
  private isAborted = false;
  private responseSent = false;
  private readonly fileSize: number;
  private remainingBytes: number;
  private relays: GrpcRelayWriterService[] = [];
  private controlledStream: Readable | null = null;

  constructor(
    private readonly nodeService: NodeService,
    private readonly grpcClientPoolService: GrpcClientsPoolService,
    private readonly binFileStorageService: BinFileStorageService,
    fileSize: number,
    private readonly experessResponse?: express.Response,
  ) {
    this.remainingBytes = fileSize;
    this.fileSize = fileSize;
  }

  async handleClientFileStream(
    fileStream: Readable,
    replicaNodes: string[],
    objectId: string,
  ) {
    try {
      const chunkSizer = new StreamChunkSizerService(STREAM_CHUNK_SIZE);
      this.controlledStream = fileStream.pipe(chunkSizer);

      // Attach error listener immediately to prevent unhandled stream errors
      fileStream.on('error', (err) => this.abort(err, fileStream, objectId));

      const metadata = new Metadata();
      metadata.add('file-size', this.fileSize.toString());
      metadata.add('object-id', objectId);
      this.relays = await Promise.all(
        replicaNodes.map((node) =>
          this.grpcClientPoolService.connectToReplica(node, metadata),
        ),
      );

      const connectionResults = await Promise.allSettled(
        replicaNodes.map((node) =>
          this.grpcClientPoolService.connectToReplica(node, metadata),
        ),
      );

      const failedStreamNodes = new Set<string>();
      this.relays = [];

      connectionResults.forEach((res, index) => {
        if (res.status === 'fulfilled') {
          this.relays.push(res.value);
        } else {
          console.error(
            `Failed to connect to replica ${replicaNodes[index]}:`,
            res.reason,
          );
          failedStreamNodes.add(replicaNodes[index]);
        }
      });

      if (failedStreamNodes.size > REPLICATION_COUNT - MIN_FILE_REPLICATION) {
        throw new HttpException(
          'Insufficient nodes available to satisfy quorum',
          HttpStatus.SERVICE_UNAVAILABLE,
        );
      }

      await this.processClientStream(this.controlledStream, objectId);

      if (this.isAborted) return;

      const healthyRelays = this.relays.filter(
        (r) => !failedStreamNodes.has(r.nodeId),
      );
      await Promise.allSettled(healthyRelays.map((r) => r.end()));

      await this.nodeService.updateObjectStatus(objectId, ObjectStatus.COMPLETED);
      console.log(`${NODE} fanned out chunks to all replicas successfully`);
      this.sendResponse(HttpStatus.CREATED, 'File uploaded successfully');
    } catch (err) {
      console.error('Error handling node stream: ', err);
      this.abort(err, fileStream, objectId);
    }
  }

  private async processClientStream(stream: Readable, objectId: string) {
    let length = 0;
    let chunkIndex = 0;
    let fileChunkSizeToAllocate = Math.min(
      this.remainingBytes,
      STORAGE_CHUNK_SIZE,
    );
    let {
      startOffset,
      binFileId,
      location: filePath,
    } = await this.binFileStorageService.getStorageForChunk(
      fileChunkSizeToAllocate,
    );
    let containerBaseOffset = startOffset;
    let currentWriteOffset = startOffset;
    let currentChunkId = v7();
    let chunksToRelay: { chunk: Buffer; chunkId: string }[] = [];
    const failedStreamNodes = new Set();

    for await (const controlledChunk of stream) {
      if (this.isAborted) return;

      const chunk = controlledChunk as Buffer;
      const chunkLength = chunk.length;
      chunksToRelay = [{ chunk, chunkId: currentChunkId }];

      if (this.remainingBytes - chunkLength + BUFFER_STREAM_SIZE < 0) {
        console.error('File size exceeded expected number of bytes');
        throw new BadRequestException(
          'File size exceeded expected number of bytes',
        );
      }
      this.remainingBytes -= chunkLength;

      if (length + chunkLength > STORAGE_CHUNK_SIZE) {
        const spaceLeftInOldBin = STORAGE_CHUNK_SIZE - length;
        const [oldChunk, newChunk] = this.splitChunk(chunk, spaceLeftInOldBin);

        await this.binFileStorageService.writeChunkToStorage(
          oldChunk,
          filePath,
          currentWriteOffset,
          binFileId
        );
        await this.binFileStorageService.writeChunkToDb(
          objectId,
          chunkIndex,
          STORAGE_CHUNK_SIZE,
          binFileId,
          containerBaseOffset,
          currentChunkId,
        );
        chunksToRelay = [{ chunk: oldChunk, chunkId: currentChunkId }];
        currentChunkId = v7();
        chunkIndex++;

        // Allocate and switch to new container file
        const nextAllocationSize = Math.min(
          this.remainingBytes + newChunk.length,
          STORAGE_CHUNK_SIZE,
        );
        ({
          startOffset,
          binFileId,
          location: filePath,
        } = await this.binFileStorageService.getStorageForChunk(
          nextAllocationSize,
        ));

        containerBaseOffset = startOffset;
        currentWriteOffset = startOffset;

        // Write remainder into new container file
        await this.binFileStorageService.writeChunkToStorage(
          newChunk,
          filePath,
          currentWriteOffset,
          binFileId
        );
        currentWriteOffset += newChunk.length;
        length = newChunk.length;

        chunksToRelay.push({ chunk: newChunk, chunkId: currentChunkId });
      } else {
        await this.binFileStorageService.writeChunkToStorage(
          chunk,
          filePath,
          currentWriteOffset,
          binFileId
        );
        currentWriteOffset += chunkLength;
        length += chunkLength;
      }

      const healthyNodes = this.relays.filter(
        (r) => !failedStreamNodes.has(r.nodeId),
      );
      const chunksRelayed = await Promise.allSettled(
        healthyNodes.flatMap((r) =>
          chunksToRelay.map((c) =>
            r
              .write({ chunk: c.chunk, chunkId: c.chunkId })
              .then((d) => ({ success: true, nodeId: r.nodeId }))
              .catch((e) => {
                console.error(
                  'Chunk streaming failed: ',
                  e,
                  ' nodeId: ',
                  r.nodeId,
                );
                return { success: false, nodeId: r.nodeId };
              }),
          ),
        ),
      );

      for (const result of chunksRelayed) {
        if (result.status === 'fulfilled' && !result.value.success) {
          failedStreamNodes.add(result.value.nodeId);
        }
      }

      if (failedStreamNodes.size > REPLICATION_COUNT - MIN_FILE_REPLICATION) {
        this.abort('Quorum failed', stream, objectId);
        return;
      }

      this.nodeService.increaseAllocatedSpace(chunkLength);
    }

    if (this.remainingBytes > 0) {
      console.log('Stream end prematurely');
      throw new Error('Stream end prematurely');
    }

    if (length > 0) {
      await this.binFileStorageService.writeChunkToDb(
        objectId,
        chunkIndex,
        length,
        binFileId,
        containerBaseOffset,
        currentChunkId,
      );
    }
  }

  async processNodeStream(
    stream: ServerReadableStream<NodeStreamRequest, NodeStreamResponse>,
    objectId: string,
  ) {
    try {
      let length = 0;
      let fileChunkSizeToAllocate = Math.min(
        this.remainingBytes,
        STORAGE_CHUNK_SIZE,
      );
      let {
        startOffset,
        binFileId,
        location: filePath,
      } = await this.binFileStorageService.getStorageForChunk(
        fileChunkSizeToAllocate,
      );
      let containerBaseOffset = startOffset;
      let currentWriteOffset = startOffset;
      let currentChunkId = '';

      for await (const data of stream) {
        if (this.isAborted) return;
        const { chunkId, chunk: _chunk } = data as NodeStreamRequest;
        const chunkBuffer = Buffer.from(
          _chunk.buffer,
          _chunk.byteOffset,
          _chunk.byteLength,
        );

        const chunkLength = chunkBuffer.length;

        if (this.remainingBytes - chunkLength + BUFFER_STREAM_SIZE < 0) {
          console.error('File size exceeded expected number of bytes');
          throw new BadRequestException(
            'File size exceeded expected number of bytes',
          );
        }

        this.remainingBytes -= chunkLength;

        if (length === STORAGE_CHUNK_SIZE) {
          await this.binFileStorageService.writeChunkReplicaToDb(
            currentChunkId,
            binFileId,
            containerBaseOffset,
            STORAGE_CHUNK_SIZE
          );

          const nextAllocationSize = Math.min(
            this.remainingBytes + chunkLength,
            STORAGE_CHUNK_SIZE,
          );
          ({
            startOffset,
            binFileId,
            location: filePath,
          } = await this.binFileStorageService.getStorageForChunk(
            nextAllocationSize,
          ));

          containerBaseOffset = startOffset;
          currentWriteOffset = startOffset;
          length = 0;
        }

        currentChunkId = chunkId;
        await this.binFileStorageService.writeChunkToStorage(
          chunkBuffer,
          filePath,
          currentWriteOffset,
          binFileId
        );
        length += chunkLength;
        currentWriteOffset += chunkLength;
      }

      if (this.remainingBytes > 0) {
        console.log('Stream end prematurely');
        throw new Error('Stream end prematurely');
      }

      if (length > 0) {
        await this.binFileStorageService.writeChunkReplicaToDb(
          currentChunkId,
          binFileId,
          containerBaseOffset,
          length
        );
      }
    } catch (err) {
      console.error('Error handling replica node stream:', err);
      this.abort(err, stream, objectId);
      throw err;
    }
  }

  private splitChunk(chunk: Buffer, size: number): [Buffer, Buffer] {
    return [chunk.subarray(0, size), chunk.subarray(size)];
  }

  public async abort(err: any, fileStream: Readable, objectId: string) {
    console.log('Aborting upload', err, objectId);
    if (this.isAborted) return;
    this.isAborted = true;

    this.relays.forEach((r) => r.cancel());

    if (this.controlledStream && !this.controlledStream.destroyed) {
      this.controlledStream.destroy(err);
    }

    fileStream.destroy();

    await this.nodeService.updateObjectStatus(objectId, ObjectStatus.ABORTED);
    this.sendError(err);
  }

  public sendResponse(statusCode: number, message: string) {
    if (this.responseSent) return;
    this.responseSent = true;
    if (!this.experessResponse?.headersSent) {
      this.experessResponse?.status(statusCode).json({ message });
    }
  }

  public sendError(err: any) {
    console.error(`${NODE} error uploading/downstreaming file: `, err);
    const statusCode =
      err instanceof HttpException
        ? err.getStatus()
        : HttpStatus.INTERNAL_SERVER_ERROR;
    const message =
      err instanceof HttpException ? err.message : 'Error uploading the file';
    this.sendResponse(statusCode, message);
  }
}
