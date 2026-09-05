import { STREAM_CHUNK_SIZE, NODE, BUFFER_STREAM_SIZE, NODE_FILES_WRITE_PATH, NODE_IDENTIFIER } from "@app/shared/helpers/constants";
import { StreamChunkSizerService } from "@app/shared/helpers/stream-chunk-sizer";
import { InternalServerErrorException, HttpStatus, BadRequestException, HttpException } from "@nestjs/common";
import { createHash } from "crypto";
import { Readable } from "stream";
import { GrpcRelayWriter } from "./grpc-relay-writer";
import { NodeService } from "../node.service";
import { StreamRequest } from "../node.type";
import express from "express";

export class UploadStreamSession {
    private isAborted = false;
    private responseSent = false;
    private remainingBytes: bigint;
    private readonly chunkHashes: string[] = [];
    private relays: GrpcRelayWriter[] = [];
    private controlledStream: Readable | null = null;
  
    constructor(
      private readonly service: NodeService,
      private readonly response: express.Response,
      private readonly data: StreamRequest,
    ) {
      this.remainingBytes = BigInt(data.fileSize);
    }
  
    async handleFileStream(fileStream: Readable, replicaNodes: string[]) {
      try {
        const chunkSizer = new StreamChunkSizerService(STREAM_CHUNK_SIZE);
        this.controlledStream = fileStream.pipe(chunkSizer);
  
        // Attach error listener immediately to prevent unhandled stream errors
        fileStream.on('error', (err) => this.abort(err));
  
        this.relays = await Promise.all(
          replicaNodes.map((node) => this.service.connectToReplica(node)),
        );
  
        await this.processChunks(this.controlledStream);
  
        if (this.isAborted) return;
  
        await Promise.all(this.relays.map((r) => r.end()));
        const committed = await this.service.commitUpload(this.data.fileId, this.chunkHashes, true);
  
        if (!committed) {
          throw new InternalServerErrorException('Failed to commit upload metadata');
        }
  
        console.log(`${NODE} fanned out chunks to all replicas successfully`);
        this.sendResponse(HttpStatus.CREATED, 'File uploaded successfully');
      } catch (err) {
        this.abort(err);
      }
    }
  
    private async processChunks(stream: Readable) {
      for await (const controlledChunk of stream) {
        if (this.isAborted) return;
  
        const chunk = controlledChunk as Buffer;
        const hash = createHash('sha256').update(chunk).digest('hex');
        this.chunkHashes.push(hash);
        this.remainingBytes -= BigInt(chunk.length);
  
        if (this.remainingBytes + BigInt(BUFFER_STREAM_SIZE) < 0n) {
          throw new BadRequestException('File size exceeded expected number of bytes');
        }
  
        await Promise.all([
          this.service.writeChunkToDisk(chunk, [
            NODE_FILES_WRITE_PATH,
            NODE_IDENTIFIER,
            hash,
          ]),
          ...this.relays.map((r) => r.write({ chunk, chunkHash: hash })),
        ]);
  
        this.service.allocatedSpaceSinceLastHeartbeat += chunk.length;
      }
    }
  
    public abort(err: any) {
      if (this.isAborted) return;
      this.isAborted = true;
  
      this.relays.forEach((r) => r.cancel());
      if (this.controlledStream && !this.controlledStream.destroyed) {
        this.controlledStream.destroy(err);
      }
  
      void this.service.commitUpload(this.data.fileId, [], false);
      this.sendError(err);
    }
  
    public sendResponse(statusCode: number, message: string) {
      if (this.responseSent) return;
      this.responseSent = true;
      if (!this.response.headersSent) {
        this.response.status(statusCode).json({ message });
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