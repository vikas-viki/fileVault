import { STREAM_CHUNK_SIZE, NODE, BUFFER_STREAM_SIZE, STORAGE_CHUNK_SIZE, CURRENT_BIN_FILE_KEY, CURRENT_BIN_FILE_OFFSET_KEY, BIN_FILE_SIZE } from "@app/shared/helpers/constants";
import { StreamChunkSizerService } from "@app/shared/helpers/stream-chunk-sizer";
import { HttpStatus, BadRequestException, HttpException, Inject } from "@nestjs/common";
import { createHash } from "crypto";
import { Readable } from "stream";
import { GrpcRelayWriterService } from "../grpc/grpc-relay-writer.service";
import { NodeService } from "../node.service";
import { StreamRequest } from "../node.dto";
import express from "express";
import { GrpcClientsPoolService } from "../grpc/grpc-clients-pool.service";
import { BinFileStorageService } from "../bin-file-storage/bin-file-storage.service";
import { RedisService } from "@app/shared/redis.service";

export class UploadStreamSessionService {
    private isAborted = false;
    private responseSent = false;
    private remainingBytes;
    private relays: GrpcRelayWriterService[] = [];
    private controlledStream: Readable | null = null;

    constructor(
        private readonly nodeService: NodeService,
        private readonly grpcClientPoolService: GrpcClientsPoolService,
        private readonly binFileStorageService: BinFileStorageService,
        data: StreamRequest,
        private readonly response: express.Response
    ) {
        this.remainingBytes = data.fileSize;
    }

    async handleFileStream(fileStream: Readable, replicaNodes: string[], objectId: string) {
        try {
            const chunkSizer = new StreamChunkSizerService(STREAM_CHUNK_SIZE);
            this.controlledStream = fileStream.pipe(chunkSizer);

            // Attach error listener immediately to prevent unhandled stream errors
            fileStream.on('error', (err) => this.abort(err, fileStream));

            this.relays = await Promise.all(
                replicaNodes.map((node) => this.grpcClientPoolService.connectToReplica(node)),
            );

            await this.processChunks(this.controlledStream, objectId);

            if (this.isAborted) return;

            await Promise.all(this.relays.map((r) => r.end()));

            // mark as completed once quorun

            console.log(`${NODE} fanned out chunks to all replicas successfully`);
            this.sendResponse(HttpStatus.CREATED, 'File uploaded successfully');
        } catch (err) {
            console.error('Error processing chunk: ', err);
            this.abort(err, fileStream);
        }
    }

    private async processChunks(stream: Readable, objectId: string) {
        let length = 0;
        let chunkIndex = 0;
        let fileChunkSizeToAllocate = Math.min(this.remainingBytes, STORAGE_CHUNK_SIZE);
        let { startOffset, binFileId, location: filePath } = await this.binFileStorageService.getStroageForChunk(fileChunkSizeToAllocate);
        let containerBaseOffset = startOffset;
        let currentWriteOffset = startOffset;
    
        for await (const controlledChunk of stream) {
            if (this.isAborted) return;
    
            const chunk = controlledChunk as Buffer;
            const chunkLength = chunk.length;
            this.remainingBytes -= chunkLength;
    
            if (this.remainingBytes + BUFFER_STREAM_SIZE < 0) {
                console.error('File size exceeded expected number of bytes');
                throw new BadRequestException('File size exceeded expected number of bytes');
            }
    
            if (length + chunkLength > STORAGE_CHUNK_SIZE) {
                const spaceLeftInOldBin = STORAGE_CHUNK_SIZE - length;
                const [oldChunk, newChunk] = this.splitChunk(chunk, spaceLeftInOldBin);
    
                // 1. Fill and finalize current container file
                await this.binFileStorageService.writeChunkToStorage(oldChunk, filePath, currentWriteOffset);
                await this.binFileStorageService.writeChunkToDb(
                    objectId,
                    chunkIndex,
                    STORAGE_CHUNK_SIZE,
                    binFileId,
                    containerBaseOffset
                );
    
                chunkIndex++;
    
                // 2. Allocate and switch to new container file
                const nextAllocationSize = Math.min(this.remainingBytes + newChunk.length, STORAGE_CHUNK_SIZE);
                ({ startOffset, binFileId, location: filePath } = await this.binFileStorageService.getStroageForChunk(nextAllocationSize));
    
                containerBaseOffset = startOffset;
                currentWriteOffset = startOffset;
    
                // 3. Write remainder into new container file
                await this.binFileStorageService.writeChunkToStorage(newChunk, filePath, currentWriteOffset);
                currentWriteOffset += newChunk.length;
                length = newChunk.length;
            } else {
                await this.binFileStorageService.writeChunkToStorage(chunk, filePath, currentWriteOffset);
                currentWriteOffset += chunkLength;
                length += chunkLength;
            }
    
            const hash = createHash('sha256').update(chunk).digest('hex');
            // TODO: handle quorum later
            await Promise.allSettled([
                ...this.relays.map((r) => r.write({ chunk, chunkHash: hash })),
            ]);
    
            this.nodeService.increaseAllocatedSpace(chunk.length);
        }
    
        if (this.remainingBytes > 0) {
            console.log('Stream end a-mid');
            throw new Error('Stream end a-mid');
        }
    
        if (length > 0) {
            await this.binFileStorageService.writeChunkToDb(
                objectId,
                chunkIndex,
                length,
                binFileId,
                containerBaseOffset
            );
        }
    }

    private splitChunk(chunk: Buffer, size: number): [Buffer, Buffer] {
        return [chunk.subarray(0, size), chunk.subarray(size)];
    }

    public abort(err: any, fileStream: Readable) {
        console.log('Aborting upload', err);
        if (this.isAborted) return;
        this.isAborted = true;

        this.relays.forEach((r) => r.cancel());

        if (this.controlledStream && !this.controlledStream.destroyed) {
            this.controlledStream.destroy(err);
        }

        fileStream.destroy();

        // TODO: write to db update status later
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