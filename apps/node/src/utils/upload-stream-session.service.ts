import { STREAM_CHUNK_SIZE, NODE, BUFFER_STREAM_SIZE,  CHUNK_SIZE } from "@app/shared/helpers/constants";
import { StreamChunkSizerService } from "@app/shared/helpers/stream-chunk-sizer";
import { HttpStatus, BadRequestException, HttpException, Inject } from "@nestjs/common";
import { createHash } from "crypto";
import { Readable } from "stream";
import { GrpcRelayWriterService } from "../grpc/grpc-relay-writer.service";
import { NodeService } from "../node.service";
import { StreamRequest } from "../node.dto";
import express from "express";
import { GrpcClientsPoolService } from "../grpc/grpc-clients-pool.service";

export class UploadStreamSession {
    private isAborted = false;
    private responseSent = false;
    private remainingBytes: bigint;
    private readonly chunkHashes: string[] = [];
    private relays: GrpcRelayWriterService[] = [];
    private controlledStream: Readable | null = null;

    constructor(
        private readonly service: NodeService,
        private readonly response: express.Response,
        private readonly data: StreamRequest,
        private readonly grpcClientPoolService: GrpcClientsPoolService
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
                replicaNodes.map((node) => this.grpcClientPoolService.connectToReplica(node)),
            );

            await this.processChunks(this.controlledStream);

            if (this.isAborted) return;

            await Promise.all(this.relays.map((r) => r.end()));
            
            // write to db

            console.log(`${NODE} fanned out chunks to all replicas successfully`);
            this.sendResponse(HttpStatus.CREATED, 'File uploaded successfully');
        } catch (err) {
            console.error('Error processing chunk: ', err);
            this.abort(err);
        }
    }

    private async processChunks(stream: Readable) {
        let length = 0;
        
        for await (const controlledChunk of stream) {
            if (this.isAborted) return;

            let chunk = controlledChunk as Buffer;
            length += controlledChunk.length;

            // TODO: 
            // get storage for chunk
            // check if the current chunk size != 5mb, if so split it such that previous chunk gets filled to 5mb
            // write the chunk data to given offset
            // write to db if the current chunk hits 5mb

            while (length > CHUNK_SIZE) {
                // split 
                const [currentChunk, remainingChunk] = this.splitChunk(controlledChunk, CHUNK_SIZE);

                // store first slice
                // get new storage 
                // store into file
                // todo: write to db
            }


            const hash = createHash('sha256').update(chunk).digest('hex');
            this.chunkHashes.push(hash);
            this.remainingBytes -= BigInt(chunk.length);

            if (this.remainingBytes + BigInt(BUFFER_STREAM_SIZE) < 0n) {
                throw new BadRequestException('File size exceeded expected number of bytes');
            }

            await Promise.all([
                // wrtie to disk
                ...this.relays.map((r) => r.write({ chunk, chunkHash: hash })),
            ]);

            this.service.allocatedSpaceSinceLastHeartbeat += chunk.length;
        }
    }

    private splitChunk(chunk: Buffer, size: number): [Buffer, Buffer] {
        return [chunk.subarray(0, size), chunk.subarray(size)];
    }

    public abort(err: any) {
        if (this.isAborted) return;
        this.isAborted = true;

        this.relays.forEach((r) => r.cancel());
        if (this.controlledStream && !this.controlledStream.destroyed) {
            this.controlledStream.destroy(err);
        }

        // TODO: write to db
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