import {
  Injectable,
  InternalServerErrorException,
  NotFoundException,
  OnModuleDestroy,
} from '@nestjs/common';
import { open, FileHandle } from 'fs/promises';
import * as path from 'path';
import * as uuid from 'uuid';
import {
  CURRENT_BIN_FILE_KEY,
  CURRENT_BIN_FILE_OFFSET_KEY,
  BIN_FILE_SIZE,
  BIN_FILES_LOCATION,
  MAX_OPEN_HANDLES,
  CURRENT_BIN_FILE_ID_KEY,
  CURRENT_NODE_ID,
} from '@app/shared/helpers/constants';
import { RedisService } from '@app/shared/redis.service';
import { BinFileRepository } from '@app/shared/database/repository/bin-file.repository';
import { ChunkRepository } from '@app/shared/database/repository/chunk.repository';
import { ChunkReplicaRepository } from '@app/shared/database/repository/chunk-replica.repository';
import { ThrottleStream } from '@app/shared/helpers/throttle-stream';
import { BinFileStatus } from '@app/shared/database/models/bin-file.model';
import fs from "fs/promises";

export interface StorageAllocationResult {
  location: string;
  binFileId: string;
  startOffset: number;
}

@Injectable()
export class BinFileStorageService implements OnModuleDestroy {
  private nodeId: string;
  private fileHandleCache = new Map<string, FileHandle>();
  // Dynamic lock promise for file creation across concurrent requests
  private creationPromise: Promise<void> | null = null;
  private readonly binFileKey: string;
  private readonly binFileOffsetKey: string;
  private readonly binFileIdKey: string;

  constructor(
    private readonly redis: RedisService,
    private readonly binFileRepo: BinFileRepository,
    private readonly chunkRepository: ChunkRepository,
    private readonly chunkReplicaRepositry: ChunkReplicaRepository,
  ) {
    this.nodeId = CURRENT_NODE_ID;
    this.binFileKey = `${CURRENT_BIN_FILE_KEY}${this.nodeId}`;
    this.binFileOffsetKey = `${CURRENT_BIN_FILE_OFFSET_KEY}${this.nodeId}`;
    this.binFileIdKey = `${CURRENT_BIN_FILE_ID_KEY}${this.nodeId}`;
  }

  public async writeChunkToStorage(
    chunkBuffer: Buffer,
    filePath: string,
    startOffset: number,
    binFileId: string
  ): Promise<void> {
    await this.performOffsetWrite(filePath, chunkBuffer, startOffset, binFileId);
  }

  public async performOffsetReadAndWrite(
    from: {
      binFileId: string,
      startOffset: number,
      endOffset: number
    },
    to: {
      binFileId: string,
      filePath: string,
      startOffset: number
    }
  ) {
    const fromFileHandle = await this.getOrCreateHandle(null, from.binFileId);
    let writeStartOffset = to.startOffset;

    const readableStream = fromFileHandle.createReadStream({
      start: from.startOffset,
      end: from.endOffset,
      autoClose: false
    });

    for await (let chunk of readableStream) {
      await this.performOffsetWrite(to.filePath, chunk, writeStartOffset, to.binFileId);
      writeStartOffset += chunk.length;
    }
  }

  public async performOffsetRead(
    binFileId: string,
    startOffset: number,
    endOffset: number,
    stream: ThrottleStream
  ) {
    const fileHandle = await this.getOrCreateHandle(null, binFileId);

    return new Promise((resolve, reject) => {
      const readableStream = fileHandle.createReadStream({
        start: startOffset,
        end: endOffset,
        autoClose: false
      });

      readableStream.pipe(stream, { end: false });

      readableStream.on('end', () => {
        resolve(null);
      });

      readableStream.on('error', (err) => {
        reject(err);
      });
    })
  }

  public async writeChunkToDb(
    objectId: string,
    chunkIndex: number,
    chunkSize: number, // mostly 5mb, but can change if its lesser
    binFileId: string,
    byteOffset: number,
    chunkId: string,
  ) {
    const chunk = await this.chunkRepository.create({
      id: chunkId,
      objectId,
      chunkIndex,
      chunkSize,
    });
    await this.writeChunkReplicaToDb(chunk.id, binFileId, byteOffset, chunkSize);
    return chunk.id;
  }

  public async writeChunkReplicaToDb(
    chunkId: string,
    binFileId: string,
    byteOffset: number,
    chunkSize: number
  ) {
    await this.chunkReplicaRepositry.create({
      chunkId,
      nodeId: this.nodeId,
      binFileId,
      byteOffset,
      chunkSize
    });
  }

  private async performOffsetWrite(
    filePath: string | null,
    buffer: Buffer,
    startOffset: number,
    binFileId: string
  ): Promise<void> {
    // TODO: increase libuv thread pool to 64 and os file descriptors to 65536
    // every file write takes 1 libuv thread
    // every upload takes 3fd (1 file write + 1 inbound + 1 outbound)
    const handle = await this.getOrCreateHandle(filePath, binFileId);
    await handle.write(buffer, 0, buffer.byteLength, startOffset);
  }

  private async getOrCreateHandle(filePath: string | null, binFileId: string): Promise<FileHandle> {
    let handle = this.fileHandleCache.get(binFileId);

    if (handle) return handle;

    if (!filePath) {
      filePath = (await this.binFileRepo.findPathById(binFileId))?.filepath ?? null;
      if (!filePath) {
        console.error('Binfile path not found', binFileId);
        throw new NotFoundException('Bin file not found');
      }
    }

    if (!handle) {
      if (this.fileHandleCache.size >= MAX_OPEN_HANDLES) {
        await this.evictOldestHandle();
      }

      handle = await open(filePath, 'r+');
      this.fileHandleCache.set(binFileId, handle);
    }

    return handle;
  }

  public async closeFileHandle(binFileId: string, filePath: string) {
    const handle = this.fileHandleCache.get(binFileId);

    if (!handle) {
      await fs.unlink(filePath);
      return;
    }

    try {
     await  handle.close();
      this.fileHandleCache.delete(binFileId);
    } catch (err) {
      console.error('Error closing the binfile ', binFileId);
    } finally {
      await fs.unlink(filePath);
    }
  }

  private async evictOldestHandle(): Promise<void> {
    const oldestBinFileId = this.fileHandleCache.keys().next().value;

    if (oldestBinFileId) {
      const handleToClose = this.fileHandleCache.get(oldestBinFileId);
      console.info('Evicting oldest bin file handle', oldestBinFileId);
      if (handleToClose) {
        try {
          await handleToClose.close();
        } catch (err) {
          console.warn(
            `Failed to close evicted handle for ${oldestBinFileId}:`,
            err,
          );
        }
      }

      this.fileHandleCache.delete(oldestBinFileId);
    }
  }

  public async getStorageForChunk(
    totalBytes: number,
  ): Promise<StorageAllocationResult> {
    const [filePath, startOffsetStr, binFileId] =
      await this.redis.allocateChunk(
        this.binFileKey,
        this.binFileOffsetKey,
        totalBytes,
        BIN_FILE_SIZE,
      );

    if (filePath === 'NEW_FILE_NEEDED') {
      // mark the bin file as sealed, cause if the current bin file can't 
      // store a chunk it meanse, it has <5mb space, since chunk size can be
      // maximum 5mb
      await this.binFileRepo.updateStatus({ id: binFileId, status: BinFileStatus.SEALED });
      return await this.createNewBinFile(totalBytes);
    }

    return {
      binFileId,
      startOffset: Number(startOffsetStr),
      location: filePath,
    };
  }

  private async createNewBinFile(
    initialChunkBytes: number,
  ): Promise<StorageAllocationResult> {
    if (this.creationPromise) {
      await this.creationPromise;

      // re run the lua to get the data once file creation is done.
      const [filePath, startOffsetStr, currentBinfileId] =
        await this.redis.allocateChunk(
          this.binFileKey,
          this.binFileOffsetKey,
          this.binFileIdKey,
          initialChunkBytes,
          BIN_FILE_SIZE,
        );

      if (filePath !== 'NEW_FILE_NEEDED') {
        return {
          location: filePath,
          binFileId: currentBinfileId,
          startOffset: Number(startOffsetStr),
        };
      }
    }

    let resolveLock: () => void;
    this.creationPromise = new Promise((resolve) => {
      resolveLock = resolve;
    });

    try {
      const newFilePath = path.join(
        BIN_FILES_LOCATION,
        `${this.nodeId}.${uuid.v4()}.bin`,
      );

      // Allocate sparse file on physical disk FIRST
      const handle = await open(newFilePath, 'w');
      await handle.truncate(BIN_FILE_SIZE);
      await handle.close();

      const binFile = await this.binFileRepo.create({
        nodeId: this.nodeId,
        filepath: newFilePath,
      });

      // Cache the file for effecient reading/writing
      const readWriteHandle = await open(newFilePath, 'r+');
      this.fileHandleCache.set(binFile.id, readWriteHandle);

      await this.redis
        .multi()
        .set(this.binFileKey, newFilePath)
        .set(this.binFileIdKey, binFile.id)
        .set(this.binFileOffsetKey, initialChunkBytes)
        .exec();

      return {
        location: newFilePath,
        binFileId: binFile.id,
        startOffset: 0,
      };
    } catch (err) {
      console.error('Failed during bin file rollover', err);
      throw new InternalServerErrorException('Error creating bin storage file');
    } finally {
      this.creationPromise = null;
      resolveLock!();
    }
  }

  async onModuleDestroy() {
    for (const [, handle] of this.fileHandleCache.entries()) {
      await handle.close();
    }
    this.fileHandleCache.clear();
  }
}
