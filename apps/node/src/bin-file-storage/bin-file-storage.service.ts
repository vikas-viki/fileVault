import {
    Injectable,
    InternalServerErrorException,
    OnModuleDestroy,
} from "@nestjs/common";
import { open, FileHandle } from "fs/promises";
import * as path from "path";
import * as uuid from "uuid";
import { CURRENT_BIN_FILE_KEY, CURRENT_BIN_FILE_OFFSET_KEY, BIN_FILE_SIZE, BIN_FILES_LOCATION, MAX_OPEN_HANDLES, NODE_INDEX_KEY, NODE_IDS } from "@app/shared/helpers/constants";
import { RedisService } from "@app/shared/redis.service";
import { BinFileRepository } from "@app/shared/repository/bin-file.repository";
import { ConfigService } from "@nestjs/config";

export interface StorageAllocationResult {
    location: string;
    startOffset: number;
}

@Injectable()
export class BinFileStorageService implements OnModuleDestroy {

    private nodeId: string;
    private fileHandleCache = new Map<string, FileHandle>();
    // Dynamic lock promise for file creation across concurrent requests
    private creationPromise: Promise<void> | null = null;

    constructor(
        private readonly redis: RedisService,
        private readonly binFileRepo: BinFileRepository,
        private readonly configService: ConfigService
    ) { 
        const nodeIndex = this.configService.get<number>(NODE_INDEX_KEY);
        if(nodeIndex != 0 && !nodeIndex){
            throw new Error("Node index not provided");
        }
        this.nodeId = NODE_IDS[nodeIndex];
    }

    public async writeChunkToStorage(chunkBuffer: Buffer): Promise<void> {
        const totalBytes = chunkBuffer.byteLength;

        // reserve and return storage
        const [filePath, startOffsetStr] = await this.redis.allocateChunk(
            CURRENT_BIN_FILE_KEY,
            CURRENT_BIN_FILE_OFFSET_KEY,
            totalBytes,
            BIN_FILE_SIZE
        );

        let targetFilePath = filePath;
        let targetStartOffset = Number(startOffsetStr);

        if (filePath === "NEW_FILE_NEEDED") {
            const newStorage = await this.createNewBinFile(totalBytes);
            targetFilePath = newStorage.location;
            targetStartOffset = newStorage.startOffset;
        }

        await this.performOffsetWrite(
            targetFilePath,
            chunkBuffer,
            targetStartOffset
        );
    }

    private async performOffsetWrite(
        filePath: string,
        buffer: Buffer,
        startOffset: number
    ): Promise<void> {
        const handle = await this.getOrCreateHandle(filePath);
        await handle.write(buffer, 0, buffer.byteLength, startOffset);
    }

    private async getOrCreateHandle(filePath: string): Promise<FileHandle> {
        let handle = this.fileHandleCache.get(filePath);

        if (!handle) {
            // Safety cap: If we reached the limit, close the oldest cached handle
            if (this.fileHandleCache.size >= MAX_OPEN_HANDLES) {
                await this.evictOldestHandle();
            }

            handle = await open(filePath, 'r+');
            this.fileHandleCache.set(filePath, handle);
        } else {
            this.fileHandleCache.delete(filePath);
            this.fileHandleCache.set(filePath, handle);
        }

        return handle;
    }

    private async evictOldestHandle(): Promise<void> {
        const oldestFilePath = this.fileHandleCache.keys().next().value;

        if (oldestFilePath) {
            const handleToClose = this.fileHandleCache.get(oldestFilePath);

            if (handleToClose) {
                try {
                    await handleToClose.close();
                } catch (err) {
                    console.warn(`Failed to close evicted handle for ${oldestFilePath}:`, err);
                }
            }

            this.fileHandleCache.delete(oldestFilePath);
        }
    }

    private async createNewBinFile(
        initialChunkBytes: number
    ): Promise<StorageAllocationResult> {
        if (this.creationPromise) {
            await this.creationPromise;

            // re run the lua to get the data once file creation is done.
            const [filePath, startOffsetStr] = await this.redis.allocateChunk(
                CURRENT_BIN_FILE_KEY,
                CURRENT_BIN_FILE_OFFSET_KEY,
                initialChunkBytes,
                BIN_FILE_SIZE
            );

            if (filePath !== "NEW_FILE_NEEDED") {
                return {
                    location: filePath,
                    startOffset: Number(startOffsetStr),
                };
            }
        }

        let resolveLock: () => void;
        this.creationPromise = new Promise((resolve) => {
            resolveLock = resolve;
        });

        try {
            const newFilePath = path.join(BIN_FILES_LOCATION, `${this.nodeId}.${uuid.v4()}.bin`);

            // Allocate sparse file on physical disk FIRST
            const handle = await open(newFilePath, "w");
            await handle.truncate(BIN_FILE_SIZE);
            await handle.close();

            await this.binFileRepo.create({
                nodeId: this.nodeId,
                fileName: newFilePath
            });

            // Cache the file for effecient reading/writing
            const readWriteHandle = await open(newFilePath, "r+");
            this.fileHandleCache.set(newFilePath, readWriteHandle);

            await this.redis
                .multi()
                .set(CURRENT_BIN_FILE_KEY, newFilePath)
                .set(CURRENT_BIN_FILE_OFFSET_KEY, initialChunkBytes)
                .exec();

            return {
                location: newFilePath,
                startOffset: 0,
            };
        } catch (err) {
            console.error("Failed during bin file rollover", err);
            throw new InternalServerErrorException("Error creating bin storage file");
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