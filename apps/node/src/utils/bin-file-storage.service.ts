import {
    Injectable,
    Inject,
    InternalServerErrorException,
    OnModuleDestroy,
} from "@nestjs/common";
import Redis from "ioredis";
import { open, FileHandle } from "fs/promises";
import * as path from "path";
import * as uuid from "uuid";
import { REDIS_CLIENT, ALLOCATE_CHUNK_STORAGE_LUA_KEY, CURRENT_BIN_FILE_KEY, CURRENT_BIN_FILE_OFFSET_KEY, BIN_FILE_SIZE, BIN_FILES_LOCATION, MAX_OPEN_HANDLES } from "@app/shared/helpers/constants";

export interface StorageAllocationResult {
    location: string;
    startOffset: number;
}

@Injectable()
export class BinFileStorageService implements OnModuleDestroy {

    private fileHandleCache = new Map<string, FileHandle>();
    // Dynamic lock promise for file creation across concurrent requests
    private creationPromise: Promise<void> | null = null;

    constructor(
        @Inject(REDIS_CLIENT) private readonly redis: Redis
    ) { }

    public async writeChunkToStorage(chunkBuffer: Buffer): Promise<void> {
        const totalBytes = chunkBuffer.byteLength;

        // reserve and return storage
        const [filePath, startOffsetStr] = await this.redis[
            ALLOCATE_CHUNK_STORAGE_LUA_KEY
        ](
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
            const [filePath, startOffsetStr] = await this.redis[
                ALLOCATE_CHUNK_STORAGE_LUA_KEY
            ](
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
            const newFilePath = path.join(BIN_FILES_LOCATION, `${uuid.v4()}.bin`);

            // Allocate sparse file on physical disk FIRST
            const handle = await open(newFilePath, "w");
            await handle.truncate(BIN_FILE_SIZE);
            await handle.close();

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