import { BinFileRepository } from '@app/shared/database/repository/bin-file.repository';
import { BUFFER_STORAGE_SPACE, CURRENT_NODE_ID } from '@app/shared/helpers/constants';
import { NodeService } from '../node.service';
import { BinFileStatus } from '@app/shared/database/models/bin-file.model';
import { BinFileStorageService } from '../bin-file-storage/bin-file-storage.service';

export class WorkerController {
    // assume this listents to sqs message

    constructor(
        private readonly binFileRepository: BinFileRepository,
        private readonly nodeService: NodeService,
        private readonly binFileStorageService: BinFileStorageService
    ) { }

    async garbageCollectorHandler(message: {}) {
        // 1. Fetch candidate binFiles for compaction and mark status as 'COMPACTING'
        //    (Acts as a job lock to prevent concurrent GC workers; active user reads remain unaffected).
        // 2. Stream surviving chunks into a new binFile (copy-on-write).
        // 3. Execute atomic DB transaction:
        //    - Point ChunkReplicas to the new binFile and updated byte offsets.
        //    - Mark old binFile as 'DELETED'.
        // 4. Call fs.unlink() immediately on the old binFile path post-transaction.
        //    - POSIX/Linux removes the directory entry so new requests cleanly target the new binFile.
        //    - Active streams keep their open file handles alive without interruption.
        //    - OS automatically reclaims disk space the moment active downloads finish and handles close.

        // TODO: get only required attributes from binfFileModel once whole flow is done
        const binFileForCompaction = await this.binFileRepository.getBinFilesForCompaction({ nodeId: CURRENT_NODE_ID });

        for (let i = 0; i < binFileForCompaction.length; i++) {
            const availableSpace = Number(this.nodeService.getAvailableSpaceInBytes());
            const shouldContinue = availableSpace > (BUFFER_STORAGE_SPACE / 2);

            if (!shouldContinue) {
                console.info("Skipping compaction due to less available storage space");
                break;
            }

            const currentBinFile = binFileForCompaction[i];
            const chunkReplicas = currentBinFile.chunkReplicas ?? [];
            if (chunkReplicas.length == 0) {
                await this.binFileRepository.update(
                    { id: currentBinFile.id },
                    { status: BinFileStatus.DELETED }
                );
                await this.binFileStorageService.closeFileHandle(currentBinFile.id);
                continue;
            }
            await this.binFileRepository.updateStatus({ id: currentBinFile.id, status: BinFileStatus.COMPACTING });

            for (let j = 0; j < chunkReplicas.length; j++) {
                // get new storage and stream the current chunk data to it.
                const currentChunkReplica = chunkReplicas[j];
                let {
                    startOffset,
                    binFileId,
                    location: filePath,
                } = await this.binFileStorageService.getStorageForChunk(currentChunkReplica.chunkSize);
            }
        }
    }
}
