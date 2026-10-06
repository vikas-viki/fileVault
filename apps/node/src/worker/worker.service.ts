import { BinFileStatus } from "@app/shared/database/models/bin-file.model";
import { BinFileRepository } from "@app/shared/database/repository/bin-file.repository";
import { ChunkReplicaRepository } from "@app/shared/database/repository/chunk-replica.repository";
import { CURRENT_NODE_ID, BUFFER_STORAGE_SPACE } from "@app/shared/helpers/constants";
import { InjectConnection } from "@nestjs/sequelize";
import { Sequelize } from "sequelize";
import { BinFileStorageService } from "../bin-file-storage/bin-file-storage.service";
import { NodeService } from "../node.service";

export class WorkerService {

    constructor(
        @InjectConnection()
        private readonly sequelize: Sequelize,
        private readonly binFileRepository: BinFileRepository,
        private readonly nodeService: NodeService,
        private readonly binFileStorageService: BinFileStorageService,
        private readonly chunkReplicaRepository: ChunkReplicaRepository,
        private readonly workerService: WorkerService
    ) { }
    
    async handleGarbageCollection(){
        try{
            const binFileForCompaction = await this.binFileRepository.getBinFilesForCompaction({ nodeId: CURRENT_NODE_ID });

            for (let i = 0; i < binFileForCompaction.length; i++) {
                const chunkReplicasToUpdate = [] as { id: string, binFileId: string, byteOffset: number }[]; // binFileId, chunkReplicaIds
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
                        {
                            id: currentBinFile.id
                        },
                        {
                            status: BinFileStatus.DELETED
                        }
                    );
                    await this.binFileStorageService.closeFileHandle(currentBinFile.id, currentBinFile.filepath);
                    continue;
                }
    
                await this.binFileRepository.updateStatus({ id: currentBinFile.id, status: BinFileStatus.COMPACTING });
    
                for (let j = 0; j < chunkReplicas.length; j++) {
                    // get new storage and stream the current chunk data to it.
                    const currentChunkReplica = chunkReplicas[j];
                    let {
                        startOffset: toStartOffset,
                        binFileId: toBinFileId,
                        location: toFilePath,
                    } = await this.binFileStorageService.getStorageForChunk(currentChunkReplica.chunkSize);
    
                    await this.binFileStorageService.performOffsetReadAndWrite(
                        {
                            binFileId: currentChunkReplica.binFileId,
                            startOffset: currentChunkReplica.byteOffset,
                            endOffset: currentChunkReplica.byteOffset + currentChunkReplica.chunkSize - 1
                        },
                        {
                            binFileId: toBinFileId,
                            filePath: toFilePath,
                            startOffset: toStartOffset
                        }
                    );
    
                    chunkReplicasToUpdate.push({ id: currentChunkReplica.id, byteOffset: toStartOffset, binFileId: toBinFileId });
                }
    
                await this.sequelize.transaction(async (t) => {
                    await this.binFileRepository.update(
                        {
                            id: currentBinFile.id
                        },
                        {
                            status: BinFileStatus.DELETED
                        },
                        t
                    );
    
                    await this.chunkReplicaRepository.bulkUpdate(chunkReplicasToUpdate, t);
                });
                await this.binFileStorageService.closeFileHandle(currentBinFile.id, currentBinFile.filepath);
            }
        }catch(err){
            console.error('Error garbage collecting, ', err);
        }
    }
}