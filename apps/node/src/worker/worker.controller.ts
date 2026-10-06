import { InjectConnection } from '@nestjs/sequelize';
import { WorkerService } from './worker.service';

export class WorkerController {
    constructor(
        @InjectConnection()
        private readonly workerService: WorkerService
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

        await this.workerService.handleGarbageCollection();
    }
}
