import { BinFileRepository } from '@app/shared/database/repository/bin-file.repository';

export class WorkerController {
    // assume this listents to sqs message

    constructor(private readonly binFileRepository: BinFileRepository){}

    async garbageCollectorHandler(message: {}) {
        // get all the binFiles in current node, 
        // loop through them, for each 
    }
}
