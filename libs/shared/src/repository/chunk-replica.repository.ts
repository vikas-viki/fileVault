import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { ChunkReplicaModel } from '../models/chunk-replica.model';

@Injectable()
export class ChunkReplicaRepository {
  constructor(@InjectModel(ChunkReplicaModel) private readonly model: typeof ChunkReplicaModel) { }

  create(attrs: {
    chunkId: string, 
    nodeId: string, 
    binFileId: string,
    byteOffset: number
  }): Promise<ChunkReplicaModel> {
    return this.model.create(attrs);
  }
}
