import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { ChunkReplicaModel } from '../models/chunk-replica.model';
import { Transaction } from 'sequelize';

@Injectable()
export class ChunkReplicaRepository {
  constructor(
    @InjectModel(ChunkReplicaModel)
    private readonly model: typeof ChunkReplicaModel,
  ) {}

  create(attrs: {
    chunkId: string;
    nodeId: string;
    binFileId: string;
    byteOffset: number;
    chunkSize: number;
  }): Promise<ChunkReplicaModel> {
    return this.model.create(attrs);
  }

  bulkUpdate(
    data: { id: string; byteOffset: number; binFileId: string }[],
    transaction?: Transaction,
  ) {
    return this.model.bulkCreate(data, {
      updateOnDuplicate: ['byteOffset', 'binFileId'], 
      transaction,
    });
  }
}
