import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { ChunkAttributes, ChunkModel } from '../models/chunk.model';
import { Op } from 'sequelize';
import { ChunkReplicaAttributes, ChunkReplicaModel } from '../models/chunk-replica.model';

export type ChunkWithReplicas = ChunkModel & {
  chunkReplica: ChunkReplicaModel[];
};

@Injectable()
export class ChunkRepository {
  constructor(
    @InjectModel(ChunkModel) private readonly model: typeof ChunkModel,
  ) { }

  create(attrs: {
    id: string;
    objectId: string;
    chunkIndex: number;
    chunkSize: number;
  }): Promise<ChunkModel> {
    return this.model.create(attrs);
  }

  getChunksCountByObjectId(attrs: {
    objectId: string,
    chunkIndex: number
  }) {
    return this.model.count({
      where: {
        objectId: attrs.objectId,
        chunkIndex: {
          [Op.gte]: attrs.chunkIndex
        },
        deletedAt: null
      }
    })
  }

  getChunksByObjectIdAndChunkIndex(attrs: {
    objectId: string,
    chunkIndex: number,
    limit: number,
    offset: number,
    nodeId: string
  }): Promise<ChunkWithReplicas[]> {
    return this.model.findAll({
      attributes: [ChunkAttributes.chunkSize],
      where: {
        objectId: attrs.objectId,
        chunkIndex: {
          [Op.gte]: attrs.chunkIndex
        },
        deletedAt: null
      },
      include: [
        {
          model: ChunkReplicaModel,
          as: 'chunkReplica',
          attributes: [
            ChunkReplicaAttributes.binFileId,
            ChunkReplicaAttributes.byteOffset
          ],
          where: {
            nodeId: attrs.nodeId,
            deleteAt: null
          }
        }
      ],
      order: [['chunkIndex', 'ASC']],
      limit: attrs.limit,
      offset: attrs.offset
    }) as Promise<ChunkWithReplicas[]>;
  }

}
