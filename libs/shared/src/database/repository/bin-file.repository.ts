import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { BinFileAttributes, BinFileModel, BinFileStatus } from '../models/bin-file.model';
import { Op, where, WhereOptions } from 'sequelize';
import { BIN_FILE_COMPACTION_THRESHOLD } from '@app/shared/helpers/constants';
import { ChunkReplicaAttributes, ChunkReplicaModel } from '../models/chunk-replica.model';
import { ChunkAttributes, ChunkModel } from '../models/chunk.model';

@Injectable()
export class BinFileRepository {
  constructor(
    @InjectModel(BinFileModel) private readonly model: typeof BinFileModel,
  ) { }

  create(attrs: { nodeId: string; filepath: string }): Promise<BinFileModel> {
    return this.model.create(attrs);
  }

  getBinFilesForCompaction(attrs: { nodeId: string })
    : Promise<(BinFileModel & { chunkReplicas?: ChunkReplicaModel[] })[]> {
    return this.model.findAll(
      {
        where: {
          nodeId: attrs.nodeId,
          allocatedSpace: {
            [Op.lte]: BIN_FILE_COMPACTION_THRESHOLD
          },
          status: BinFileStatus.SEALED
        },
        include: [
          {
            model: ChunkReplicaModel,
            as: 'chunkReplicas',
            required: false,
            attributes: [ChunkReplicaAttributes.chunkSize]
          }
        ],
        order: [
          [{ model: ChunkReplicaModel, as: 'chunkReplicas' }, ChunkReplicaAttributes.byteOffset, 'ASC'],
        ]
      }) as Promise<(BinFileModel & { chunkReplicas?: ChunkReplicaModel[] })[]>;
  }

  updateStatus(attrs: { id: string, status: BinFileStatus }): Promise<[affectedCount: number]> {
    return this.model.update(
      { status: attrs.status },
      { where: { id: attrs.id } }
    )
  }

  update(where: WhereOptions<any>, data: object): Promise<[affectedCount: number]>{
    return this.model.update(
      data,
      { where });
  }

  findPathById(id: string) {
    return this.model.findByPk(id, { attributes: [BinFileAttributes.filepath] })
  }
}
