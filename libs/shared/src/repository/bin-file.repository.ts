import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { BinFileAttributes, BinFileModel, BinFileStatus } from '../models/bin-file.model';
import { where } from 'sequelize';

@Injectable()
export class BinFileRepository {
  constructor(
    @InjectModel(BinFileModel) private readonly model: typeof BinFileModel,
  ) { }

  create(attrs: { nodeId: string; filepath: string }): Promise<BinFileModel> {
    return this.model.create(attrs);
  }

  updateStatus(attrs: { id: string, status: BinFileStatus }): Promise<[affectedCount: number]> {
    return this.model.update(
      { status: attrs.status },
      { where: { id: attrs.id } }
    )
  }

  findPathById(id: string) {
    return this.model.findByPk(id, { attributes: [BinFileAttributes.filepath] })
  }
}
