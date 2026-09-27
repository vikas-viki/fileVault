import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { BinFileAttributes, BinFileModel } from '../models/bin-file.model';

@Injectable()
export class BinFileRepository {
  constructor(
    @InjectModel(BinFileModel) private readonly model: typeof BinFileModel,
  ) {}

  create(attrs: { nodeId: string; filepath: string }): Promise<BinFileModel> {
    return this.model.create(attrs);
  }

  findPathById(id: string){
    return this.model.findByPk(id, {attributes: [BinFileAttributes.filepath]})
  }
}
