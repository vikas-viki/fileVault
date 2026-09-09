import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { BinFileModel } from '../models/bin-file.model';

@Injectable()
export class BinFileRepository {
  constructor(@InjectModel(BinFileModel) private readonly model: typeof BinFileModel) { }

  create(attrs: {
    nodeId: string;
    fileName: string;
  }): Promise<BinFileModel> {
    return this.model.create(attrs);
  }
}
