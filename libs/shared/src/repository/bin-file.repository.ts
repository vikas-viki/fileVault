import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { BinFileModel } from '../models/bin-file.model';

@Injectable()
export class BinFileRepository {
  constructor(@InjectModel(BinFileModel) private readonly model: typeof BinFileModel) { }

  findByEmail(fileName: string): Promise<BinFileModel | null> {
    return this.model.findOne({ where: { fileName } });
  }

  create(attrs: {
    nodeId: string;
    fileName: string;
  }): Promise<BinFileModel> {
    return this.model.create(attrs);
  }

  async findOrCreate(email: string, name: string): Promise<BinFileModel> {
    const [binFile] = await this.model.findOrCreate({
      where: {
        email
      },
      defaults: {
        name, email
      }
    });
    return binFile;
  }
}
