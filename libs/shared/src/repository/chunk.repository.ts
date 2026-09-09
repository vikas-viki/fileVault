import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { ChunkModel } from '../models/chunk.model';

@Injectable()
export class ChunkRepository {
  constructor(@InjectModel(ChunkModel) private readonly model: typeof ChunkModel) { }

  create(attrs: {
    objectId: string, 
    chunkIndex: number, 
    chunkSize: number 
  }): Promise<ChunkModel> {
    return this.model.create(attrs);
  }
}
