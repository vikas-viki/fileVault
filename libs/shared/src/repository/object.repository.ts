import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { ObjectModel } from '../models/object.model';

@Injectable()
export class ObjectRepository {
  constructor(@InjectModel(ObjectModel) private readonly model: typeof ObjectModel) { }

  create(attrs: {
    userId: string, 
    fileName: string, 
    fileSize: number
  }): Promise<ObjectModel> {
    return this.model.create(attrs);
  }
}
