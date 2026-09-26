import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { ObjectModel, ObjectStatus } from '../models/object.model';
import { where } from 'sequelize';
import { UserModel } from '../models/user.model';

@Injectable()
export class ObjectRepository {
  constructor(
    @InjectModel(ObjectModel) private readonly model: typeof ObjectModel,
  ) { }

  create(attrs: {
    userId: string;
    fileName: string;
    fileSize: number;
  }): Promise<ObjectModel> {
    return this.model.create(attrs);
  }

  findById(id: string) {
    return this.model.findByPk(id);
  }

  findObjectByUserId(objectId: string, userId: string) {
    return this.model.findOne({
      where: {
        id: objectId,
        userId
      }
    })
  }

  async updateStatus(attrs: { id: string; status: ObjectStatus }): Promise<void> {
    await this.model.update(
      { status: attrs.status },
      { where: { id: attrs.id } }
    );
  }
}
