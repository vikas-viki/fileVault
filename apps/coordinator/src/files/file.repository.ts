import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { UniqueConstraintError } from 'sequelize';
import { FileModel, FileStatus } from '../../../../libs/shared/src/models/file.model';

@Injectable()
export class FileRepository {
  constructor(
    @InjectModel(FileModel) private readonly model: typeof FileModel,
  ) {}

  // Returns null when the name collides with a live file (unique violation).
  async create(attrs: {
    userId: string;
    fileName: string;
    size: string;
    nodes: string[];
    status: FileStatus;
  }): Promise<FileModel | null> {
    try {
      return await this.model.create(attrs);
    } catch (err) {
      if (err instanceof UniqueConstraintError) return null;
      throw err;
    }
  }

  findById(id: string): Promise<FileModel | null> {
    return this.model.findByPk(id);
  }

  save(file: FileModel): Promise<FileModel> {
    return file.save();
  }
}
