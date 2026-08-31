import { Module } from '@nestjs/common';
import { SequelizeModule } from '@nestjs/sequelize';
import { FileModel } from '../../../../libs/shared/src/models/file.model';
import { FilesService } from './files.service';
import { FileRepository } from './file.repository';
import { UploadController } from './upload.controller';

@Module({
  imports: [SequelizeModule.forFeature([FileModel])],
  controllers: [UploadController],
  providers: [FilesService, FileRepository],
  exports: [FilesService],
})
export class FilesModule {}
