import { Controller } from '@nestjs/common';
import { GrpcMethod } from '@nestjs/microservices';
import { FilesService } from './files.service';
import type {
  CommitUploadRequest,
  CommitUploadResponse,
} from '@app/shared/protos/interfaces/coordinator';

@Controller()
export class UploadController {
  constructor(private readonly filesService: FilesService) {}

  @GrpcMethod('UploadService', 'CommitUpload')
  async commitUpload(
    data: CommitUploadRequest,
  ): Promise<CommitUploadResponse> {
    const ok = await this.filesService.commitUpload(
      data.fileId,
      data.chunkHashes,
      data.success,
    );
    return { ok };
  }
}
