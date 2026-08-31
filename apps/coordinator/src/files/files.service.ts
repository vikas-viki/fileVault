import {
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { FileModel, FileStatus } from '../../../../libs/shared/src/models/file.model';
import { FileRepository } from './file.repository';

// Inserts " (n)" before the extension: "a.txt" -> "a (1).txt", ".env" -> ".env (1)".
function suffixName(name: string, n: number): string {
  const dot = name.lastIndexOf('.');
  if (dot <= 0) return `${name} (${n})`;
  return `${name.slice(0, dot)} (${n})${name.slice(dot)}`;
}

@Injectable()
export class FilesService {
  constructor(private readonly fileRepo: FileRepository) {}

  async createPendingFile(
    userId: string,
    fileName: string,
    size: string,
    nodes: string[],
  ): Promise<FileModel> {
    for (let attempt = 0; attempt < 100; attempt++) {
      const candidate = attempt === 0 ? fileName : suffixName(fileName, attempt);
      const file = await this.fileRepo.create({
        userId,
        fileName: candidate,
        size,
        nodes,
        status: FileStatus.INPROGRESS,
      });
      if (file) return file;
    }
    throw new ConflictException('Could not allocate a unique file name');
  }

  async getFileForDownload(fileId: string, userId: string): Promise<FileModel> {
    const file = await this.fileRepo.findById(fileId);
    if (!file || file.userId !== userId || file.status !== FileStatus.ACTIVE) {
      throw new NotFoundException('File not found');
    }
    return file;
  }

  async commitUpload(
    fileId: string,
    chunkHashes: string[],
    success: boolean,
  ): Promise<boolean> {
    const file = await this.fileRepo.findById(fileId);
    if (!file) return false;

    if (success) {
      file.chunkHashes = chunkHashes;
      file.status = FileStatus.ACTIVE;
    } else {
      file.status = FileStatus.INACTIVE;
    }
    await this.fileRepo.save(file);
    return true;
  }
}
