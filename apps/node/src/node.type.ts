import { IsArray, IsString } from 'class-validator';

export class StreamRequest {
  @IsString()
  fileId!: string;

  @IsArray()
  nodesToStream!: string[];

  @IsString()
  fileSize!: string;
}

export class DownloadRequest {
  @IsArray()
  chunkHashes!: string[];
}
