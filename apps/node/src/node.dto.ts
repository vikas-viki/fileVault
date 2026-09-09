import { IsArray, IsString } from 'class-validator';

export class StreamRequest {
  @IsString()
  fileId!: string;

  @IsArray()
  nodesToStream!: string[];

  @IsString()
  fileSize!: number;
}

export class DownloadRequest {
  @IsArray()
  chunkHashes!: string[];
}
