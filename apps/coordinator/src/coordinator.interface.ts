import { Expose } from 'class-transformer';
import { IsNotEmpty, IsString } from 'class-validator';
export interface HealthCheckResponse {
  status: string;
}

export class UploadRequestDTO {
  @IsString()
  @IsNotEmpty()
  fileName!: string;

  // fileSize in number of bytes
  @IsString()
  @IsNotEmpty()
  fileSize!: string;
}

export class UploadResponseDTO {
  @Expose()
  fileId!: string;

  @Expose()
  nodesToStream!: string[];
}
