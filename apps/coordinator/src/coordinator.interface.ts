import { Expose } from 'class-transformer';
import { IsNotEmpty, IsString } from 'class-validator';
export interface HealthCheckResponse {
  status: string;
}

export class UploadRequestDTO {
  // fileSize in number of bytes
  @IsString()
  @IsNotEmpty()
  fileSize!: string;
}

export class UploadResponseDTO {
  @Expose()
  nodesToStream!: string[];
}
