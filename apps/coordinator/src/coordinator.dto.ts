import { MAX_FILE_SIZE } from '@app/shared/helpers/constants';
import { Expose } from 'class-transformer';
import { IsNotEmpty, IsString, Max } from 'class-validator';
export interface HealthCheckResponse {
  status: string;
}

export class UploadRequestDTO {
  @IsString()
  @IsNotEmpty()
  fileName!: string;

  // fileSize in number of bytes
  @IsString()
  @Max(MAX_FILE_SIZE)
  @IsNotEmpty()
  fileSize!: string;
}

export class Response {
  token: string;
}
