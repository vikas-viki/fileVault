import { IsNotEmpty, IsNumber, isString, IsString, IsUUID } from "class-validator";

export type CurrentBinFileResponse = {
  location: string;
  startOffset: number;
} | null;


export class DownloadRquestDTO {
  @IsString()
  @IsNotEmpty()
  byteOffset: string;

  @IsString()
  @IsNotEmpty()
  objectId: string;

  @IsUUID()
  @IsNotEmpty()
  userId: string;
}