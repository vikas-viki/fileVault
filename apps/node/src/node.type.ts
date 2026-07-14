import { IsArray, IsString } from "class-validator";

export class StreamRequest {
    @IsArray()
    nodesToStream!: string[];

    @IsString()
    fileSize!: string;
}