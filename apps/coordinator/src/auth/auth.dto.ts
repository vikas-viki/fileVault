import { AuthType } from '@app/shared/helpers/constants';
import { IsEnum, IsNotEmpty, IsString } from 'class-validator';


export class GoogleAuthDto {
  @IsNotEmpty()
  @IsString()
  token: string;
}