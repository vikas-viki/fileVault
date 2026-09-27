import {
  Body,
  Controller,
  Get,
  Ip,
  Post,
  Query,
  Req,
  Res,
  UseGuards,
} from '@nestjs/common';
import { CoordinatorService } from './coordinator.service';
import { UploadRequestDTO } from './coordinator.dto';
import type { HealthCheckResponse } from './coordinator.dto';
import { JwtHttpGuard } from '@app/shared/auth';
import { UserThrottlerGuard } from '@app/shared/helpers/throttle-guard';
import { RATE_LIMIT } from '@app/shared/helpers/constants';
import { Throttle } from '@nestjs/throttler';

@Controller()
export class CoordinatorController {
  constructor(private readonly coordinatorService: CoordinatorService) { }

  @Get('health')
  getHealth(): HealthCheckResponse {
    return this.coordinatorService.getHealth();
  }

  @UseGuards(JwtHttpGuard, UserThrottlerGuard)
  @Throttle({ default: { limit: RATE_LIMIT.UPLOADS.MAX, ttl: RATE_LIMIT.UPLOADS.TTL } })
  @Post('upload-request')
  async uploadRequest(@Req() req, @Body() uploadRequest: UploadRequestDTO) {
    return await this.coordinatorService.uploadRequest(
      uploadRequest,
      req.user.sub,
    );
  }

  @UseGuards(JwtHttpGuard, UserThrottlerGuard)
  @Throttle({ default: { limit: RATE_LIMIT.DOWNLOADS.MAX, ttl: RATE_LIMIT.DOWNLOADS.TTL } })
  @Get('download-request')
  async downloadRequest(@Req() req, @Query('objectId') fileId: string, @Query('byteOffset') byteOffset: string) {
    return await this.coordinatorService.downloadRequest(fileId, req.user.userId, byteOffset);
  }
}
