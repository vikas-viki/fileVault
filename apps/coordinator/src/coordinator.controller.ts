import {
  Body,
  Controller,
  Get,
  Post,
  Query,
  Req,
  UseGuards,
} from '@nestjs/common';
import { CoordinatorService } from './coordinator.service';
import { UploadRequestDTO } from './coordinator.dto';
import type { HealthCheckResponse } from './coordinator.dto';
import { JwtHttpGuard } from '@app/shared/auth';

@Controller()
export class CoordinatorController {
  constructor(private readonly coordinatorService: CoordinatorService) {}

  @Get('health')
  getHealth(): HealthCheckResponse {
    return this.coordinatorService.getHealth();
  }

  @UseGuards(JwtHttpGuard)
  @Post('upload-request')
  async uploadRequest(@Req() req, @Body() uploadRequest: UploadRequestDTO) {
    return await this.coordinatorService.uploadRequest(uploadRequest, req.user.sub);
  }

  @UseGuards(JwtHttpGuard)
  @Get('download-request')
  async downloadRequest(@Req() req, @Query('fileId') fileId: string) {
    return await this.coordinatorService.downloadRequest(fileId, req.user.sub);
  }
}
