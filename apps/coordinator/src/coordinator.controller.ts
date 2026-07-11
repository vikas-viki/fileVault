import { Controller, Get, Param } from '@nestjs/common';
import { CoordinatorService } from './coordinator.service';
import type { HealthCheckResponse, UploadRequestDTO } from './coordinator.interface';

@Controller()
export class CoordinatorController {
  constructor(
    private readonly coordinatorService: CoordinatorService,
  ) {}

  @Get('health')
  getHealth(): HealthCheckResponse {
    return this.coordinatorService.getHealth();
  }

  // add authentication later
  @Get('upload')
  async upload(@Param() uploadRequest: UploadRequestDTO) {
    return await this.coordinatorService.upload(uploadRequest);
  }
}
