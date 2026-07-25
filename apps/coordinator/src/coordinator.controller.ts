import { Controller, Get, Param, UseGuards } from '@nestjs/common';
import { CoordinatorService } from './coordinator.service';
import type {
  HealthCheckResponse,
  UploadRequestDTO,
} from './coordinator.interface';
import { JwtHttpGuard } from '@app/shared/auth';

@Controller()
export class CoordinatorController {
  constructor(private readonly coordinatorService: CoordinatorService) {}

  @Get('health')
  getHealth(): HealthCheckResponse {
    return this.coordinatorService.getHealth();
  }

  @UseGuards(JwtHttpGuard)
  @Get('upload-request')
  async uploadRequest(@Param() uploadRequest: UploadRequestDTO) {
    return await this.coordinatorService.uploadRequest(uploadRequest);
  }
}
