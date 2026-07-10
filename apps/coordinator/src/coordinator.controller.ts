import { Controller, Get } from '@nestjs/common';
import { CoordinatorService } from './coordinator.service';
import type { HealthCheckResponse } from './coordinator.interface';

@Controller()
export class CoordinatorController {
  constructor(private readonly coordinatorService: CoordinatorService) {}

  @Get('health')
  getHealth(): HealthCheckResponse {
    return this.coordinatorService.getHealth();
  }
}
