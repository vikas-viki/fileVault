import { Injectable } from '@nestjs/common';
import { HealthCheckResponse } from './coordinator.interface';

@Injectable()
export class CoordinatorService {
  getHealth(): HealthCheckResponse {
    return {
      status: 'active'
    };
  }
}
