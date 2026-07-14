import { Controller } from '@nestjs/common';
import { GrpcMethod } from '@nestjs/microservices';
import { HeartbeatService } from './heartbeat.service';
import type {
  HeartbeatRequest,
  HeartbeatResponse,
} from '@app/shared/protos/interfaces/coordinator';

@Controller()
export class HeartbeatController {
  constructor(private readonly heartbeatService: HeartbeatService) {}

  @GrpcMethod('HeartbeatService', 'Heartbeat')
  async heartbeat(data: HeartbeatRequest): Promise<HeartbeatResponse> {
    return await this.heartbeatService.logHeartbeat(data);
  }
}
