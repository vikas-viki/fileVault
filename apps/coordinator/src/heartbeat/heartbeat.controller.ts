import { Controller } from '@nestjs/common';
import { GrpcMethod } from '@nestjs/microservices';
import type { HeartbeatRequest, HeartbeatResponse } from './heartbeat.type';
import { HeartbeatService } from './heartbeat.service';

@Controller()
export class HeartbeatController {

    constructor(private readonly heartbeatService: HeartbeatService) {}

    @GrpcMethod('HeartbeatService', 'Heartbeat')
    async heartbeat(data: HeartbeatRequest): Promise<HeartbeatResponse> {
        return await this.heartbeatService.logHeartbeat(data);
    }
}
