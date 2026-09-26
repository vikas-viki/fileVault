import { Controller } from '@nestjs/common';
import { GrpcMethod } from '@nestjs/microservices';
import { HeartbeatService } from './heartbeat.service';
import type {
  GetIpResponse,
  HeartbeatRequest,
  HeartbeatResponse,
} from '@app/shared/protos/interfaces/coordinator';
import type { Metadata, ServerUnaryCall } from '@grpc/grpc-js';

@Controller()
export class HeartbeatController {
  constructor(private readonly heartbeatService: HeartbeatService) { }

  @GrpcMethod('HeartbeatService', 'Heartbeat')
  async heartbeat(data: HeartbeatRequest): Promise<HeartbeatResponse> {
    return await this.heartbeatService.logHeartbeat(data);
  }

  @GrpcMethod('HeartbeatService', 'GetIp')
  async getIp(metadat: Metadata, call: ServerUnaryCall<any, any>): Promise<GetIpResponse> {
    const forwaredFor = metadat.get('x-forwarded-for')[0] as string;
    const ip = forwaredFor || call.getPeer();

    return { ip };
  }

}
