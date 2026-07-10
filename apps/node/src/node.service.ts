import { COORDINATOR_GRPC_CLIENT, NODE } from '@app/shared/constants';
import { HEARTBEAT_SERVICE_NAME } from '@app/shared/protos/interfaces/coordinator';
import type { HeartbeatResponse, HeartbeatServiceController } from '@app/shared/protos/interfaces/coordinator';
import { Inject, Injectable } from '@nestjs/common';
import type { ClientGrpc } from '@nestjs/microservices';
import { statfs } from 'fs/promises';
import { firstValueFrom, Observable } from 'rxjs';

@Injectable()
export class NodeService {

  constructor(@Inject(COORDINATOR_GRPC_CLIENT) private readonly client: ClientGrpc) { }
  private heartbeatService!: HeartbeatServiceController;

  onModuleInit() {
    this.heartbeatService = this.client.getService<HeartbeatServiceController>(HEARTBEAT_SERVICE_NAME);
  }

  async onApplicationBootstrap() {
    await this.heartbeat();
  }

  async heartbeat() {
    while (true) {
      try {
        const availableSpaceInBytes = await this.getAvailableSpaceInBytes();

        const response = await firstValueFrom(
          this.heartbeatService.heartbeat({
            spaceAvailableInBytes: Number(availableSpaceInBytes),
            ip: 'localhost',
            port: 4001
          }) as Observable<HeartbeatResponse>
        );

        console.log(`${NODE} got response from coordinator: `, response);
      } catch (err) {
        console.error(`${NODE} error communicating heartbeat: `, err);
      }

      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  async getAvailableSpaceInBytes(): Promise<Number> {
    try {
      const stats = await statfs('/');
      const availableSpace = stats.bavail * stats.bsize;

      return stats.ffree > 0 ? availableSpace : 0;
    } catch (err) {
      console.error(`${NODE} error getting available space: `, err);
      return 0;
    }
  }

}
