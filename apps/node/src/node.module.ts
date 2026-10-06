import { Module } from '@nestjs/common';
import { NodeController } from './node.controller';
import { NodeStreamController } from './node-stream.controller';
import { NodeService } from './node.service';
import { ClientsModule, Transport } from '@nestjs/microservices';
import path from 'path';
import { COORDINATOR_GRPC_CLIENT } from '@app/shared/helpers/constants';
import { COORDINATOR_PACKAGE_NAME } from '@app/shared/protos/interfaces/coordinator';
import { GrpcClientsPoolService } from './grpc/grpc-clients-pool.service';
import { BinFileStorageService } from './bin-file-storage/bin-file-storage.service';
import { ConfigModule } from '@nestjs/config';
import { SharedModule } from '@app/shared';
import { AuthService } from 'apps/coordinator/src/auth/auth.service';
import { ThrottlerModule } from '@nestjs/throttler';
import { WorkerService } from './worker/worker.service';
import { WorkerController } from './worker/worker.controller';

@Module({
  imports: [
    ConfigModule,
    SharedModule,
    ClientsModule.register([
      {
        name: COORDINATOR_GRPC_CLIENT,
        transport: Transport.GRPC,
        options: {
          protoPath: path.join(
            __dirname,
            '../../libs/shared/protos/coordinator.proto',
          ),
          url: 'localhost:3001',
          package: COORDINATOR_PACKAGE_NAME,
          loader: {
            longs: String,
            keepCase: true,
          },
        },
      },
    ]),
    ThrottlerModule.forRoot([
      {
        ttl: 60000,
        limit: 10,
      },
    ]),
  ],
  controllers: [NodeController, NodeStreamController, WorkerController],
  providers: [
    NodeService,
    GrpcClientsPoolService,
    BinFileStorageService,
    AuthService,
    WorkerService
  ],
})
export class NodeModule {}
