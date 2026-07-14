import { Module } from '@nestjs/common';
import { NodeController } from './node.controller';
import { NodeService } from './node.service';
import { ClientsModule, Transport } from '@nestjs/microservices';
import path from 'path';
import { COORDINATOR_GRPC_CLIENT } from '@app/shared/helpers/constants';
import { COORDINATOR_PACKAGE_NAME } from '@app/shared/protos/interfaces/coordinator';
import { GrpcClientsPoolService } from './grpc-clients-pool/grpc-clients-pool.service';

@Module({
  imports: [
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
  ],
  controllers: [NodeController],
  providers: [NodeService, GrpcClientsPoolService],
})
export class NodeModule {}
