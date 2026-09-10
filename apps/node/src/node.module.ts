import { Inject, Module } from '@nestjs/common';
import { NodeController } from './node.controller';
import { NodeStreamController } from './node-stream.controller';
import { NodeService } from './node.service';
import { ClientsModule, Transport } from '@nestjs/microservices';
import path from 'path';
import { COORDINATOR_GRPC_CLIENT } from '@app/shared/helpers/constants';
import { COORDINATOR_PACKAGE_NAME } from '@app/shared/protos/interfaces/coordinator';
import { GrpcClientsPoolService } from './grpc/grpc-clients-pool.service';
import Redis from 'ioredis';
import { ObjectRepository } from '@app/shared/repository/object.repository';
import { BinFileStorageService } from './bin-file-storage/bin-file-storage.service';
import { RedisService } from '@app/shared/redis.service';
import { BinFileRepository } from '@app/shared/repository/bin-file.repository';
import { ChunkReplicaRepository } from '@app/shared/repository/chunk-replica.repository';
import { ChunkRepository } from '@app/shared/repository/chunk.repository';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { ObjectModel } from '@app/shared/models/object.model';
import { BinFileModel } from '@app/shared/models/bin-file.model';
import { ChunkModel } from '@app/shared/models/chunk.model';
import { ChunkReplicaModel } from '@app/shared/models/chunk-replica.model';
import { SharedModule } from '@app/shared';
import { AuthService } from 'apps/coordinator/src/auth/auth.service';

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
    ])
  ],
  controllers: [NodeController, NodeStreamController],
  providers: [
    NodeService,
    GrpcClientsPoolService,
    BinFileStorageService,
    AuthService
  ],
})
export class NodeModule { }

