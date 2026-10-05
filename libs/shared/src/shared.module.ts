import { Global, Module } from '@nestjs/common';
import { SharedService } from './shared.service';
import { SequelizeModule } from '@nestjs/sequelize';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { UserModel } from './database/models/user.model';
import { NodeModel } from './database/models/node.model';
import { ObjectModel } from './database/models/object.model';
import { ChunkModel } from './database/models/chunk.model';
import { ChunkReplicaModel } from './database/models/chunk-replica.model';
import { BinFileModel } from './database/models/bin-file.model';
import { RedisService } from './redis.service';
import { BinFileRepository } from './database/repository/bin-file.repository';
import { UserRepository } from './database/repository/user.repository';
import { ChunkReplicaRepository } from './database/repository/chunk-replica.repository';
import { ChunkRepository } from './database/repository/chunk.repository';
import { ObjectRepository } from './database/repository/object.repository';
import { NodeRepository } from './database/repository/node.repository';

@Global()
@Module({
  providers: [
    SharedService,
    RedisService,
    BinFileRepository,
    UserRepository,
    ObjectRepository,
    ChunkRepository,
    ChunkReplicaRepository,
    NodeRepository
  ],
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    SequelizeModule.forFeature([
      UserModel,
      NodeModel,
      ObjectModel,
      ChunkModel,
      ChunkReplicaModel,
      BinFileModel,
    ]),
    SequelizeModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        dialect: 'postgres',
        host: config.get<string>('POSTGRES_HOST'),
        port: config.get<number>('POSTGRES_PORT', 5432),
        username: config.get<string>('POSTGRES_USER'),
        password: config.get<string>('POSTGRES_PASSWORD'),
        database: config.get<string>('POSTGRES_DB'),
        logging: false
      }),
    }),
  ],
  exports: [
    SharedService,
    SequelizeModule,
    RedisService,
    BinFileRepository,
    UserRepository,
    ChunkReplicaRepository,
    ChunkRepository,
    ObjectRepository,
    NodeRepository
  ],
})
export class SharedModule {}
