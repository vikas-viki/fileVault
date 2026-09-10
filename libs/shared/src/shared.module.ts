import { Global, Module } from '@nestjs/common';
import { SharedService } from './shared.service';
import { SequelizeModule } from '@nestjs/sequelize';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { UserModel } from '@app/shared/models/user.model';
import { NodeModel } from './models/node.model';
import { ObjectModel } from './models/object.model';
import { FileModel } from './models/file.model';
import { ChunkModel } from './models/chunk.model';
import { ChunkReplicaModel } from './models/chunk-replica.model';
import { BinFileModel } from './models/bin-file.model';
import { RedisService } from './redis.service';
import { BinFileRepository } from './repository/bin-file.repository';
import { UserRepository } from './repository/user.repository';
import { ChunkReplicaRepository } from './repository/chunk-replica.repository';
import { ChunkRepository } from './repository/chunk.repository';
import { ObjectRepository } from './repository/object.repository';

@Global()
@Module({
  providers: [
    SharedService,
    RedisService,
    BinFileRepository,
    UserRepository,
    ObjectRepository,
    ChunkRepository,
    ChunkReplicaRepository
  ],
  imports: [
    ConfigModule.forRoot({isGlobal: true}),
    SequelizeModule.forFeature([
      UserModel,
      NodeModel,
      ObjectModel,
      FileModel,
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
        autoLoadModels: true,
        synchronize: true,
        logging: false,
        sync: {
          alter: true
        }
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
    ObjectRepository
  ],
})
export class SharedModule {}
