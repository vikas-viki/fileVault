import { Global, Module } from '@nestjs/common';
import { SharedService } from './shared.service';
import { SequelizeModule } from '@nestjs/sequelize';
import { ConfigService } from '@nestjs/config';
import { UserModel } from '@app/shared/models/user.model';
import { NodeModel } from './models/node.model';
import { ObjectModel } from './models/object.model';
import { FileModel } from './models/file.model';
import { ChunkModel } from './models/chunk.model';
import { ChunkReplicaModel } from './models/chunk_replica.model';
import { BinFileModel } from './models/bin_file.model';
import { RedisService } from './redis.service';

@Global()
@Module({
  providers: [
    SharedService,
    RedisService
  ],
  exports: [SharedService, SequelizeModule, RedisService],
  imports: [
    SequelizeModule.forFeature([UserModel, NodeModel, ObjectModel, FileModel, ChunkModel, ChunkReplicaModel, BinFileModel]),
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
})
export class SharedModule {}
