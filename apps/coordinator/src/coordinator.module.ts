import { Module } from '@nestjs/common';
import { CoordinatorController } from './coordinator.controller';
import { CoordinatorService } from './coordinator.service';
import { HeartbeatController } from './heartbeat/heartbeat.controller';
import { HeartbeatService } from './heartbeat/heartbeat.service';
import Redis from 'ioredis';
import { COORDINATOR, REDIS_CLIENT } from '@app/shared/helpers/constants';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { AuthModule } from './auth/auth.module';
import { SharedModule } from '@app/shared';
import { RedisService } from '@app/shared/redis.service';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: '.env',
    }),
    SharedModule,
    AuthModule,
  ],
  controllers: [CoordinatorController, HeartbeatController],
  providers: [
    CoordinatorService,
    HeartbeatService,
    {
      provide: REDIS_CLIENT,
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        return new Redis({
          host: configService.get<string>('REDIS_HOST'),
          port: configService.get('REDIS_PORT', 6379),
        });
      },
    },
  ],
})
export class CoordinatorModule {}
