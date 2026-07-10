import { Module } from '@nestjs/common';
import { CoordinatorController } from './coordinator.controller';
import { CoordinatorService } from './coordinator.service';
import { HeartbeatController } from './heartbeat/heartbeat.controller';
import { HeartbeatService } from './heartbeat/heartbeat.service';
import Redis from 'ioredis';
import { REDIS_CLIENT } from '@app/shared/constants';
import { ConfigModule } from '@nestjs/config';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true, 
      envFilePath: '.env'
    })
  ],
  controllers: [CoordinatorController, HeartbeatController],
  providers: [
    CoordinatorService,
    HeartbeatService,
    {
      provide: REDIS_CLIENT,
      useFactory: ()=>{
        return new Redis({
          host: process.env.REDIS_HOST,
          port: Number(process.env.REDIS_PORT)
        });
      }
    }
  ],
})
export class CoordinatorModule {}
