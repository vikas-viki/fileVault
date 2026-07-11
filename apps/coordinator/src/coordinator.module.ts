import { Inject, Module } from '@nestjs/common';
import { CoordinatorController } from './coordinator.controller';
import { CoordinatorService } from './coordinator.service';
import { HeartbeatController } from './heartbeat/heartbeat.controller';
import { HeartbeatService } from './heartbeat/heartbeat.service';
import Redis from 'ioredis';
import { COORDINATOR, REDIS_CLIENT } from '@app/shared/constants';
import { ConfigModule, ConfigService } from '@nestjs/config';

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
      inject: [ConfigService],
      useFactory: (configService: ConfigService)=>{
        return new Redis({
          host: configService.get<string>('REDIS_HOST'),
          port: configService.get('REDIS_PORT', 6379)
        });
      }
    }
  ],
})
export class CoordinatorModule {

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  async onModuleInit(){
    try {
      await this.redis.ping();
      console.log(`${COORDINATOR} redis initialized successfully`);
    }catch(err) {
      console.error(`${COORDINATOR} error initilizing the module: `, err);
    }
  }
}
