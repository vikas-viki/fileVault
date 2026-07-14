import { Global, Module } from '@nestjs/common';
import { SharedService } from './shared.service';
import { REDIS_CLIENT } from './helpers/constants';
import Redis from 'ioredis';

@Global()
@Module({
  providers: [
    SharedService,
    {
      provide: REDIS_CLIENT,
      useFactory: () => {
        return new Redis({
          host: process.env.REDIS_HOST,
          port: Number(process.env.REDIS_PORT),
        });
      },
    },
  ],
  exports: [SharedService],
  imports: [],
})
export class SharedModule {}
