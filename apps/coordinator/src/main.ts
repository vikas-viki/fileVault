import { NestFactory } from '@nestjs/core';
import { CoordinatorModule } from './coordinator.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import path from 'path';

async function bootstrap() {
  const app = await NestFactory.create(CoordinatorModule);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.GRPC,
    options: {
      package: 'coordinator',
      protoPath: path.join(__dirname, '../../libs/shared/coordinator.proto'),
      loader: {
        longs: String
      },
      url: '0.0.0.0:5001'
    }
  });
  await app.startAllMicroservices();
  await app.listen(process.env.COORDINATOR_PORT ?? 3000);
}
bootstrap();
