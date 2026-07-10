import { NestFactory } from '@nestjs/core';
import { CoordinatorModule } from './coordinator.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import path from 'path';
import { COORDINATOR_PACKAGE_NAME } from '@app/shared/protos/interfaces/coordinator';

async function bootstrap() {
  const app = await NestFactory.create(CoordinatorModule);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.GRPC,
    options: {
      package: COORDINATOR_PACKAGE_NAME,
      protoPath: path.join(__dirname, '../../libs/shared/protos/coordinator.proto'),
      loader: {
        longs: String,
        keepCase: true
      },
      url: '0.0.0.0:3001'
    }
  });
  await app.startAllMicroservices();
  await app.listen(process.env.COORDINATOR_PORT ?? 3000);
}
bootstrap();
