import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { CoordinatorModule } from './coordinator.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import path from 'path';
import { COORDINATOR_PACKAGE_NAME } from '@app/shared/protos/interfaces/coordinator';
import { COORDINATOR } from '@app/shared/helpers/constants';

async function bootstrap() {
  const app = await NestFactory.create(CoordinatorModule);

  app.useGlobalPipes(
    new ValidationPipe({ whitelist: true, transform: true }),
  );

  app.enableCors({
    origin: 'http://localhost:3000',
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE',
    credentials: true,
    allowedHeaders: 'Content-Type, Accept, Authorization',
  });

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.GRPC,
    options: {
      package: COORDINATOR_PACKAGE_NAME,
      protoPath: path.join(
        __dirname,
        '../../libs/shared/protos/coordinator.proto',
      ),
      loader: {
        longs: String,
        keepCase: true,
      },
      url: '0.0.0.0:3001',
    },
  });

  const PORT = process.env.COORDINATOR_PORT ?? 3000;

  await app.startAllMicroservices();
  await app.listen(PORT, () =>{
    console.log(`${COORDINATOR} running on port ${PORT}`);
  });
}
bootstrap();
