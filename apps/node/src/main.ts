import { NestFactory } from '@nestjs/core';
import { NodeModule } from './node.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import path from 'path';

async function bootstrap() {
  const app = await NestFactory.create(NodeModule);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.GRPC,
    options: {
      package: 'coordinator',
      protoPath: path.join(__dirname, '../../libs/shared/protos/coordinator.proto'),
      loader: {
        longs: String,
        keepCase: true
      },
      url: 'localhost:4001'
    }
  })

  await app.startAllMicroservices();

  await app.listen(process.env.port ?? 4000);
}
bootstrap();
