import { NestFactory } from '@nestjs/core';
import { NodeModule } from './node.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import path from 'path';
import { STREAM_CHUNK_SIZE } from '@app/shared/helpers/constants';

async function bootstrap() {
  const app = await NestFactory.create(NodeModule);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.GRPC,
    options: {
      package: 'coordinator',
      url: 'localhost:4001',
      protoPath: path.join(
        __dirname,
        '../../libs/shared/protos/coordinator.proto',
      ),
      loader: {
        longs: String,
        keepCase: true,
      },
      channelOptions: {
        'grpc.max_send_message_length': STREAM_CHUNK_SIZE,
        'grpc.max_receive_message_length': STREAM_CHUNK_SIZE,
      },
    },
  });

  await app.startAllMicroservices();

  await app.listen(process.env.port ?? 4000);
}
bootstrap();
