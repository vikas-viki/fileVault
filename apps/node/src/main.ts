import { NestFactory } from '@nestjs/core';
import { NodeModule } from './node.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import path from 'path';
import { GRPC_PORT, STREAM_CHUNK_SIZE } from '@app/shared/helpers/constants';

async function bootstrap() {
  const app = await NestFactory.create(NodeModule);

  // Serves NodeService.StreamChunk so other nodes can relay file chunks to this one.
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.GRPC,
    options: {
      package: 'node',
      url: `0.0.0.0:${GRPC_PORT}`,
      protoPath: path.join(__dirname, '../../libs/shared/protos/node.proto'),
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
