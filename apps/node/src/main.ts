import { NestFactory } from '@nestjs/core';
import { NodeModule } from './node.module';

async function bootstrap() {
  const app = await NestFactory.create(NodeModule);
  await app.listen(process.env.port ?? 3000);
}
bootstrap();
