import { Injectable } from '@nestjs/common';

@Injectable()
export class NodeService {
  getHello(): string {
    return 'Hello World!';
  }
}
