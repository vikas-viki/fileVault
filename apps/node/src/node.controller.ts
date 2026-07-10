import { Controller, Get } from '@nestjs/common';
import { NodeService } from './node.service';

@Controller()
export class NodeController {
  constructor(private readonly nodeService: NodeService) {}

  @Get()
  getHello(): string {
    return this.nodeService.getHello();
  }
}
