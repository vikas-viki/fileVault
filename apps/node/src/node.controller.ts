import { Body, Controller, Get, Post, Req, Res } from '@nestjs/common';
import { NodeService } from './node.service';
import { StreamRequest } from './node.type';

@Controller('node')
export class NodeController {
  constructor(private readonly nodeService: NodeService) {}

  @Post('stream')
  async streamFile(@Req() request, @Res() response, @Body() body: StreamRequest){
    return this.nodeService.streamFile(request, response, body);
  }

}
