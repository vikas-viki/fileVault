import { Body, Controller, Post, Req, Res, UseGuards } from '@nestjs/common';
import { NodeService } from './node.service';
import { StreamRequest } from './node.type';
import { JwtHttpGuard } from '@app/shared/auth';

@Controller('node')
export class NodeController {
  constructor(private readonly nodeService: NodeService) {}

  @UseGuards(JwtHttpGuard)
  @Post('stream')
  async streamFile(
    @Req() request,
    @Res() response,
    @Body() body: StreamRequest,
  ) {
    return this.nodeService.clientStreamFile(request, response, body);
  }
}
