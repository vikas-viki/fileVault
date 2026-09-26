import { Body, Controller, Post, Req, Res, UseGuards } from '@nestjs/common';
import { NodeService } from './node.service';
import { StreamRequest } from './node.dto';
import { JwtHttpGuard } from '@app/shared/auth';

@Controller('node')
export class NodeController {
  constructor(private readonly nodeService: NodeService) {}

  @UseGuards(JwtHttpGuard)
  @Post('download')
  async download(@Req() request, @Res() response) {
    const data = {
      byteOffset: request.header['x-byte-offset'],
      objectId: request.headers['x-object-id'],
      userId: request.user.userId
    };
    
    return this.nodeService.streamFileToClient(
      data,
      response
    );
  }

  @UseGuards(JwtHttpGuard)
  @Post('stream')
  async streamFile(@Req() request, @Res() response) {
    // TODO: proxy to pass the data parsed as headers
    const data: StreamRequest = {
      fileId: String(request.headers['x-file-id'] ?? ''),
      fileSize: request.headers['x-file-size'] ?? '',
      nodesToStream: String(request.headers['x-nodes-to-stream'] ?? '')
        .split(',')
        .filter(Boolean),
    };
    return this.nodeService.handleClientFileStream(request, response, data);
  }
}
