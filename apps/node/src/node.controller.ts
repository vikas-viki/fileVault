import { Controller, Post, Req, Res, UseGuards } from '@nestjs/common';
import { NodeService } from './node.service';
import { StreamRequest } from './node.type';
import { JwtHttpGuard } from '@app/shared/auth';

@Controller('node')
export class NodeController {
  constructor(private readonly nodeService: NodeService) {}

  @UseGuards(JwtHttpGuard)
  @Post('stream')
  async streamFile(@Req() request, @Res() response) {
    // Upload metadata rides in headers; the body is the (multipart) file, so
    // it must be readable before busboy parses the body.
    const data: StreamRequest = {
      fileId: String(request.headers['x-file-id'] ?? ''),
      fileSize: String(request.headers['x-file-size'] ?? ''),
      nodesToStream: String(request.headers['x-nodes-to-stream'] ?? '')
        .split(',')
        .filter(Boolean),
    };
    return this.nodeService.clientStreamFile(request, response, data);
  }
}
