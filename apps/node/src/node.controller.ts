import { Body, Controller, Post, Req, Res, UseGuards } from '@nestjs/common';
import { NodeService } from './node.service';
import { DownloadRequest, StreamRequest } from './node.type';
import { JwtHttpGuard } from '@app/shared/auth';
import { STREAM_CHUNK_SIZE } from '@app/shared/helpers/constants';

@Controller('node')
export class NodeController {
  constructor(private readonly nodeService: NodeService) {}

  @UseGuards(JwtHttpGuard)
  @Post('download')
  async download(@Body() body: DownloadRequest, @Res() response) {
    return this.nodeService.streamFileToClient(response, body?.chunkHashes ?? []);
  }

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
