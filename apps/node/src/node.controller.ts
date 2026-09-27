import { Body, Controller, Post, Req, Res, UseGuards, UseInterceptors } from '@nestjs/common';
import { NodeService } from './node.service';
import { StreamRequest } from './node.dto';
import { JwtHttpGuard } from '@app/shared/auth';
import { LifeCycleInterceptor } from '@app/shared/helpers/life-cycle-interceptor';
import { UserThrottlerGuard } from '@app/shared/helpers/throttle-guard';
import { Throttle } from '@nestjs/throttler';
import { RATE_LIMIT } from '@app/shared/helpers/constants';

@Controller('node')
export class NodeController {
  constructor(private readonly nodeService: NodeService) { }

  @UseGuards(JwtHttpGuard, UserThrottlerGuard)
  @UseInterceptors(LifeCycleInterceptor)
  @Throttle({ default: { limit: RATE_LIMIT.DOWNLOADS.MAX, ttl: RATE_LIMIT.DOWNLOADS.TTL } })
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

  @UseGuards(JwtHttpGuard, UserThrottlerGuard)
  @UseInterceptors(LifeCycleInterceptor)
  @Throttle({ default: { limit: RATE_LIMIT.UPLOADS.MAX, ttl: RATE_LIMIT.UPLOADS.TTL } })
  @Post('upload')
  async upload(@Req() request, @Res() response) {
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
