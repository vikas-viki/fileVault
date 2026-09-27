import { BadRequestException, CallHandler, ExecutionContext, HttpException, HttpStatus, Inject, Injectable, NestInterceptor, NestMiddleware } from "@nestjs/common";
import { RedisService } from "../redis.service";
import { MAX_CONCURRENT_DOWNLOADS, MAX_CONCURRENT_DOWNLOADS_KEY, MAX_CONCURRENT_UPLOADS, MAX_CONCURRENT_UPLOADS_KEY, RequestType } from "./constants";
import { TokenPayload } from "apps/coordinator/src/auth/auth.types";
import { Observable } from "rxjs";

@Injectable()
export class LifeCycleInterceptor implements NestInterceptor {

    constructor(private readonly redisService: RedisService) { }

    async intercept(context: ExecutionContext, next: CallHandler): Promise<Observable<any>> {
        const httpContext = context.switchToHttp();
        const request = httpContext.getRequest();
        const response = httpContext.getResponse();
        let isCleanedup = false;
        const user = request.user as TokenPayload;
        const requestType = request.headers['x-request-type'] as RequestType;
        const isUploadOrDownload = [RequestType.DOWNLOAD, RequestType.UPLOAD].includes(requestType);

        if (!user?.userId || !isUploadOrDownload) {
            return next.handle();
        }

        const baseKey = requestType === RequestType.DOWNLOAD ? MAX_CONCURRENT_DOWNLOADS_KEY : MAX_CONCURRENT_UPLOADS_KEY;
        const maxCount = requestType == RequestType.DOWNLOAD ? MAX_CONCURRENT_DOWNLOADS : MAX_CONCURRENT_UPLOADS;
        const redisKey = `${baseKey}:${user.userId}`;
        const requestCount = await this.redisService.incr(redisKey);

        if (requestCount > maxCount) {
            await this.redisService.decr(redisKey);
            throw new HttpException(`Max concurrent ${requestType.toLocaleLowerCase()} limit exceeded`, HttpStatus.TOO_MANY_REQUESTS);
        }

        const closeHandle = async () => {
            try {
                if (isCleanedup) return;
                isCleanedup = true;
                await this.redisService.decr(redisKey);
            } catch (err) {
                console.error(`Error decrementing ${redisKey} at interceptor`, err);
            }
        }

        response.on('finish', closeHandle);
        response.on('close', closeHandle);
        await this.redisService.expire(redisKey, 7200);

        return next.handle();
    }

}