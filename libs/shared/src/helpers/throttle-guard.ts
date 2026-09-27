import { ExecutionContext, Injectable } from '@nestjs/common';
import { ThrottlerGuard } from '@nestjs/throttler';
import { TokenPayload } from 'apps/coordinator/src/auth/auth.types';
import { SYSTEM_USER_ID } from './constants';

@Injectable()
export class UserThrottlerGuard extends ThrottlerGuard {
    protected async getTracker(req: Record<string, any>): Promise<string> {
        return req.user?.userId || req.ip;
    }

    protected async shouldSkip(_context: ExecutionContext): Promise<boolean> {
        const req = _context.switchToHttp().getRequest();
        const user = req.user as TokenPayload;

        if (user.userId === SYSTEM_USER_ID) {
            return true;
        }

        return super.shouldSkip(_context);
    }
}