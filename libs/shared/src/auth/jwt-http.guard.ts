import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { TokenScope } from '../helpers/constants';
import { AuthService } from 'apps/coordinator/src/auth/auth.service';
import { TokenPayload } from 'apps/coordinator/src/auth/auth.types';

@Injectable()
export class JwtHttpGuard implements CanActivate {

  constructor(private readonly authService: AuthService){}

  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest();
    const header: string = request.headers?.authorization ?? '';
    const token = header.replace(/^Bearer\s+/i, '').trim();

    if (!token) {
      throw new UnauthorizedException('Missing bearer token');
    }

    let payload: TokenPayload;
    try {
      payload = this.authService.verifyToken(token, TokenScope.CLIENT);
    } catch {
      throw new UnauthorizedException('Invalid or expired token');
    }

    request.user = payload;
    return true;
  }
}
