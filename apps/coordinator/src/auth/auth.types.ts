import { TokenScope } from '@app/shared/helpers/constants';
import * as jwt from 'jsonwebtoken';

export type AuthResponse = {
    name: string;
    email: string;
}

export interface TokenPayload extends jwt.JwtPayload {
    scope: TokenScope;
    sub?: string;
}