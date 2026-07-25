import * as jwt from 'jsonwebtoken';
import { TokenScope } from '../helpers/constants';

export interface TokenPayload extends jwt.JwtPayload {
  scope: TokenScope;
  sub?: string;
}

function requireSecret(): string {
  const secret = process.env.JWT_SECRET;
  if (!secret) {
    throw new Error(
      'JWT_SECRET is not set. Set it in the environment before signing/verifying tokens.',
    );
  }
  return secret;
}

// Omit expiresIn for a non-expiring token (the internal service token).
export function signToken(
  payload: { scope: TokenScope; sub?: string },
  expiresIn?: string | number,
): string {
  const options: jwt.SignOptions = {};
  if (expiresIn !== undefined) {
    options.expiresIn = expiresIn as jwt.SignOptions['expiresIn'];
  }
  return jwt.sign(payload, requireSecret(), options);
}

export function verifyToken(token: string, scope?: TokenScope): TokenPayload {
  const decoded = jwt.verify(token, requireSecret()) as TokenPayload;
  if (scope && decoded.scope !== scope) {
    throw new Error(`token scope mismatch: expected ${scope}`);
  }
  return decoded;
}
