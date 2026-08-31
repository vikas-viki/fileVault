import {
  Injectable,
  InternalServerErrorException,
  UnauthorizedException,
} from '@nestjs/common';
import { TokenScope } from '@app/shared/helpers/constants';
import { UserRepository } from '../../../../libs/shared/src/repository/user.repository';
import { ConfigService } from '@nestjs/config';
import { OAuth2Client } from 'google-auth-library';
import { UserModel } from '../../../../libs/shared/src/models/user.model';
import { GoogleAuthDto } from './auth.dto';
import * as jwt from 'jsonwebtoken';
import { AuthResponse, TokenPayload } from './auth.types';
import { UniqueConstraintError } from 'sequelize';
import { Response } from 'express';

@Injectable()
export class AuthService {
  private googleOauthClient: OAuth2Client;
  private jwtAuthSecret: string;

  constructor(
    private readonly userRepo: UserRepository,
    private readonly configService: ConfigService
  ) {
    const googleAuthClientId = this.configService.get('GOOGLE_AUTH_CLIENT_ID');
    const jwtSecret = this.configService.get('JWT_SECRET');

    if (!jwtSecret || !googleAuthClientId) {
      throw new InternalServerErrorException('Auth secrets not found');
    }

    this.googleOauthClient = new OAuth2Client(googleAuthClientId);
    this.jwtAuthSecret = jwtSecret;
  }

  async register(data: GoogleAuthDto) {
    try {
      const authResponse = await this.getGoogleAuthResponse(data);
      const user = await this.userRepo.create({ name: authResponse.name || "User", email: authResponse.email });
      return this.tokenResponse(user);
    } catch (error) {
      console.error(error);
      if (error instanceof UniqueConstraintError) {
        return this.login(data);
      }
      throw new InternalServerErrorException('Unable to process authentication request');
    }
  }

  async login(data: GoogleAuthDto) {
    try {
      const authResponse = await this.getGoogleAuthResponse(data);
      const user = await this.userRepo.findOrCreate(authResponse.email, authResponse.name);
      return this.tokenResponse(user);
    } catch (error) {
      console.error(error);
      throw new InternalServerErrorException('Unable to process authentication request');
    }
  }

  private async getGoogleAuthResponse(data: GoogleAuthDto): Promise<AuthResponse> {
    try {
      const ticket = await this.googleOauthClient.verifyIdToken({
        idToken: data.token,
        audience: this.configService.get('GOOGLE_AUTH_CLIENT_ID')
      });
      const payload = ticket.getPayload();

      if (!payload || !payload.email) {
        throw new UnauthorizedException('Ivalid authentication token');
      }

      return { email: payload.email, name: payload.name || "User" };
    } catch (error) {
      console.error(error);
      throw new InternalServerErrorException('Unable to process authentication request');
    }
  }

  private tokenResponse(user: UserModel) {
    const token = this.signToken({ scope: TokenScope.CLIENT, sub: user.id }, '30d');
    return {
      token,
      message: 'Authentication successful',
      user: { id: user.id, name: user.name, email: user.email },
    };
  }


  private signToken(
    payload: { scope: TokenScope; sub?: string },
    expiresIn?: string | number,
  ): string {
    const options: jwt.SignOptions = {
      expiresIn: expiresIn as jwt.SignOptions['expiresIn']
    };

    return jwt.sign(payload, this.jwtAuthSecret, options);
  }

  public verifyToken(token: string, scope?: TokenScope): TokenPayload {
    const decoded = jwt.verify(token, this.jwtAuthSecret) as TokenPayload;
    if (scope && decoded.scope !== scope) {
      throw new Error(`token scope mismatch: expected ${scope}`);
    }
    return decoded;
  }

  public setCookie(response: Response, token) {
    response.cookie('access_token', token, {
      httpOnly: true,
      secure: this.configService.get('SECURE_ACCESS_TOKEN') ?? true,
      sameSite: 'lax',
      maxAge: 30 * 24 * 60 * 60 * 1000,
      path: '/'
    });
  }
}
