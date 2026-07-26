import {
  ConflictException,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import * as bcrypt from 'bcryptjs';
import { signToken } from '@app/shared/auth';
import { TokenScope } from '@app/shared/helpers/constants';
import { User } from '../models/user.model';
import { UserRepository } from './user.repository';
import { LoginDto, RegisterDto } from './auth.dto';

@Injectable()
export class AuthService {
  constructor(private readonly userRepo: UserRepository) {}

  async register(dto: RegisterDto) {
    const existing = await this.userRepo.findByEmail(dto.email);
    if (existing) {
      throw new ConflictException('Email already registered');
    }

    const passwordHash = await bcrypt.hash(dto.password, 10);
    const user = await this.userRepo.create({
      name: dto.name,
      email: dto.email,
      passwordHash,
    });

    return this.tokenResponse(user);
  }

  async login(dto: LoginDto) {
    const user = await this.userRepo.findByEmail(dto.email);
    if (!user || !(await bcrypt.compare(dto.password, user.passwordHash))) {
      throw new UnauthorizedException('Invalid credentials');
    }
    return this.tokenResponse(user);
  }

  private tokenResponse(user: User) {
    const token = signToken({ scope: TokenScope.CLIENT, sub: user.id }, '30d');
    return {
      token,
      user: { id: user.id, name: user.name, email: user.email },
    };
  }
}
