import { Module } from '@nestjs/common';
import { SequelizeModule } from '@nestjs/sequelize';
import { UserModel } from '../../../../libs/shared/src/models/user.model';
import { AuthController } from './auth.controller';
import { AuthService } from './auth.service';
import { UserRepository } from '../../../../libs/shared/src/repository/user.repository';

@Module({
  imports: [SequelizeModule.forFeature([UserModel])],
  controllers: [AuthController],
  providers: [AuthService, UserRepository],
  exports: [AuthService]
})
export class AuthModule {}
