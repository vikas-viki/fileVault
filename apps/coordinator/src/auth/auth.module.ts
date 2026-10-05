import { Module } from '@nestjs/common';
import { SequelizeModule } from '@nestjs/sequelize';
import { UserModel } from '@app/shared/database/models/user.model';
import { AuthController } from './auth.controller';
import { AuthService } from './auth.service';
import { UserRepository } from '@app/shared/database/repository/user.repository';

@Module({
  imports: [SequelizeModule.forFeature([UserModel])],
  controllers: [AuthController],
  providers: [AuthService, UserRepository],
  exports: [AuthService],
})
export class AuthModule {}
