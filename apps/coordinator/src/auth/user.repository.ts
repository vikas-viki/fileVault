import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { User } from '../models/user.model';

@Injectable()
export class UserRepository {
  constructor(@InjectModel(User) private readonly model: typeof User) {}

  findByEmail(email: string): Promise<User | null> {
    return this.model.findOne({ where: { email } });
  }

  create(attrs: {
    name: string;
    email: string;
    passwordHash: string;
  }): Promise<User> {
    return this.model.create(attrs);
  }
}
