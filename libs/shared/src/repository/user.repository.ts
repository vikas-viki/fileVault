import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { UserModel } from '../models/user.model';

@Injectable()
export class UserRepository {
  constructor(@InjectModel(UserModel) private readonly model: typeof UserModel) { }

  findByEmail(email: string): Promise<UserModel | null> {
    return this.model.findOne({ where: { email } });
  }

  create(attrs: {
    name: string;
    email: string;
  }): Promise<UserModel> {
    return this.model.create(attrs);
  }

  async findOrCreate(email: string, name: string): Promise<UserModel> {
    const [user] = await this.model.findOrCreate({
      where: {
        email
      },
      defaults: {
        name, email
      }
    });
    return user;
  }
}
