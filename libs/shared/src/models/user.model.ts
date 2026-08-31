import {
  AllowNull,
  Column,
  DataType,
  Default,
  Model,
  PrimaryKey,
  Table,
  Unique,
} from 'sequelize-typescript';
import { v7 as uuidv7 } from 'uuid';

@Table({ tableName: 'users', underscored: true })
export class UserModel extends Model {
  @PrimaryKey
  @Default(() => uuidv7())
  @Column(DataType.UUID)
  declare id: string;

  @AllowNull(false)
  @Column(DataType.STRING)
  declare name: string;

  @Unique
  @AllowNull(false)
  @Column(DataType.STRING)
  declare email: string;

  @Default(DataType.NOW)
  @AllowNull(false)
  @Column(DataType.DATE)
  declare createdAt: Date;

  @Default(DataType.NOW)
  @AllowNull(false)
  @Column(DataType.DATE)
  declare updatedAt: Date;

}
