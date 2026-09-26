import {
  AllowNull,
  BelongsTo,
  Column,
  DataType,
  Default,
  ForeignKey,
  PrimaryKey,
  Table,
  Model,
} from 'sequelize-typescript';
import { v7 as uuidv7 } from 'uuid';
import { UserModel } from './user.model';

export enum ObjectStatus {
  COMPLETED = 'COMPLETED',
  INPROGRESS = 'INPROGRESS',
  ABORTED = 'ABORTED',
}
@Table({ tableName: 'objects', underscored: true, paranoid: true })
export class ObjectModel extends Model {
  @PrimaryKey
  @Default(() => uuidv7())
  @Column(DataType.UUID)
  declare id: string;

  @AllowNull(false)
  @Column(DataType.UUID)
  @ForeignKey(() => UserModel)
  declare userId: string;

  @AllowNull(false)
  @Column(DataType.STRING)
  declare fileName: string;

  // file size in bytes
  @AllowNull(false)
  @Column(DataType.BIGINT)
  declare fileSize: number;

  @Default(ObjectStatus.INPROGRESS)
  @Column(DataType.ENUM(...Object.values(ObjectStatus)))
  declare status: ObjectStatus;

  @AllowNull(false)
  @Default(DataType.NOW)
  @Column(DataType.DATE)
  declare createdAt: Date;

  @BelongsTo(() => UserModel, { onDelete: 'CASCADE' })
  declare user: UserModel;
}
