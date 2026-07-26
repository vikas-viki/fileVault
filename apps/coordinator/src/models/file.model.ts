import { literal } from 'sequelize';
import {
  AllowNull,
  Column,
  DataType,
  Default,
  ForeignKey,
  Model,
  PrimaryKey,
  Table,
} from 'sequelize-typescript';
import { User } from './user.model';

export enum FileStatus {
  ACTIVE = 'active',
  INACTIVE = 'inactive',
  INPROGRESS = 'inprogress',
}

@Table({
  tableName: 'files',
  underscored: true,
  paranoid: true,
  indexes: [
    {
      name: 'files_user_filename_live_unique',
      unique: true,
      fields: ['user_id', 'file_name'],
      where: literal("deleted_at IS NULL AND status <> 'inactive'"),
    },
  ],
})
export class FileModel extends Model {
  @PrimaryKey
  @Default(DataType.UUIDV4)
  @Column(DataType.UUID)
  declare id: string;

  @ForeignKey(() => User)
  @AllowNull(false)
  @Column(DataType.UUID)
  declare userId: string;

  @AllowNull(false)
  @Column(DataType.STRING)
  declare fileName: string;

  @AllowNull(false)
  @Column(DataType.BIGINT)
  declare size: string;

  @Column(DataType.ARRAY(DataType.STRING))
  declare nodes: string[];

  @Column(DataType.JSONB)
  declare chunkHashes: string[];

  @Default(FileStatus.INPROGRESS)
  @Column(DataType.ENUM(...Object.values(FileStatus)))
  declare status: FileStatus;
}
