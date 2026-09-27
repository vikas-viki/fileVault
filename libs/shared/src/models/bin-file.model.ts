import {
  AllowNull,
  BelongsTo,
  Column,
  DataType,
  Default,
  ForeignKey,
  Model,
  PrimaryKey,
  Table,
} from 'sequelize-typescript';
import { v7 as uuidv7 } from 'uuid';
import { NodeModel } from './node.model';

@Table({ tableName: 'bin_files', underscored: true, paranoid: true })
export class BinFileModel extends Model {
  @PrimaryKey
  @Default(() => uuidv7())
  @Column(DataType.UUID)
  declare id: string;

  @AllowNull(false)
  @ForeignKey(() => NodeModel)
  @Column(DataType.UUID)
  declare nodeId: string;

  @AllowNull(false)
  @Column(DataType.STRING)
  declare filepath: string;

  @AllowNull(false)
  @Default(DataType.NOW)
  @Column(DataType.DATE)
  declare created_at: Date;

  @AllowNull(false)
  @Default(DataType.NOW)
  @Column(DataType.DATE)
  declare updatedAt: Date;

  @BelongsTo(() => NodeModel, { onDelete: 'CASCADE' })
  declare node: NodeModel;
}


export const BinFileAttributes = new Proxy({} as Record<keyof BinFileModel, string>, {
  get: (_, prop: string) => prop
});