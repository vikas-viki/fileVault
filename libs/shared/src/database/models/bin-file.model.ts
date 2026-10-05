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
import { BIN_FILE_SIZE } from '../../helpers/constants';

export enum BinFileStatus {
  ACTIVE = 'active',
  SEALED = 'sealed',
  COMPACTING = 'compacting',
  COMPACTED = 'compacted',
  DELETED = 'deleted'
}


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

  // used only for garbage collection.
  // TODO: when user deletes a files subtract the same amount
  // of bytes from the binFiles that stored that file, so Garbagecollector
  // worker can later identify whenter to move this file into a new file
  // if the current actual size of data present is <= 30%
  @Default(BIN_FILE_SIZE)
  @AllowNull(false)
  @Column(DataType.NUMBER)
  declare allocatedSpace: number;

  @Default(BinFileStatus.ACTIVE)
  @AllowNull(false)
  @Column(DataType.ENUM(...Object.values(BinFileStatus)))
  declare status: BinFileStatus;

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