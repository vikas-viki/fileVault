import { AllowNull, Column, DataType, Default, Model, PrimaryKey, Table } from "sequelize-typescript";
import { v7 as uuidv7 } from 'uuid';

@Table({ tableName: 'nodes', underscored: true })
export class NodeModel extends Model {
    @PrimaryKey
    @Default(() => uuidv7())
    @Column(DataType.UUID)
    declare id: string;

    @AllowNull(false)
    @Column(DataType.STRING)
    declare name: string;

    @AllowNull(false)
    @Column(DataType.STRING)
    declare ipAddress: string;

    @AllowNull(false)
    @Column(DataType.INTEGER)
    declare port: number;

    @AllowNull(false)
    @Default(DataType.NOW)
    @Column(DataType.DATE)
    declare createdAt: Date;

    @AllowNull(false)
    @Default(DataType.NOW)
    @Column(DataType.DATE)
    declare updatedAt: Date;
}