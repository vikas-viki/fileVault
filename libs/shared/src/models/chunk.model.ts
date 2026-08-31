import { AllowNull, BelongsTo, Column, DataType, Default, ForeignKey, Model, PrimaryKey, Table } from "sequelize-typescript";
import {v7 as uuidv7} from "uuid";
import { ObjectModel } from "./object.model";

@Table({modelName: 'chunks', underscored: true})
export class ChunkModel extends Model {

    @PrimaryKey
    @AllowNull(false)
    @Default(() => uuidv7())
    @Column(DataType.UUID)
    declare id: string;

    @ForeignKey(()=> ObjectModel)
    @AllowNull(false)
    @Column(DataType.UUID)
    declare objectId: string;

    @AllowNull(false)
    @Column(DataType.INTEGER)
    declare chunkIndex: number;

    @AllowNull(false)
    @Column(DataType.STRING)
    declare chunkHash: string;

    @AllowNull(false)
    @Column(DataType.INTEGER)
    declare size: number;

    @BelongsTo(() => ObjectModel, {onDelete: 'CASCADE'})
    declare object: ObjectModel
}