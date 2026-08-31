import { AllowNull, BelongsTo, Column, DataType, Default, ForeignKey, Model, Table } from "sequelize-typescript";
import {v7 as uuidv7} from "uuid";
import { ObjectModel } from "./object.model";

@Table({modelName: 'chunks', underscored: true})
export class ChunkModel extends Model {

    @Column(DataType.UUID)
    @AllowNull(false)
    @Default(() => uuidv7())
    declare id: string;

    @ForeignKey(()=> ObjectModel)
    @AllowNull(false)
    @Column(DataType.UUID)
    declare objectId: string;

    @Column(DataType.INTEGER)
    @AllowNull(false)
    declare chunkIndex: number;

    @Column(DataType.STRING)
    @AllowNull(false)
    declare chunkHash: string;

    @Column(DataType.INTEGER)
    @AllowNull(false)
    declare size: number;

    @BelongsTo(() => ObjectModel, {onDelete: 'CASCADE'})
    declare object: ObjectModel
}