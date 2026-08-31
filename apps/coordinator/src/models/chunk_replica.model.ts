import { AllowNull, BelongsTo, Column, DataType, Default, ForeignKey, Model, Table } from "sequelize-typescript";
import { v7 as uuidv7 } from "uuid";
import { ChunkModel } from "./chunk.model";
import { NodeModel } from "./node.model";
import { BinFileModel } from "./bin_file.model";

@Table({ tableName: 'chunk_replicas', underscored: true })
export class ChunkReplicaModel extends Model {

    @Column(DataType.UUID)
    @AllowNull(false)
    @Default(() => uuidv7())
    declare id: string;

    @Column(DataType.UUID)
    @AllowNull(false)
    @ForeignKey(() => ChunkModel)
    declare chunkId: string;

    @Column(DataType.UUID)
    @AllowNull(false)
    @ForeignKey(() => NodeModel)
    declare nodeId: string;

    @Column(DataType.UUID)
    @AllowNull(false)
    @ForeignKey(() => BinFileModel)
    declare binFileId: string;

    @Column(DataType.BIGINT)
    @AllowNull(false)
    declare byteOffset: number;

    @AllowNull(false)
    @Column(DataType.DATE)
    @Default(DataType.NOW)
    declare createdAt: Date;

    @AllowNull(false)
    @Column(DataType.DATE)
    declare deletedAt: Date;

    @BelongsTo(() => ChunkModel, { onDelete: 'CASCADE' })
    declare chunk: ChunkModel

    @BelongsTo(() => NodeModel, { onDelete: 'CASCADE' })
    declare node: NodeModel

    @BelongsTo(() => BinFileModel, { onDelete: 'CASCADE' })
    declare binFile: BinFileModel
}