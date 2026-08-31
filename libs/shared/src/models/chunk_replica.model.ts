import { AllowNull, BelongsTo, Column, DataType, Default, ForeignKey, Model, PrimaryKey, Table } from "sequelize-typescript";
import { v7 as uuidv7 } from "uuid";
import { ChunkModel } from "./chunk.model";
import { NodeModel } from "./node.model";
import { BinFileModel } from "./bin_file.model";

@Table({ tableName: 'chunk_replicas', underscored: true })
export class ChunkReplicaModel extends Model {

    @PrimaryKey
    @AllowNull(false)
    @Default(() => uuidv7())
    @Column(DataType.UUID)
    declare id: string;

    @AllowNull(false)
    @ForeignKey(() => ChunkModel)
    @Column(DataType.UUID)
    declare chunkId: string;

    @AllowNull(false)
    @ForeignKey(() => NodeModel)
    @Column(DataType.UUID)
    declare nodeId: string;

    @AllowNull(false)
    @ForeignKey(() => BinFileModel)
    @Column(DataType.UUID)
    declare binFileId: string;

    @AllowNull(false)
    @Column(DataType.BIGINT)
    declare byteOffset: number;

    @AllowNull(false)
    @Default(DataType.NOW)
    @Column(DataType.DATE)
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