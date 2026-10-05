import { Injectable } from "@nestjs/common";
import { NodeModel } from '../models/node.model';
import { InjectModel } from "@nestjs/sequelize";

@Injectable()
export class NodeRepository {
    constructor(
        @InjectModel(NodeModel) private readonly model: typeof NodeModel
    ) { }

    public async upsert(attrs: {
        nodeId: string,
        ipAddress: string,
        name: string | null,
        port: number
    }): Promise<NodeModel> {
        const [data] = await this.model.upsert({
            id: attrs.nodeId,
            ipAddress: attrs.ipAddress,
            name: attrs?.name ?? "NODE",
            port: attrs.port
        });
        return data;
    }
}