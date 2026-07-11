export type AvailableNodesResponse = string[];

export type NodeData = {
    spaceAvailableInBytes: string;
    allocatedSpaceInBytes: string;
} | null;