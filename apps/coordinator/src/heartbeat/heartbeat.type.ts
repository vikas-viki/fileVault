export type HeartbeatRequest = {
    ip: string;
    port: number; 
    spaceAvailableInBytes: string;
}

export type HeartbeatResponse = {
    status: boolean;
}

export type AvailableNodesResponse = string[];