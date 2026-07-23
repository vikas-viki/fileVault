import * as os from "os";
import path from "path";

// to identify different node
export const NODE_IDENTIFIER = process.env.NODE_ID || 'CO_ORDINATOR';
export const GRPC_PORT = process.env.GRPC_PORT || '4001';

export const REDIS_CLIENT = 'REDIS_CLIENT';
export const AVAILABLE_NODES_KEY = 'AVAILABLE_NODES';
export const HEARTBEAT_TIMEOUT_SECONDS = 15;

// for logging
export const HEARTBEAT_SERVICE = '[HEARTBEAT_SERVICE]';
export const NODE = `[NODE ${NODE_IDENTIFIER}]`;
export const COORDINATOR = '[COORDINATOR]';

export const COORDINATOR_GRPC_CLIENT = 'COORDINATOR_GRPC_CLIENT';
export const CURRENT_NODE_INDEX = 'CURRENT_NODE_INDEX';
export const REPLICATION_COUNT = 3;

export const STREAM_CHUNK_SIZE = 5 * 1024 * 1024;
export const BUFFER_STREAM_SIZE = BigInt(1024 * 1024);
export const NODE_FILES_WRITE_PATH = path.join(os.homedir(), 'Documents', 'fileVault');
