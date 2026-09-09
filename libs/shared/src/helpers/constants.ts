import * as os from "os";
import path from "path";

export const NODE_IDENTIFIER = process.env.NODE_ID || 'CO_ORDINATOR';
export const GRPC_PORT = process.env.GRPC_PORT || '4001';
export const HTTP_PORT = process.env.port || '4000';

export const REDIS_CLIENT = 'REDIS_CLIENT';
export const AVAILABLE_NODES_KEY = 'AVAILABLE_NODES';
export const HEARTBEAT_TIMEOUT_SECONDS = 15;

// for logging
export const HEARTBEAT_SERVICE = '[HEARTBEAT_SERVICE]';
export const NODE = `[NODE ${NODE_IDENTIFIER}]`;
export const COORDINATOR = '[COORDINATOR]';

export const COORDINATOR_GRPC_CLIENT = 'COORDINATOR_GRPC_CLIENT';
export const CURRENT_NODE_INDEX = 'CURRENT_NODE_INDEX';
export const DOWNLOAD_NODE_INDEX = 'DOWNLOAD_NODE_INDEX';
export const REPLICATION_COUNT = 3;
export const NODE_INDEX_KEY = 'NODE_INDEX';

// file storage
export const CURRENT_BIN_FILE_KEY = 'CURRENT_BIN_FILE';
export const CURRENT_BIN_FILE_ID_KEY = 'CURRENT_BIN_FILE_ID';
export const CURRENT_BIN_FILE_OFFSET_KEY = 'CURRENT_BIN_FILE_OFFSET'
export const BIN_FILES_LOCATION = '~/bin-files/';
export const BIN_FILE_SIZE = 1073741824;
// size of chunks we store at files/db
export const STORAGE_CHUNK_SIZE = 5 * 1024 * 1024;
export const  MAX_OPEN_HANDLES = 5;
export const STREAM_CHUNK_SIZE = 64 * 1024;


// Must exceed a chunk plus its hash + protobuf framing, or the relay send fails.
export const GRPC_MAX_MESSAGE_SIZE = STREAM_CHUNK_SIZE + 1024 * 1024;
// Per-stream download cap; a client aggregates higher throughput across replicas.
export const DOWNLOAD_RATE_LIMIT_BYTES_PER_SEC = 5 * 1024 * 1024;
export const BUFFER_STREAM_SIZE = BigInt(1024 * 1024);
export const NODE_FILES_WRITE_PATH = path.join(os.homedir(), 'Documents', 'fileVault');

export enum TokenScope {
  CLIENT = 'client',
}


export enum AuthType {
  SINGIN = 'SIGNIN',
  SIGNUP = 'SIGNUP'
}

export const NODE_IDS = [
  'e682240b-17b6-4f5c-9a67-f9730362e9af',
  '05082ac7-77ec-4331-bcee-5aa7f27fc5ba',
  '1be4b21e-3fae-4120-b159-d01e96a0ed50',
  '0d0f0cd9-3dbe-479b-bbff-7e7b46205965'
]