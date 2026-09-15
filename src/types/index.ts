export type DataType = 'TIMESTAMP' | 'FLOAT32' | 'FLOAT64' | 'UTF8';

export interface ColumnDefinition {
  column: string;
  type: DataType;
}

export type AssetClass = 'macro' | 'equities' | 'fixed_income' | 'alternative';

export type ETLEngine = 'duckdb-sql' | 'pyodide-python';

export interface ETLConfig {
  engine: ETLEngine;
  source_api: string;
  script: string;
  requirements?: string[];
}

export interface DatasetManifest {
  $schema?: string;
  id: string;
  name: string;
  description?: string;
  asset_class: AssetClass;
  schema: ColumnDefinition[];
  etl: ETLConfig;
}

export interface ChunkMetadata {
  datasetId: string;
  chunkIndex: number;
  totalChunks: number;
  byteLength: number;
  sha256Hash: string;
  lastAccessed: number;
  offloaded?: boolean;
}

export interface MerkleNode {
  hash: string;
  left?: MerkleNode;
  right?: MerkleNode;
}

export interface StorageStatus {
  usage: number;
  quota: number;
  percentUsed: number;
  isNearQuota: boolean; // >85%
  evictedChunksCount: number;
}

export interface PeerSignalMessage {
  type: 'offer' | 'answer' | 'candidate';
  fromPubkey: string;
  toPubkey: string;
  datasetId: string;
  payload: any;
  timestamp: number;
}

export interface NostrEvent {
  id?: string;
  pubkey: string;
  created_at: number;
  kind: number; // 29333 for Orogen ephemeral signaling
  tags: string[][];
  content: string;
  sig?: string;
}

export interface BannedPeerRecord {
  pubkey: string;
  reason: string;
  bannedAt: number;
  invalidChunkIndex?: number;
  datasetId?: string;
}

export interface QueryRequest {
  id: string;
  sql: string;
  params?: any[];
  useSharedBuffer?: boolean;
}

export interface QueryResponse {
  id: string;
  success: boolean;
  arrowBuffer?: ArrayBuffer | SharedArrayBuffer;
  rowCount?: number;
  executionTimeMs?: number;
  error?: string;
}
