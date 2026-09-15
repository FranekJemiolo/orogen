import { DuckDBWasmEngine, IDuckDBEngine } from './duckdb-engine.js';
import { QueryRequest, QueryResponse } from '../types/index.js';

let engine: IDuckDBEngine | null = null;

async function getEngine(): Promise<IDuckDBEngine> {
  if (!engine) {
    engine = new DuckDBWasmEngine();
    await engine.init();
  }
  return engine;
}

self.onmessage = async (event: MessageEvent<QueryRequest & { action?: string; parquetData?: Uint8Array; tableName?: string }>) => {
  const { data } = event;

  try {
    const ddb = await getEngine();

    if (data.action === 'registerParquet' && data.tableName && data.parquetData) {
      await ddb.registerParquet(data.tableName, data.parquetData);
      self.postMessage({ id: data.id, success: true });
      return;
    }

    if (data.sql) {
      const result = await ddb.query(data.sql, data.params);
      const response: QueryResponse = {
        id: data.id,
        success: true,
        arrowBuffer: result.arrowBuffer,
        rowCount: result.rowCount,
        executionTimeMs: result.executionTimeMs,
      };

      // Zero-copy transfer of the Arrow IPC ArrayBuffer
      (self as any).postMessage(response, [result.arrowBuffer]);
    }
  } catch (err: any) {
    const errorResponse: QueryResponse = {
      id: data.id,
      success: false,
      error: err.message || String(err),
    };
    self.postMessage(errorResponse);
  }
};
