import { PyodideETLRunner } from './pyodide-runner.js';
import { DatasetManifest } from '../types/index.js';

const runner = new PyodideETLRunner();

self.onmessage = async (event: MessageEvent<{ id: string; manifest: DatasetManifest }>) => {
  const { id, manifest } = event.data;

  try {
    const result = await runner.executeManifest(manifest);
    // Transfer the deterministic Arrow ArrayBuffer with zero-copy IPC
    (self as any).postMessage(
      {
        id,
        success: true,
        datasetId: result.datasetId,
        arrowBuffer: result.arrowBuffer,
        rowCount: result.rowCount,
        deterministicSha256: result.deterministicSha256,
        executionTimeMs: result.executionTimeMs,
      },
      [result.arrowBuffer]
    );
  } catch (err: any) {
    self.postMessage({
      id,
      success: false,
      error: err.message || String(err),
    });
  }
};
