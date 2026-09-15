import { describe, it, expect, vi } from 'vitest';
import { OPFSStorageEngine } from '../../src/storage/opfs.js';

describe('OPFS Storage & LRU Eviction Subsystem', () => {
  it('correctly detects quota threshold and flags warnings at 85%', async () => {
    let warned = false;
    const engine = new OPFSStorageEngine({
      onQuotaWarning: (status) => {
        if (status.percentUsed >= 85) warned = true;
      },
    });

    // Mock navigator.storage.estimate returning 86%
    const mockEstimate = vi.fn().mockResolvedValue({
      usage: 860 * 1024 * 1024,
      quota: 1000 * 1024 * 1024,
    });

    vi.stubGlobal('navigator', {
      storage: {
        estimate: mockEstimate,
      },
    });

    const status = await engine.checkAndEnforceLRUQuota();
    expect(status.percentUsed).toBe(86);
    expect(status.isNearQuota).toBe(true);
    expect(warned).toBe(true);

    vi.unstubAllGlobals();
  });

  it('marks offloaded chunks correctly to prevent reading dropped data', async () => {
    let offloadedChunkId = '';
    const engine = new OPFSStorageEngine({
      onChunkOffloaded: (chunk) => {
        offloadedChunkId = `${chunk.datasetId}_${chunk.chunkIndex}`;
      },
    });

    // Simulate registered chunk in metadata
    (engine as any).metadataMap.set('macro_0', {
      datasetId: 'macro',
      chunkIndex: 0,
      totalChunks: 1,
      byteLength: 1024,
      sha256Hash: 'abc',
      lastAccessed: Date.now() - 100000,
      offloaded: false,
    });

    await engine.offloadChunk('macro', 0);
    expect(offloadedChunkId).toBe('macro_0');

    const meta = engine.getChunkMetadata('macro', 0);
    expect(meta?.offloaded).toBe(true);
  });
});
