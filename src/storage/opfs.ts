import { ChunkMetadata, StorageStatus } from '../types/index.js';

export interface OPFSEventListeners {
  onChunkOffloaded?: (metadata: ChunkMetadata) => void;
  onQuotaWarning?: (status: StorageStatus) => void;
}

export class OPFSStorageEngine {
  private rootDir: FileSystemDirectoryHandle | null = null;
  private metadataMap: Map<string, ChunkMetadata> = new Map();
  private listeners: OPFSEventListeners = {};

  constructor(listeners: OPFSEventListeners = {}) {
    this.listeners = listeners;
  }

  /**
   * Initializes or gets the root directory in the Origin Private File System
   */
  async init(): Promise<FileSystemDirectoryHandle | null> {
    if (typeof navigator !== 'undefined' && navigator.storage && navigator.storage.getDirectory) {
      try {
        this.rootDir = await navigator.storage.getDirectory();
        await this.loadMetadata();
        return this.rootDir;
      } catch (err) {
        console.warn('OPFS initialization failed or not supported in this context:', err);
        return null;
      }
    }
    return null;
  }

  /**
   * Evaluates current browser storage quota and executes LRU eviction if above 85%
   */
  async checkAndEnforceLRUQuota(): Promise<StorageStatus> {
    let usage = 0;
    let quota = 1024 * 1024 * 1024; // Default fallback 1GB

    if (typeof navigator !== 'undefined' && navigator.storage && navigator.storage.estimate) {
      try {
        const estimate = await navigator.storage.estimate();
        usage = estimate.usage ?? 0;
        quota = estimate.quota ?? quota;
      } catch (e) {
        console.warn('Storage estimate failed:', e);
      }
    }

    const percentUsed = quota > 0 ? (usage / quota) * 100 : 0;
    const isNearQuota = percentUsed >= 85;
    let evictedChunksCount = 0;

    if (isNearQuota) {
      if (this.listeners.onQuotaWarning) {
        this.listeners.onQuotaWarning({
          usage,
          quota,
          percentUsed,
          isNearQuota,
          evictedChunksCount,
        });
      }

      // Evict oldest chunks by lastAccessed timestamp
      const sortedChunks = Array.from(this.metadataMap.values())
        .filter((c) => !c.offloaded)
        .sort((a, b) => a.lastAccessed - b.lastAccessed);

      for (const chunk of sortedChunks) {
        await this.offloadChunk(chunk.datasetId, chunk.chunkIndex);
        evictedChunksCount++;

        // Re-estimate if possible
        if (typeof navigator !== 'undefined' && navigator.storage && navigator.storage.estimate) {
          const currentEst = await navigator.storage.estimate();
          const curPercent = ((currentEst.usage ?? 0) / (currentEst.quota ?? quota)) * 100;
          if (curPercent < 75) {
            break; // Dropped safely below threshold
          }
        } else {
          break;
        }
      }
    }

    return {
      usage,
      quota,
      percentUsed,
      isNearQuota,
      evictedChunksCount,
    };
  }

  /**
   * Writes a Parquet file chunk to OPFS.
   * STRICT CONCURRENCY RULE: createSyncAccessHandle() is strictly utilized ONLY
   * during atomic write operations to prevent long-lived file locking.
   */
  async writeChunk(
    datasetId: string,
    chunkIndex: number,
    data: Uint8Array | ArrayBuffer,
    sha256Hash: string,
    totalChunks = 1
  ): Promise<ChunkMetadata> {
    const dir = await this.getOrCreateDatasetDir(datasetId);
    const fileName = `chunk_${chunkIndex}.parquet`;
    const buffer = data instanceof Uint8Array ? data.buffer : data;

    const fileHandle = await dir.getFileHandle(fileName, { create: true });

    // Prefer createSyncAccessHandle() if available (WebWorker exclusive write lock)
    if ('createSyncAccessHandle' in fileHandle && typeof (fileHandle as any).createSyncAccessHandle === 'function') {
      const accessHandle = await (fileHandle as any).createSyncAccessHandle();
      try {
        accessHandle.truncate(0);
        accessHandle.write(new Uint8Array(buffer), { at: 0 });
        accessHandle.flush();
      } finally {
        accessHandle.close(); // Crucial: close immediately to avoid lock starvation
      }
    } else {
      // Fallback to async writable stream on main thread / browsers lacking sync handle
      const writable = await (fileHandle as any).createWritable();
      await writable.write(buffer);
      await writable.close();
    }

    const metadataKey = `${datasetId}_${chunkIndex}`;
    const meta: ChunkMetadata = {
      datasetId,
      chunkIndex,
      totalChunks,
      byteLength: buffer.byteLength,
      sha256Hash,
      lastAccessed: Date.now(),
      offloaded: false,
    };

    this.metadataMap.set(metadataKey, meta);
    await this.saveMetadata();
    await this.checkAndEnforceLRUQuota();

    return meta;
  }

  /**
   * Reads a Parquet file chunk from OPFS.
   * STRICT CONCURRENCY RULE: Always uses asynchronous read-only getFile() stream
   * so multiple workers (DuckDB & Service Worker) can query concurrently without deadlocking.
   */
  async readChunk(datasetId: string, chunkIndex: number): Promise<Uint8Array> {
    const metadataKey = `${datasetId}_${chunkIndex}`;
    const meta = this.metadataMap.get(metadataKey);

    if (meta && meta.offloaded) {
      throw new Error(`CHUNK_OFFLOADED: Chunk ${chunkIndex} for dataset ${datasetId} was evicted by LRU engine.`);
    }

    const dir = await this.getOrCreateDatasetDir(datasetId);
    const fileName = `chunk_${chunkIndex}.parquet`;
    const fileHandle = await dir.getFileHandle(fileName, { create: false });

    // Non-exclusive getFile() read
    const file = await fileHandle.getFile();
    const arrayBuffer = await file.arrayBuffer();

    // Update lastAccessed for LRU
    if (meta) {
      meta.lastAccessed = Date.now();
      this.metadataMap.set(metadataKey, meta);
      await this.saveMetadata();
    }

    return new Uint8Array(arrayBuffer);
  }

  /**
   * Marks a chunk as offloaded and deletes the underlying Parquet file from OPFS to free quota
   */
  async offloadChunk(datasetId: string, chunkIndex: number): Promise<void> {
    const metadataKey = `${datasetId}_${chunkIndex}`;
    const meta = this.metadataMap.get(metadataKey);

    if (!meta) return;

    try {
      const dir = await this.getOrCreateDatasetDir(datasetId);
      const fileName = `chunk_${chunkIndex}.parquet`;
      await dir.removeEntry(fileName);
    } catch (e) {
      console.warn(`Could not remove chunk file ${datasetId}/${chunkIndex}:`, e);
    }

    meta.offloaded = true;
    this.metadataMap.set(metadataKey, meta);
    await this.saveMetadata();

    if (this.listeners.onChunkOffloaded) {
      this.listeners.onChunkOffloaded(meta);
    }
  }

  /**
   * Returns metadata for all known chunks
   */
  getChunkMetadata(datasetId: string, chunkIndex: number): ChunkMetadata | undefined {
    return this.metadataMap.get(`${datasetId}_${chunkIndex}`);
  }

  getAllMetadata(): ChunkMetadata[] {
    return Array.from(this.metadataMap.values());
  }

  private async getOrCreateDatasetDir(datasetId: string): Promise<FileSystemDirectoryHandle> {
    if (!this.rootDir) {
      await this.init();
    }
    if (!this.rootDir) {
      throw new Error('OPFS not initialized or not supported');
    }
    return await this.rootDir.getDirectoryHandle(datasetId, { create: true });
  }

  private async loadMetadata(): Promise<void> {
    if (!this.rootDir) return;
    try {
      const metaFileHandle = await this.rootDir.getFileHandle('_orogen_meta.json', { create: false });
      const file = await metaFileHandle.getFile();
      const text = await file.text();
      const records: ChunkMetadata[] = JSON.parse(text);
      this.metadataMap.clear();
      for (const r of records) {
        this.metadataMap.set(`${r.datasetId}_${r.chunkIndex}`, r);
      }
    } catch {
      // Metadata doesn't exist yet, start fresh
    }
  }

  private async saveMetadata(): Promise<void> {
    if (!this.rootDir) return;
    try {
      const metaFileHandle = await this.rootDir.getFileHandle('_orogen_meta.json', { create: true });
      const writable = await (metaFileHandle as any).createWritable();
      const data = JSON.stringify(Array.from(this.metadataMap.values()));
      await writable.write(data);
      await writable.close();
    } catch {
      // Ignore in mock/non-writable contexts
    }
  }
}
