import { BannedPeerRecord } from '../types/index.js';
import { MerkleTree } from './merkle-verify.js';

export interface StrikeSystemCallbacks {
  onPeerBanned?: (record: BannedPeerRecord) => void;
  onRequestFailover?: (datasetId: string, chunkIndex: number) => void;
}

export class StrikeSystem {
  private bannedPeers: Map<string, BannedPeerRecord> = new Map();
  private callbacks: StrikeSystemCallbacks;
  private dbName = 'orogen_p2p_security';
  private storeName = 'banned_peers';

  constructor(callbacks: StrikeSystemCallbacks = {}) {
    this.callbacks = callbacks;
    this.initDatabase();
  }

  private async initDatabase(): Promise<void> {
    if (typeof indexedDB === 'undefined') return;

    try {
      const req = indexedDB.open(this.dbName, 1);
      req.onupgradeneeded = (e: any) => {
        const db = e.target.result;
        if (!db.objectStoreNames.contains(this.storeName)) {
          db.createObjectStore(this.storeName, { keyPath: 'pubkey' });
        }
      };
      req.onsuccess = (e: any) => {
        const db = e.target.result;
        const tx = db.transaction(this.storeName, 'readonly');
        const store = tx.objectStore(this.storeName);
        const getAll = store.getAll();
        getAll.onsuccess = () => {
          for (const item of getAll.result || []) {
            this.bannedPeers.set(item.pubkey, item);
          }
        };
      };
    } catch (err) {
      console.warn('Could not initialize strike system IndexedDB store:', err);
    }
  }

  isPeerBanned(pubkey: string): boolean {
    return this.bannedPeers.has(pubkey);
  }

  getBannedPeers(): BannedPeerRecord[] {
    return Array.from(this.bannedPeers.values());
  }

  /**
   * Addendum 1 Directive 4: Audits received chunk against expected SHA-256 hash.
   * If invalid, terminates peer connection, bans peer in IndexedDB, and requests failover.
   */
  async verifyChunkAndHandlePeer(
    peerPubkey: string,
    peerConnection: RTCPeerConnection | null,
    datasetId: string,
    chunkIndex: number,
    chunkBuffer: ArrayBuffer,
    expectedHash: string
  ): Promise<boolean> {
    if (this.isPeerBanned(peerPubkey)) {
      if (peerConnection) {
        try {
          peerConnection.close();
        } catch {}
      }
      return false;
    }

    const actualHash = await MerkleTree.hashBuffer(chunkBuffer);
    const isValid = actualHash.toLowerCase() === expectedHash.toLowerCase();

    if (!isValid) {
      console.error(
        `POISONED_CHUNK_DETECTED: Peer ${peerPubkey} sent corrupt chunk ${chunkIndex} for ${datasetId}. Expected: ${expectedHash}, Got: ${actualHash}`
      );

      // 1. Immediately close the RTCPeerConnection
      if (peerConnection) {
        try {
          peerConnection.close();
        } catch (e) {
          console.warn('Error terminating peer connection:', e);
        }
      }

      // 2. Flag their Nostr public key in IndexedDB as banned
      const record: BannedPeerRecord = {
        pubkey: peerPubkey,
        reason: `Mismatched SHA-256 hash on dataset ${datasetId} chunk ${chunkIndex}`,
        bannedAt: Date.now(),
        invalidChunkIndex: chunkIndex,
        datasetId,
      };
      this.bannedPeers.set(peerPubkey, record);
      await this.persistBan(record);

      if (this.callbacks.onPeerBanned) {
        this.callbacks.onPeerBanned(record);
      }

      // 3. Re-request the chunk from next available peer
      if (this.callbacks.onRequestFailover) {
        this.callbacks.onRequestFailover(datasetId, chunkIndex);
      }

      return false;
    }

    return true;
  }

  private async persistBan(record: BannedPeerRecord): Promise<void> {
    if (typeof indexedDB === 'undefined') return;

    try {
      const req = indexedDB.open(this.dbName, 1);
      req.onsuccess = (e: any) => {
        const db = e.target.result;
        const tx = db.transaction(this.storeName, 'readwrite');
        const store = tx.objectStore(this.storeName);
        store.put(record);
      };
    } catch (e) {
      console.warn('Failed to persist peer ban record to IndexedDB:', e);
    }
  }

  async unbanPeer(pubkey: string): Promise<void> {
    this.bannedPeers.delete(pubkey);
    if (typeof indexedDB === 'undefined') return;

    try {
      const req = indexedDB.open(this.dbName, 1);
      req.onsuccess = (e: any) => {
        const db = e.target.result;
        const tx = db.transaction(this.storeName, 'readwrite');
        const store = tx.objectStore(this.storeName);
        store.delete(pubkey);
      };
    } catch {}
  }
}
