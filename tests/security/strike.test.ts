import { describe, it, expect, vi } from 'vitest';
import { StrikeSystem } from '../../src/security/strike-system.js';
import { MerkleTree } from '../../src/security/merkle-verify.js';

describe('Cryptographic Strike System & Peer Quarantine', () => {
  it('detects mismatched SHA-256 hash, terminates peer connection, and triggers failover', async () => {
    let bannedPubkey = '';
    let failoverDataset = '';

    const strikeSystem = new StrikeSystem({
      onPeerBanned: (record) => {
        bannedPubkey = record.pubkey;
      },
      onRequestFailover: (dId, _chunk) => {
        failoverDataset = dId;
      },
    });

    const mockPeerConnection = {
      close: vi.fn(),
    } as unknown as RTCPeerConnection;

    const data = new TextEncoder().encode('legitimate data');
    const validHash = await MerkleTree.hashBuffer(data.buffer);
    const corruptedHash = 'f'.repeat(64);

    // Test corrupted payload
    const isAccepted = await strikeSystem.verifyChunkAndHandlePeer(
      'peer_attacker_01',
      mockPeerConnection,
      'dataset_macro',
      0,
      data.buffer,
      corruptedHash
    );

    expect(isAccepted).toBe(false);
    expect(mockPeerConnection.close).toHaveBeenCalledTimes(1);
    expect(bannedPubkey).toBe('peer_attacker_01');
    expect(failoverDataset).toBe('dataset_macro');
    expect(strikeSystem.isPeerBanned('peer_attacker_01')).toBe(true);

    // Test legitimate payload
    const legitResult = await strikeSystem.verifyChunkAndHandlePeer(
      'peer_honest_02',
      mockPeerConnection,
      'dataset_macro',
      0,
      data.buffer,
      validHash
    );
    expect(legitResult).toBe(true);
  });
});
