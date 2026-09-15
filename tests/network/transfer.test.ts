import { describe, it, expect } from 'vitest';
import { MerkleTree } from '../../src/security/merkle-verify.js';
import { WebRTCTransferManager } from '../../src/network/webrtc-transfer.js';

describe('WebRTC Transfer & Merkle Verification', () => {
  it('computes correct deterministic Merkle tree root and verifies valid leaf proofs', async () => {
    const leaf1 = 'a'.repeat(64);
    const leaf2 = 'b'.repeat(64);
    const leaf3 = 'c'.repeat(64);
    const leaf4 = 'd'.repeat(64);

    const tree = await MerkleTree.build([leaf1, leaf2, leaf3, leaf4]);
    const root = tree.getRoot();
    expect(root).toBeDefined();
    expect(root.length).toBe(64);

    const proof = await tree.getProof(1);
    const isValid = await MerkleTree.verifyProof(leaf2, proof, root);
    expect(isValid).toBe(true);

    const isInvalid = await MerkleTree.verifyProof('e'.repeat(64), proof, root);
    expect(isInvalid).toBe(false);
  });

  it('reassembles sliced chunks via WebRTC receiver state machine', async () => {
    let receivedDatasetId = '';
    let receivedIndex = -1;
    let receivedBufferLength = 0;

    const receiver = WebRTCTransferManager.createReceiver((dId, idx, buffer, _hash) => {
      receivedDatasetId = dId;
      receivedIndex = idx;
      receivedBufferLength = buffer.byteLength;
    });

    const header = JSON.stringify({
      type: 'CHUNK_HEADER',
      datasetId: 'test_series',
      chunkIndex: 0,
      totalChunks: 1,
      totalBytes: 10,
      sha256Hash: 'xyz',
    });

    receiver(header);

    // Send slice 1
    receiver(JSON.stringify({ type: 'CHUNK_DATA', datasetId: 'test_series', chunkIndex: 0, offset: 0, isLast: false }));
    receiver(new Uint8Array([1, 2, 3, 4, 5]).buffer);

    // Send slice 2
    receiver(JSON.stringify({ type: 'CHUNK_DATA', datasetId: 'test_series', chunkIndex: 0, offset: 5, isLast: true }));
    receiver(new Uint8Array([6, 7, 8, 9, 10]).buffer);

    expect(receivedDatasetId).toBe('test_series');
    expect(receivedIndex).toBe(0);
    expect(receivedBufferLength).toBe(10);
  });
});
