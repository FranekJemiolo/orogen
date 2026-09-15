import { describe, it, expect, vi } from 'vitest';
import { WebRTCTransferManager } from '../../src/network/webrtc-transfer.js';
import { MerkleTree } from '../../src/security/merkle-verify.js';

describe('Local P2P WebRTC Two-Peer Mesh Simulation', () => {
  it('establishes data channel, streams 150KB chunk across 64KB SCTP boundaries, and verifies hash on peer B', async () => {
    // Generate 150KB payload (spans 3 SCTP frames: 64KB + 64KB + 22KB)
    const payloadSize = 150 * 1024;
    const testData = new Uint8Array(payloadSize);
    for (let i = 0; i < payloadSize; i++) {
      testData[i] = i % 256;
    }

    const expectedHash = await MerkleTree.hashBuffer(testData.buffer);

    let receivedDataset = '';
    let receivedIndex = -1;
    let receivedBuffer: ArrayBuffer | null = null;
    let receivedHash = '';

    // Peer B receiver
    const peerBReceiver = WebRTCTransferManager.createReceiver((dId, idx, buffer, hash) => {
      receivedDataset = dId;
      receivedIndex = idx;
      receivedBuffer = buffer;
      receivedHash = hash;
    });

    // Mock RTCDataChannel connecting Peer A to Peer B
    const listeners: Record<string, ((e?: any) => void)[]> = {};
    const mockDataChannel = {
      binaryType: 'arraybuffer',
      bufferedAmount: 0,
      bufferedAmountLowThreshold: 65536,
      addEventListener: vi.fn((event: string, handler: any) => {
        if (!listeners[event]) listeners[event] = [];
        listeners[event]!.push(handler);
      }),
      removeEventListener: vi.fn((event: string, handler: any) => {
        if (listeners[event]) {
          listeners[event] = listeners[event]!.filter((h) => h !== handler);
        }
      }),
      send: vi.fn((data: string | ArrayBuffer) => {
        // Direct delivery to Peer B receiver
        peerBReceiver(data);
      }),
    } as unknown as RTCDataChannel;

    let progressCalls = 0;

    // Peer A sends file chunk with backpressure
    await WebRTCTransferManager.sendFileChunkWithBackpressure(
      mockDataChannel,
      'dataset_10y_yields',
      0,
      testData.buffer,
      expectedHash,
      (progress) => {
        progressCalls++;
        expect(progress.datasetId).toBe('dataset_10y_yields');
        expect(progress.totalBytes).toBe(payloadSize);
      }
    );

    // Verify Peer B received the complete assembled chunk
    expect(receivedDataset).toBe('dataset_10y_yields');
    expect(receivedIndex).toBe(0);
    expect(receivedBuffer).not.toBeNull();
    expect(receivedBuffer!.byteLength).toBe(payloadSize);
    expect(receivedHash).toBe(expectedHash);

    // Verify SHA-256 matches bit-for-bit
    const actualReceivedHash = await MerkleTree.hashBuffer(receivedBuffer!);
    expect(actualReceivedHash).toBe(expectedHash);
    expect(progressCalls).toBeGreaterThanOrEqual(3);
  });
});
