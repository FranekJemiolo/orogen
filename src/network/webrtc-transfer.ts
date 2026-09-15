export const SCTP_CHUNK_SIZE = 64 * 1024; // 64KB slice per SCTP packet
export const BUFFERED_AMOUNT_HIGH_WATER = 1024 * 1024; // 1MB threshold
export const BUFFERED_AMOUNT_LOW_WATER = 64 * 1024; // 65536 bytes low threshold

export interface ChunkTransferProgress {
  datasetId: string;
  chunkIndex: number;
  bytesSent: number;
  totalBytes: number;
  percent: number;
}

export interface ChunkPacketHeader {
  type: 'CHUNK_HEADER';
  datasetId: string;
  chunkIndex: number;
  totalChunks: number;
  totalBytes: number;
  sha256Hash: string;
}

export interface ChunkPacketData {
  type: 'CHUNK_DATA';
  datasetId: string;
  chunkIndex: number;
  offset: number;
  isLast: boolean;
}

export class WebRTCTransferManager {
  /**
   * Slices and transmits a Parquet chunk across RTCDataChannel with strict SCTP flow control
   */
  static async sendFileChunkWithBackpressure(
    dataChannel: RTCDataChannel,
    datasetId: string,
    chunkIndex: number,
    fileBuffer: ArrayBuffer,
    sha256Hash: string,
    onProgress?: (p: ChunkTransferProgress) => void
  ): Promise<void> {
    dataChannel.binaryType = 'arraybuffer';
    dataChannel.bufferedAmountLowThreshold = BUFFERED_AMOUNT_LOW_WATER;

    // Send chunk header first
    const header: ChunkPacketHeader = {
      type: 'CHUNK_HEADER',
      datasetId,
      chunkIndex,
      totalChunks: 1,
      totalBytes: fileBuffer.byteLength,
      sha256Hash,
    };
    dataChannel.send(JSON.stringify(header));

    const totalBytes = fileBuffer.byteLength;
    let offset = 0;

    while (offset < totalBytes) {
      // Check backpressure
      if (dataChannel.bufferedAmount > BUFFERED_AMOUNT_HIGH_WATER) {
        await new Promise<void>((resolve) => {
          const onLow = () => {
            dataChannel.removeEventListener('bufferedamountlow', onLow);
            resolve();
          };
          dataChannel.addEventListener('bufferedamountlow', onLow);
        });
      }

      const nextSliceEnd = Math.min(offset + SCTP_CHUNK_SIZE, totalBytes);
      const slice = fileBuffer.slice(offset, nextSliceEnd);
      const isLast = nextSliceEnd >= totalBytes;

      const metaHeader: ChunkPacketData = {
        type: 'CHUNK_DATA',
        datasetId,
        chunkIndex,
        offset,
        isLast,
      };

      // Send slice metadata then raw slice bytes
      dataChannel.send(JSON.stringify(metaHeader));
      dataChannel.send(slice);

      offset = nextSliceEnd;
      onProgress?.({
        datasetId,
        chunkIndex,
        bytesSent: offset,
        totalBytes,
        percent: Math.round((offset / totalBytes) * 100),
      });
    }
  }

  /**
   * Receiver state machine assembling incoming 64KB SCTP slices into a complete Parquet buffer
   */
  static createReceiver(onChunkReceived: (datasetId: string, chunkIndex: number, buffer: ArrayBuffer, expectedHash: string) => void) {
    let currentHeader: ChunkPacketHeader | null = null;
    let receivedBytes: Uint8Array | null = null;
    let currentDataMeta: ChunkPacketData | null = null;

    return (eventData: string | ArrayBuffer) => {
      if (typeof eventData === 'string') {
        try {
          const parsed = JSON.parse(eventData);
          if (parsed.type === 'CHUNK_HEADER') {
            currentHeader = parsed;
            receivedBytes = new Uint8Array(currentHeader!.totalBytes);
          } else if (parsed.type === 'CHUNK_DATA') {
            currentDataMeta = parsed;
          }
        } catch {
          // Ignored if non-JSON string
        }
      } else if (eventData instanceof ArrayBuffer && currentHeader && receivedBytes && currentDataMeta) {
        const sliceData = new Uint8Array(eventData);
        receivedBytes.set(sliceData, currentDataMeta.offset);

        if (currentDataMeta.isLast) {
          const assembledBuffer = receivedBytes.buffer;
          const header = currentHeader;
          currentHeader = null;
          receivedBytes = null;
          currentDataMeta = null;

          onChunkReceived(header.datasetId, header.chunkIndex, assembledBuffer as ArrayBuffer, header.sha256Hash);
        }
      }
    };
  }
}
