export interface MerkleProofStep {
  position: 'left' | 'right';
  hash: string;
}

export class MerkleTree {
  private leaves: string[];
  private layers: string[][];

  constructor(leafHashes: string[]) {
    this.leaves = leafHashes;
    this.layers = [leafHashes];
  }

  static async hashBuffer(buffer: ArrayBuffer | Uint8Array): Promise<string> {
    const raw = buffer instanceof Uint8Array ? buffer.buffer : buffer;
    if (typeof crypto !== 'undefined' && crypto.subtle) {
      const hashBuf = await crypto.subtle.digest('SHA-256', raw as ArrayBuffer);
      return Array.from(new Uint8Array(hashBuf))
        .map((b) => b.toString(16).padStart(2, '0'))
        .join('');
    } else {
      const { createHash } = await import('crypto');
      return createHash('sha256').update(Buffer.from(raw)).digest('hex');
    }
  }

  static async hashPair(left: string, right: string): Promise<string> {
    const combined = new TextEncoder().encode(left + right);
    return await MerkleTree.hashBuffer(combined.buffer);
  }

  static async build(leafHashes: string[]): Promise<MerkleTree> {
    const tree = new MerkleTree(leafHashes);
    await tree.computeTree();
    return tree;
  }

  private async computeTree(): Promise<void> {
    if (this.leaves.length === 0) {
      this.layers = [['0'.repeat(64)]];
      return;
    }

    let currentLayer = [...this.leaves];
    while (currentLayer.length > 1) {
      const nextLayer: string[] = [];
      for (let i = 0; i < currentLayer.length; i += 2) {
        const left = currentLayer[i]!;
        const right = i + 1 < currentLayer.length ? currentLayer[i + 1]! : left;
        const parentHash = await MerkleTree.hashPair(left, right);
        nextLayer.push(parentHash);
      }
      this.layers.push(nextLayer);
      currentLayer = nextLayer;
    }
  }

  getRoot(): string {
    const top = this.layers[this.layers.length - 1];
    return top && top[0] ? top[0] : '0'.repeat(64);
  }

  async getProof(leafIndex: number): Promise<MerkleProofStep[]> {
    const proof: MerkleProofStep[] = [];
    let idx = leafIndex;

    for (let l = 0; l < this.layers.length - 1; l++) {
      const layer = this.layers[l]!;
      const isRight = idx % 2 === 1;
      const pairIdx = isRight ? idx - 1 : idx + 1;

      if (pairIdx < layer.length) {
        proof.push({
          position: isRight ? 'left' : 'right',
          hash: layer[pairIdx]!,
        });
      } else {
        // If odd element, paired with itself
        proof.push({
          position: 'right',
          hash: layer[idx]!,
        });
      }
      idx = Math.floor(idx / 2);
    }

    return proof;
  }

  static async verifyProof(leafHash: string, proof: MerkleProofStep[], rootHash: string): Promise<boolean> {
    let current = leafHash;
    for (const step of proof) {
      if (step.position === 'left') {
        current = await MerkleTree.hashPair(step.hash, current);
      } else {
        current = await MerkleTree.hashPair(current, step.hash);
      }
    }
    return current.toLowerCase() === rootHash.toLowerCase();
  }
}
