# Orogen Release v1.0.0

## Release Notes: v1.0.0 — Initial Stable Release

Orogen is a decentralized, browser-based quant data lakehouse protocol engineered for browser environments supporting WebAssembly, Origin Private File System (OPFS), and WebRTC.

### Key Capabilities & Features

#### 1. In-Browser Vector OLAP Engine (`@duckdb/duckdb-wasm`)
- Instantiates DuckDB-Wasm inside a dedicated Web Worker.
- Executes vectorized analytical queries and temporal `ASOF JOIN`s across multiple Parquet files.
- Emits raw Apache Arrow IPC stream buffers for zero-copy memory transfers.
- Provides a synchronous mock engine abstraction for headless CI and unit testing.

#### 2. OPFS Concurrency & LRU Eviction Engine
- Employs non-exclusive asynchronous `getFile()` streams for all DuckDB queries and Service Worker catalog interceptions, eliminating file locking deadlocks.
- Restricts exclusive `createSyncAccessHandle()` strictly to atomic chunk write bursts.
- Proactively tracks disk usage via `navigator.storage.estimate()`; when usage reaches 85% quota, evicts oldest partitions according to LRU `lastAccessed` timestamps and signals `Data Offloaded` state.

#### 3. Sandboxed Pyodide ETL Execution
- Runs crowd-sourced Python data transformation pipelines inside WebAssembly.
- Pre-loads dependencies declared in dataset manifest `etl.requirements`.
- Hardened sandbox: completely purges the `js` module and browser scope from the Python environment post-initialization.
- Injects a verified `fetch_data(url)` callable that strictly enforces origin allowlists against the manifest's `source_api`.
- Generates bit-exact, deterministic Parquet/Arrow outputs with Unix epoch 0 timestamps and uniform Snappy compression.

#### 4. Nostr-Signaled WebRTC P2P Swarm & SCTP Flow Control
- Implements peer rendezvous via ephemeral Nostr events (Kind 29333) broadcast over public relays.
- Splits Parquet files into 64KB slices to adhere to browser SCTP message limits.
- Implements proactive backpressure: monitors `dataChannel.bufferedAmount`, pausing transfers if buffers exceed 1MB and resuming upon `bufferedamountlow` (65,536 bytes).

#### 5. Cryptographic Merkle Verification & Strike System
- Computes SHA-256 Merkle trees across all dataset chunk slices.
- Verifies every downloaded chunk against the signed root.
- Strike system immediately terminates peer connections for malicious or poisoned payloads, marks the offender's public key as `banned: true` in IndexedDB, and triggers automatic failover to the next available swarm peer.

#### 6. Virtual Apache Iceberg Service Worker Catalog
- Intercepts DuckDB REST queries to `https://orogen.local/api/v1/`.
- Dynamically responds with Iceberg table manifests and metadata.
- Streams Parquet byte chunks directly from OPFS to DuckDB-Wasm without external network hops.

### Installation

```bash
npm install @franekjemiolo/orogen
```

### Checksums & Artifacts
- Source code: [https://github.com/FranekJemiolo/orogen](https://github.com/FranekJemiolo/orogen)
- License: Apache-2.0
