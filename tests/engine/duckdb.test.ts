import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { MockDuckDBEngine } from '../../src/engine/duckdb-engine.js';
import { tableFromIPC } from 'apache-arrow';

describe('DuckDB Engine Abstraction & Vectorized Queries', () => {
  let engine: MockDuckDBEngine;

  beforeEach(async () => {
    engine = new MockDuckDBEngine();
    await engine.init();
  });

  afterEach(async () => {
    await engine.close();
  });

  it('initializes and executes standard SQL query returning Arrow IPC buffer', async () => {
    const result = await engine.query('SELECT * FROM series');
    expect(result.arrowBuffer).toBeDefined();
    expect(result.rowCount).toBe(2);

    const table = tableFromIPC(result.arrowBuffer);
    expect(table.numRows).toBe(2);
    expect(table.schema.fields.map((f) => f.name)).toContain('timestamp');
  });

  it('handles multi-dataset temporal ASOF JOIN query and preserves columnar types', async () => {
    const sql = `
      SELECT t.timestamp, t.price, m.macro_rate
      FROM trades t
      ASOF JOIN macro m ON t.timestamp >= m.timestamp
    `;
    const result = await engine.query(sql);
    expect(result.arrowBuffer).toBeDefined();
    expect(result.rowCount).toBe(3);

    const table = tableFromIPC(result.arrowBuffer);
    expect(table.schema.fields.map((f) => f.name)).toEqual(['timestamp', 'price', 'macro_rate']);
  });

  it('registers Parquet buffer without throwing error', async () => {
    const dummyParquet = new Uint8Array([0x50, 0x41, 0x52, 0x31, 0x00]); // PAR1 magic
    await expect(engine.registerParquet('mock_table', dummyParquet)).resolves.toBeUndefined();
  });
});
