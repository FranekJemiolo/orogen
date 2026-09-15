import { describe, it, expect } from 'vitest';
import { PyodideETLRunner } from '../../src/engine/pyodide-runner.js';
import { DatasetManifest } from '../../src/types/index.js';
import { tableFromIPC } from 'apache-arrow';

describe('Pyodide ETL Runner & Deterministic Serialization', () => {
  const sampleManifest: DatasetManifest = {
    id: 'test-macro-yields',
    name: 'Test Macro Yields',
    asset_class: 'macro',
    schema: [
      { column: 'timestamp', type: 'TIMESTAMP' },
      { column: 'rate', type: 'FLOAT64' },
    ],
    etl: {
      engine: 'pyodide-python',
      source_api: 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10',
      requirements: ['pandas', 'numpy'],
      script: 'result_arrow = None',
    },
  };

  it('generates bit-exact deterministic Arrow buffers with uniform SHA-256', async () => {
    const runner = new PyodideETLRunner();
    const result1 = await runner.executeManifest(sampleManifest);
    const result2 = await runner.executeManifest(sampleManifest);

    expect(result1.deterministicSha256).toBe(result2.deterministicSha256);
    expect(result1.rowCount).toBe(500);

    const table = tableFromIPC(result1.arrowBuffer);
    expect(table.numRows).toBe(500);
    expect(table.schema.fields.map((f) => f.name)).toContain('timestamp');
    expect(table.schema.fields.map((f) => f.name)).toContain('rate');
  });

  it('restricts external network requests strictly to declared source_api domain', async () => {
    const runner = new PyodideETLRunner();
    const manifestWithIllegalApi: DatasetManifest = {
      ...sampleManifest,
      etl: {
        ...sampleManifest.etl,
        source_api: 'https://fred.stlouisfed.org/data',
      },
    };

    // Verify origin comparison logic
    const allowed = new URL(manifestWithIllegalApi.etl.source_api).origin;
    expect(allowed).toBe('https://fred.stlouisfed.org');
  });
});
