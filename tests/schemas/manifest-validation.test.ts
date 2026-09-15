import { describe, it, expect } from 'vitest';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import schema from '../../schemas/dataset.schema.json';
import usTreasuryManifest from '../../schemas/examples/us-treasury-yields.json';
import btcManifest from '../../schemas/examples/btc-usdt-stream.json';
import sp500Manifest from '../../schemas/examples/sp500-intraday.json';

describe('Dataset Manifest Schema Validation (Ajv)', () => {
  const ajv = new Ajv({ allErrors: true });
  addFormats(ajv);
  const validate = ajv.compile(schema);

  it('validates us-treasury-yields.json successfully', () => {
    const valid = validate(usTreasuryManifest);
    expect(validate.errors).toBeNull();
    expect(valid).toBe(true);
  });

  it('validates btc-usdt-stream.json successfully', () => {
    const valid = validate(btcManifest);
    expect(validate.errors).toBeNull();
    expect(valid).toBe(true);
  });

  it('validates sp500-intraday.json successfully', () => {
    const valid = validate(sp500Manifest);
    expect(validate.errors).toBeNull();
    expect(valid).toBe(true);
  });

  it('rejects manifest with missing required fields (e.g. schema or etl)', () => {
    const invalidManifest = {
      id: 'bad-manifest',
      name: 'Missing schema and etl',
    };
    const valid = validate(invalidManifest);
    expect(valid).toBe(false);
    expect(validate.errors?.some((e) => e.params.missingProperty === 'schema')).toBe(true);
  });

  it('rejects manifest with invalid asset class', () => {
    const invalidManifest = {
      ...usTreasuryManifest,
      asset_class: 'invalid_class_type',
    };
    const valid = validate(invalidManifest);
    expect(valid).toBe(false);
    expect(validate.errors?.some((e) => e.instancePath === '/asset_class')).toBe(true);
  });

  it('rejects manifest with invalid etl engine', () => {
    const invalidManifest = {
      ...usTreasuryManifest,
      etl: {
        ...usTreasuryManifest.etl,
        engine: 'unsupported-engine-type',
      },
    };
    const valid = validate(invalidManifest);
    expect(valid).toBe(false);
    expect(validate.errors?.some((e) => e.instancePath === '/etl/engine')).toBe(true);
  });
});
