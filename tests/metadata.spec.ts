import { describe, expect, it } from 'vitest';
import { tmpdir } from 'node:os';
import * as fs from 'node:fs';
import * as path from 'node:path';

import { BigQuery } from '@google-cloud/bigquery';
import { syncMetadata } from '../src/metadata.js';

describe('syncMetadata', () => {
  const bqClient = new BigQuery({ projectId: 'bigquery-public-data' });

  it('for Table', async () => {
    const table = bqClient.dataset('austin_bikeshare').table(
      'bikeshare_stations',
    );
    console.log(table);
    // expect(metadata).toMatchInlineSnapshot();

    // test
    const _obj = fs.mkdtempSync(`${tmpdir()}${path.sep}`);
    await syncMetadata(table, _obj);

    const _load = (f: string) => fs.readFileSync(path.join(_obj, f), 'utf-8');
    expect(fs.existsSync(path.join(_obj, 'metadata.json'))).toBe(true);
    expect(JSON.parse(_load('metadata.json')))
      .toMatchSnapshot();

    expect(JSON.parse(_load('schema.json')))
      .toMatchSnapshot();
  });
});
