import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from 'vitest';
import { tmpdir } from 'node:os';
import * as fs from 'node:fs';
import * as path from 'node:path';

import { BigQuery } from '@google-cloud/bigquery';
import { syncMetadata } from '../src/metadata.js';

describe('syncMetadata: Pull', () => {
  const bqClient = new BigQuery();
  const _dataset = fs.mkdtempSync(`${tmpdir()}${path.sep}`);
  const _resource = 'resource';

  beforeAll(async () => {
    const [dataset] = await bqClient.createDataset(path.basename(_dataset), {});
    // https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
    dataset.createTable('sample_table', {
      schema: 'id:integer, name:string',
      description: 'sample table',
    });
  });

  afterAll(async () => {
    bqClient.dataset(path.basename(_dataset)).delete({ force: true });
    fs.rmdirSync(_dataset, { recursive: true });
  });

  beforeEach(async () => {
    fs.mkdirSync(path.join(_dataset, _resource), { recursive: true });
  });

  afterEach(async () => {
    fs.rmdirSync(path.join(_dataset, _resource), { recursive: true });
  });

  it('for Table: bigquery-public-data:austin_bikeshare.bikeshare_stations', async () => {
    const table = bqClient.dataset('austin_bikeshare', {
      projectId: 'bigquery-public-data',
    }).table(
      'bikeshare_stations',
    );

    // test
    const dPath = path.join(_dataset, _resource);
    console.log(dPath);
    await syncMetadata(table, dPath);

    const _load = (f: string) => fs.readFileSync(path.join(dPath, f), 'utf-8');
    expect(fs.existsSync(path.join(dPath, 'metadata.json'))).toBe(true);
    expect(JSON.parse(_load('metadata.json')))
      .toMatchSnapshot();

    expect(fs.existsSync(path.join(dPath, 'schema.json'))).toBe(true);
    expect(JSON.parse(_load('schema.json')))
      .toMatchSnapshot();

    expect(fs.existsSync(path.join(dPath, 'README.md'))).toBe(true);
    expect(_load('README.md'))
      .toMatchSnapshot();
  });
});
