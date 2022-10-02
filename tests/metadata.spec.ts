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
  const dPath = path.join(_dataset, _resource);

  afterAll(async () => {
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
    const modified = (await syncMetadata(table, dPath))
      .map((n) => path.basename(n));
    expect(modified.length).toBe(3);

    const _load = (f: string) => fs.readFileSync(path.join(dPath, f), 'utf-8');
    expect(modified).toContain('metadata.json');
    expect(fs.existsSync(path.join(dPath, 'metadata.json'))).toBe(true);
    expect(JSON.parse(_load('metadata.json')))
      .toMatchSnapshot();

    expect(modified).toContain('schema.json');
    expect(fs.existsSync(path.join(dPath, 'schema.json'))).toBe(true);
    expect(JSON.parse(_load('schema.json')))
      .toMatchSnapshot();

    expect(modified).toContain('README.md');
    expect(fs.existsSync(path.join(dPath, 'README.md'))).toBe(true);
    expect(_load('README.md'))
      .toMatchSnapshot();
  });

  it('for Routine: bqutil.fn.int', async () => {
    const table = bqClient.dataset('fn', { projectId: 'bqutil' }).routine(
      'int',
    );

    const modified = (await syncMetadata(table, dPath))
      .map((n) => path.basename(n));

    const _load = (f: string) => fs.readFileSync(path.join(dPath, f), 'utf-8');

    expect(modified.length).toBe(1);

    expect(modified).toContain('metadata.json');
    expect(fs.existsSync(path.join(dPath, 'metadata.json'))).toBe(true);
    expect(JSON.parse(_load('metadata.json')))
      .toMatchSnapshot();

    expect(fs.existsSync(path.join(dPath, 'schema.json'))).toBe(false);

    expect(fs.existsSync(path.join(dPath, 'README.md'))).toBe(false);
  });
});

describe('syncMetadata: Push', () => {
  const bqClient = new BigQuery();
  const _local = fs.mkdtempSync(`${tmpdir()}${path.sep}`);

  const _dataset = 'zz' + path.basename(_local);
  const _fsResource = _local;

  // Utils
  const toPath = (p: string) => path.join(_fsResource, p);
  const _load = (f: string) =>
    fs.readFileSync(path.join(_fsResource, f), 'utf-8');
  const _write = (p: string, content: string) =>
    fs.promises.writeFile(
      path.join(_fsResource, p),
      content,
      'utf-8',
    );

  beforeAll(async () => {
    const [dataset] = await bqClient.createDataset(_dataset, {});
    // https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
    await dataset.createTable('sample_table', {
      schema: 'id:integer, name:string',
      description: 'sample table',
    });
  });

  afterAll(async () => {
    await bqClient.dataset(_dataset).delete({ force: true });
    fs.rmdirSync(_local, { recursive: true });
  });

  it('for Table: sample_table', async () => {
    const table = bqClient.dataset(_dataset).table(
      'sample_table',
    );
    const afterDescription = 'Updated description';
    const afterSchema = [
      {
        name: 'id',
        type: 'INTEGER',
        description: 'Updated description for \'id\'',
      },
      {
        name: 'name',
        type: 'STRING',
      },
    ];

    // test
    await Promise.all([
      _write('README.md', afterDescription),
      _write('schema.json', JSON.stringify(afterSchema)),
    ]);

    const modified = (await syncMetadata(table, _fsResource, { push: true }))
      .map((n) => path.basename(n));

    expect(modified.length).toBe(3);
    const [metadata] = await table.getMetadata();

    expect(fs.existsSync(toPath('README.md'))).toBe(true);
    expect(_load('README.md'))
      .toBe(afterDescription);

    expect(fs.existsSync(toPath('schema.json'))).toBe(true);
    expect(JSON.parse(_load('schema.json')))
      .toMatchSnapshot();
    expect(metadata.schema.fields).toMatchObject(afterSchema);

    expect(fs.existsSync(path.join(toPath('metadata.json')))).toBe(true);
    expect(JSON.parse(_load('metadata.json')))
      .toMatchSnapshot();
  });
});
