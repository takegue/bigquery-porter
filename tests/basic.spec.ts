import { describe, expect, it } from 'vitest';
import {
  extractDestinations,
  extractRefenrences,
  fixDestinationSQL,
  Relation,
  topologicalSort,
} from '../src/util.js';
// import {pushBigQueryResources, pullBigQueryResources} from '../src/index.js';
import {
  BigQueryResource,
  bq2path,
  path2bq,
  normalizedBQPath,
} from '../src/bigquery.js';

describe('util test: toposort', () => {
  const cases: Array<{
    input: Relation[];
    expected: string[];
  }> = [
      {
        input: [
          ['a', 'b'],
          ['b', 'c'],
        ],
        expected: ['c', 'b', 'a'],
      },
      {
        input: [
          ['a', 'b'],
          ['c', 'b'],
        ],
        expected: ['b', 'a', 'c'],
      },
      {
        input: [
          ['c', 'b'],
          ['b', 'a'],
        ],
        expected: ['a', 'b', 'c'],
      },
    ];
  it.each(cases)('topological sort', async (args) => {
    const { input, expected } = args;
    expect(topologicalSort(input))
      .toMatchObject(expected);
  });
});

describe('util test: sql extraction', () => {
  const cases: Array<{
    input: string;
    expectedDestinations: [string, string][];
    expectedReferences: string[];
  }> = [
      {
        input: `create table \`child_table\` as select *
          from \`dataset.parent1_table\`, \`dataset.parent2_table\``,
        expectedDestinations: [['`child_table`', 'TABLE']],
        expectedReferences: [
          '`dataset.parent1_table`',
          '`dataset.parent2_table`',
        ],
      },
      {
        input: `with cte as (select * from \`child_table\`) select * from cte`,
        expectedDestinations: [],
        expectedReferences: ['`child_table`'],
      },
      {
        input: `select 1`,
        expectedDestinations: [],
        expectedReferences: [],
      },
      {
        input:
          `create or replace procedure \`sandbox.sample_proc\`(in argument int64)
            options(description="test")
            begin select 1; end`,
        expectedDestinations: [['`sandbox.sample_proc`', 'ROUTINE']],
        expectedReferences: [],
      },
      {
        input:
          `create or replace procedure \`sandbox.sample_proc\`(in argument int64)
           begin
            call \`sandbox.reference_proc\`();
          end`,
        expectedDestinations: [['`sandbox.sample_proc`', 'ROUTINE']],
        expectedReferences: ['`sandbox.reference_proc`'],
      },
      {
        input:
          `create schema \`awesome_dataset\`;`,
        expectedDestinations: [['`awesome_dataset`', 'SCHEMA']],
        expectedReferences: [],
      },
    ];
  it.each(cases)('identifier extraction: destinations', async (args) => {
    const { input, expectedDestinations: expected } = args;
    expect(extractDestinations(input))
      .toMatchObject(expected);
  });

  it.each(cases)('identifier extraction: references', async (args) => {
    const { input, expectedReferences: expected } = args;
    expect(extractRefenrences(input))
      .toMatchObject(expected);
  });
});

describe('biquery: path2bq', () => {
  const cases: Array<{
    input: [string, string, string];
    expected: string;
  }> = [
      {
        input: ["bigquery-porter/bigquery/@default/v0/ddl.sql", "bigquery-porter/bigquery", "my-project"],
        expected: 'my-project.v0',
      },
      {
        input: ["bigquery-porter/bigquery/@default/v0/@routine/some_routine/ddl.sql", "bigquery-porter/bigquery", "my-project"],
        expected: 'my-project.v0.some_routine',
      },
      {
        input: ["bigquery-porter/bigquery/@default/v0/some_table/ddl.sql", "bigquery-porter/bigquery", "my-project"],
        expected: 'my-project.v0.some_table',
      },
    ];
  it.each(cases)('path2bq test', async (args) => {
    const { input, expected } = args;
    expect(path2bq(...input)).toMatchObject(expected);
  });
});

describe('biquery: bq2path', () => {
  const client: BigQueryResource = {
    baseUrl: 'https://bigquery.googleapis.com/bigquery/v2',
    projectId: 'awesome-project',
  };

  const dataset: BigQueryResource = {
    baseUrl: '/dataset',
    projectId: 'awesome-project',
    id: 'sandbox',
    parent: client,
  };

  const cases: Array<{
    input: [BigQueryResource, boolean];
    expected: string;
  }> = [
      {
        input: [client, false],
        expected: 'awesome-project',
      },
      {
        input: [client, true],
        expected: '@default',
      },
      {
        input: [dataset, false],
        expected: 'awesome-project/sandbox',
      },
      {
        input: [{
          baseUrl: '/tables',
          projectId: 'awesome-project',
          id: 'table_id',
          parent: dataset,
        }, false],
        expected: 'awesome-project/sandbox/table_id',
      },
      {
        input: [{
          baseUrl: '/routines',
          id: 'routine_id',
          parent: dataset,
        }, false],
        expected: 'awesome-project/sandbox/@routines/routine_id',
      },
      {
        input: [{
          baseUrl: '/models',
          projectId: 'awesome-project',
          id: 'model_id',
          parent: dataset,
        }, false],
        expected: 'awesome-project/sandbox/@models/model_id',
      },
      {
        input: [{
          baseUrl: '/unknown',
          projectId: 'awesome-project',
          id: 'unknown_id',
          parent: dataset,
        }, false],
        expected: 'awesome-project/sandbox/@unknown/unknown_id',
      },
      {
        input: [{
          baseUrl: '/tables',
          projectId: 'awesome-project',
          id: 'table_id',
          parent: dataset,
        }, true],
        expected: '@default/sandbox/table_id',
      },
      {
        input: [{
          baseUrl: '/routines',
          id: 'routine_id',
          parent: dataset,
        }, true],
        expected: '@default/sandbox/@routines/routine_id',
      },
    ];
  it.each(cases)('bq2path', async (args) => {
    const { input, expected } = args;
    expect(bq2path(...input)).toMatchObject(expected);
  });
});

describe('biquery: normalizedBQPath', () => {
  const cases: Array<{
    input: [string, string?, boolean?];
    expected: string;
  }> = [
      {
        input: ['`project_id.sbx.hoge`'],
        expected: 'project_id.sbx.hoge',
      },
      {
        input: ['project_id.sbx.hoge'],
        expected: 'project_id.sbx.hoge',
      },
      {
        input: ['project_id.sbx.hoge', '@default'],
        expected: 'project_id.sbx.hoge',
      },
      {
        input: ['sbx.hoge', '@default'],
        expected: '@default.sbx.hoge',
      },
      {
        input: ['sbx', '@default', true],
        expected: '@default.sbx',
      },
      {
        input: ['`sbx`', '@default', true],
        expected: '@default.sbx',
      },
    ];
  it.each(cases)('normalized bigquery path', async (args) => {
    const { input, expected } = args;
    expect(normalizedBQPath(...input)).toMatchObject(expected);
  });
});

describe('util test: fix SQL', () => {
  const cases: Array<{
    input: [string, string];
    expected: string;
  }> = [
      {
        input: [
          'awesome-project.sandbox.hoge',
          'create table `awesome-project.sandbox.wrong_name` as select 1',
        ],
        expected: 'create table `awesome-project.sandbox.hoge` as select 1',
      },
    ];
  it.each(cases)('topological sort', async (args) => {
    const { input, expected } = args;
    expect(fixDestinationSQL(...input))
      .toMatchObject(expected);
  });
});
