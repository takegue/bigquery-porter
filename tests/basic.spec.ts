import { describe, expect, it } from 'vitest';
import {
  extractDestinations,
  extractRefenrences,
  Relation,
  topologicalSort,
} from '../src/util.js';
// import {pushBigQueryResources, pullBigQueryResources} from '../src/index.js';
import {
  BigQueryResource,
  bq2path,
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
    expectedDestinations: string[];
    expectedReferences: string[];
  }> = [
      {
        input: `create table \`child_table\` as select *
          from \`dataset.parent1_table\`, \`dataset.parent2_table\``,
        expectedDestinations: ['`child_table`'],
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
        expectedDestinations: ['`sandbox.sample_proc`'],
        expectedReferences: [],
      },
      {
        input:
          `create or replace procedure \`sandbox.sample_proc\`(in argument int64)
           begin
            call \`sandbox.reference_proc\`();
          end`,
        expectedDestinations: ['`sandbox.sample_proc`'],
        expectedReferences: ['`sandbox.reference_proc`'],
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
  it.each(cases)('topological sort', async (args) => {
    const { input, expected } = args;
    expect(bq2path(...input)).toMatchObject(expected);
  });
});

describe('biquery: normalizedBQPath', () => {
  const cases: Array<{
    input: [string, string | undefined];
    expected: string;
  }> = [
      {
        input: ['project_id.sbx.hoge', undefined],
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
        input: ['sbx', '@default'],
        expected: '@default.sbx',
      },
    ];
  it.each(cases)('topological sort', async (args) => {
    const { input, expected } = args;
    expect(normalizedBQPath(...input)).toMatchObject(expected);
  });
});
