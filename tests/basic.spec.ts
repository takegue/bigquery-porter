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
  path2bq,
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
  it.concurrent.each(cases)('topological sort', async (args) => {
    const { input, expected } = args;
    expect(topologicalSort(input))
      .toMatchObject(expected);
  });
});

describe('util test: sql extraction ', () => {
  const cases: Array<{
    input: string;
    expectedDestinations: [
      string,
      | 'SCHEMA'
      | 'MODEL'
      | 'TABLE'
      | 'ROUTINE'
      | 'TEMPORARY_ROUTINE'
      | 'TEMPORARY_TABLE',
      'DML' | 'DDL_DROP' | 'DDL_CREATE',
    ][];
    expectedReferences: string[];
  }> = [
    {
      input: `create table \`child_table\` as select *
          from \`dataset.parent1_table\`, \`dataset.parent2_table\``,
      expectedDestinations: [['`child_table`', 'TABLE', 'DDL_CREATE']],
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
      input: `select * from \`dataset.table_*\``,
      expectedDestinations: [],
      expectedReferences: ['`dataset.table_*`'],
    },
    {
      input: `select * from \`dataset.table_20221210\``,
      expectedDestinations: [],
      expectedReferences: ['`dataset.table_*`'],
    },
    {
      input: `create or replace table \`sandbox.sample_20221210\`
          as
          select 1 as a
          `,
      expectedDestinations: [['`sandbox.sample_*`', 'TABLE', 'DDL_CREATE']],
      expectedReferences: [],
    },
    {
      input:
        `create or replace procedure \`sandbox.sample_proc\`(in argument int64)
            options(description="test")
            begin select 1; end`,
      expectedDestinations: [[
        '`sandbox.sample_proc`',
        'ROUTINE',
        'DDL_CREATE',
      ]],
      expectedReferences: [],
    },
    {
      input:
        `create or replace procedure \`sandbox.sample_proc\`(in argument int64)
           begin
            call \`sandbox.reference_proc\`();
          end`,
      expectedDestinations: [[
        '`sandbox.sample_proc`',
        'ROUTINE',
        'DDL_CREATE',
      ]],
      expectedReferences: ['`sandbox.reference_proc`'],
    },
    {
      input: 'create schema `awesome_dataset`;',
      expectedDestinations: [['`awesome_dataset`', 'SCHEMA', 'DDL_CREATE']],
      expectedReferences: [],
    },
    {
      input: 'CREATE MODEL `awesome_dataset.mymodel`',
      expectedDestinations: [[
        '`awesome_dataset.mymodel`',
        'MODEL',
        'DDL_CREATE',
      ]],
      expectedReferences: [],
    },
    {
      input: 'create temp table `tmp_table` as select 1;',
      expectedDestinations: [['`tmp_table`', 'TEMPORARY_TABLE', 'DDL_CREATE']],
      expectedReferences: [],
    },
    {
      input: `create temp function \`temp_function\`() as (1)`,
      expectedDestinations: [[
        '`temp_function`',
        'TEMPORARY_ROUTINE',
        'DDL_CREATE',
      ]],
      expectedReferences: [],
    },
  ];
  it.concurrent.each(cases)(
    'identifier extraction: destinations (%#)',
    async (args) => {
      const { input, expectedDestinations: expected } = args;
      expect(extractDestinations(input))
        .toMatchObject(expected);
    },
  );

  it.concurrent.each(cases)(
    'identifier extraction: references (%#)',
    async (args) => {
      const { input, expectedReferences: expected } = args;
      expect(extractRefenrences(input)).toMatchObject(expected);
    },
  );
});

describe('biquery: path2bq', () => {
  const cases: Array<{
    input: [string, string, string];
    expected: string;
  }> = [
    {
      input: [
        'bigquery-porter/bigquery/@default/v0/ddl.sql',
        'bigquery-porter/bigquery',
        'my-project',
      ],
      expected: 'my-project.v0',
    },
    {
      input: [
        'bigquery-porter/bigquery/@default/v0/@ignored/query.sql',
        'bigquery-porter/bigquery',
        'my-project',
      ],
      expected: 'my-project.v0',
    },
    {
      input: [
        'bigquery-porter/bigquery/hoge/v0/@ignored/query.sql',
        'bigquery-porter/bigquery',
        'my-project',
      ],
      expected: 'hoge.v0',
    },
    {
      input: [
        'bigquery-porter/bigquery/@default/v0/@routine/some_routine/ddl.sql',
        'bigquery-porter/bigquery',
        'my-project',
      ],
      expected: 'my-project.v0.some_routine',
    },
    {
      input: [
        'bigquery-porter/bigquery/@default/v0/some_table/ddl.sql',
        'bigquery-porter/bigquery',
        'my-project',
      ],
      expected: 'my-project.v0.some_table',
    },
    {
      input: [
        'bigquery-porter/bigquery/@default/@special/some.sql',
        'bigquery-porter/bigquery',
        'my-project',
      ],
      expected: 'my-project.v0.some_table',
    },
  ];
  it.concurrent.each(cases)('path2bq test', async (args) => {
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
        baseUrl: '/tables',
        projectId: 'awesome-project',
        id: 'table_200221210',
        parent: dataset,
      }, true],
      expected: '@default/sandbox/table_@',
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
  it.concurrent.each(cases)('bq2path', async (args) => {
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
  it.concurrent.each(cases)('normalized bigquery path', async (args) => {
    const { input, expected } = args;
    expect(normalizedBQPath(...input)).toMatchObject(expected);
  });
});
