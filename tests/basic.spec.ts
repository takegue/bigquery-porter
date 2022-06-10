
import {expect, describe, it} from 'vitest';
import {
  topologicalSort,
  Relation,
  extractDestinations,
  extractRefenrences,
} from '../src/util.js';
// import {pushBigQueryResources, pullBigQueryResources} from '../src/index.js';
import {BigQueryResource, bq2path} from '../src/bigquery.js';

describe('util test: toposort', () => {
    const cases: Array<{
        input: Relation[],
        expected: string[]
    }> = [
        {
            input: [
                ['a', 'b'],
                ['b', 'c'],
            ],
            expected: ['c', 'b', 'a']
        },
        {
            input: [
                ['a', 'b'],
                ['c', 'b'],
            ],
            expected: ['b', 'a', 'c']
        },
        {
            input: [
                ['c', 'b'],
                ['b', 'a'],
            ],
            expected: ['a', 'b', 'c']
        }
    ];
    it.each(cases)('topological sort', async (args) => {
        const {input, expected} = args;
        expect(topologicalSort(input))
            .toMatchObject(expected)
    });
})

describe('util test: sql extraction', () => {
    const cases: Array<{
        input: string,
        expectedDestinations: string[],
        expectedReferences: string[],
    }> = [
        {
            input: `create table \`child_table\` as select * 
          from \`dataset.parent1_table\`, \`dataset.parent2_table\``,
              expectedDestinations: ["`child_table`"],
              expectedReferences: ["`dataset.parent1_table`", "`dataset.parent2_table`"],
        },
        {
            input: `with cte as (select * from \`child_table\`) select * from cte`,
            expectedDestinations: [],
            expectedReferences: ["`child_table`"],

        },
        {
            input: `select 1`,
            expectedDestinations: [],
            expectedReferences: [],
        },
        {
            input: `create or replace function \`sandbox.sample_function\`(argument int64)
              options(description="test")
              as (1)`,
            expectedDestinations: ['`sandbox.sample_function`'],
            expectedReferences: [],
        }
    ];
    it.each(cases)('identifier extraction: destinations', async (args) => {
        const {input, expectedDestinations: expected} = args;
        expect(extractDestinations(input))
            .toMatchObject(expected)
    });

    it.each(cases)('identifier extraction: references', async (args) => {
        const {input, expectedReferences: expected} = args;
        expect(extractRefenrences(input))
            .toMatchObject(expected)
    });
})

describe('biquery: bq2path', () => {
  const client: BigQueryResource = {
    baseUrl: 'https://bigquery.googleapis.com/bigquery/v2',
    projectId: 'awesome-project',
  }

  const dataset: BigQueryResource = {
    baseUrl: '/dataset',
    projectId: 'awesome-project',
    id: 'sandbox',
    parent: client,
  }

  const cases: Array<{
    input: [BigQueryResource, boolean],
    expected: string
  }> = [
    {
      input: [dataset, false],
      expected: "awesome-project/sandbox"
    },
    { 
      input: [{
          baseUrl: '/tables',
          projectId: 'awesome-project',
          id: 'table_id',
          parent: dataset,
        },
        false
      ],
      expected: "awesome-project/sandbox/table_id"
    },
    { 
      input: [{
          baseUrl: '/routines',
          projectId: 'awesome-project',
          id: 'routine_id',
          parent: dataset,
        },
        false
      ],
      expected: "awesome-project/sandbox/@routines/routine_id"
    },
    { 
      input: [{
          baseUrl: '/models',
          projectId: 'awesome-project',
          id: 'model_id',
          parent: dataset,
        },
        false
      ],
      expected: "awesome-project/sandbox/@models/model_id"
    },
    {
      input: [{
          baseUrl: '/unknown',
          projectId: 'awesome-project',
          id: 'unknown_id',
          parent: dataset,
        },
        false
      ],
      expected: "awesome-project/sandbox/@unknown/unknown_id"
    },
    { 
      input: [{
          baseUrl: '/tables',
          projectId: 'awesome-project',
          id: 'table_id',
          parent: dataset,
        },
        true
      ],
      expected: "@default/sandbox/table_id"
    },
    { 
      input: [{
          baseUrl: '/routines',
          projectId: 'awesome-project',
          id: 'routine_id',
          parent: dataset,
        },
        true
      ],
      expected: "@default/sandbox/@routines/routine_id"
    }
  ];
  it.each(cases)('topological sort', async (args) => {
    const {input, expected} = args;
    expect(bq2path(...input)).toMatchObject(expected)
  });
})


// describe('integration test', () => {
//     // it('Run push', async () => {
//     //   await pushBigQueryResources();
//     // });

//     it('Run pull', async () => {
//       await new Promise(resolve => setTimeout(resolve, 3000))
//       await pullBigQueryResources();
//     });
// })
 
