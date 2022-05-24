
import {expect, describe, it} from 'vitest';
import {
  topologicalSort,
  Relation,
  extractDestinations,
  extractRefenrences,
} from '../src/util.js';
import {pushBigQueryResources, pullBigQueryResources} from '../src/index.js';

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

