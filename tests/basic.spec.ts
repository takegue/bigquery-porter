
import {expect, describe, it} from 'vitest';
import {topologicalSort, Relation} from '../src/util.js';
import {pushBigQueryResources, pullBigQueryResources} from '../src/index.js';

describe('util test', () => {
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

describe('integration test', () => {
    it('Run push', async () => {
      await pushBigQueryResources();
    });

    it('Run pull', async () => {
      await pullBigQueryResources();
    });
})
 
