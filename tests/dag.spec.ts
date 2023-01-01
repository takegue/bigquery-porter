import { describe, expect, it } from 'vitest';
import { buildDAG, JobConfig } from '../src/commands/push.js';

describe('unit test: dag', () => {
  const cases: Array<{
    input: JobConfig[];
    expected: string[];
  }> = [
      {
        input: [
          {
            file: 'path/to/file_ddl_drop',
            namespace: 'project.dataset.table1',
            destinations: [
              ['project.dataset.table1', 'DDL_DROP'],
            ],
            dependencies: [
              'project.dataset.table2',
            ],
            shouldDeploy: true,
          },
          {
            file: 'path/to/file_ddl_create',
            namespace: 'project.dataset.table1',
            destinations: [
              ['project.dataset.table1', 'DDL_CREATE'],
            ],
            dependencies: [],
            shouldDeploy: true,
          },
          {
            file: 'path/to/file_dml',
            namespace: 'project.dataset.table1',
            destinations: [
              ['project.dataset.table1', 'DML'],
            ],
            dependencies: [],
            shouldDeploy: true,
          },
          {
            file: 'path/to/file_ref',
            namespace: 'project.dataset.table1',
            destinations: [],
            dependencies: [
              'project.dataset.table1',
            ],
            shouldDeploy: true,
          },
        ],
        expected: [
          'path/to/file_ddl_drop',
          'path/to/file_ddl_create',
          'path/to/file_dml',
          'path/to/file_ref',
        ],
      },
      {
        input: [
          {
            file: 'path/to/file_ddl_create_and_drop',
            namespace: 'project.dataset.table1',
            destinations: [
              ['project.dataset.table1', 'DDL_DROP'],
              ['project.dataset.table1', 'DDL_CREATE'],
            ],
            dependencies: ['project.dataset.table2'],
            shouldDeploy: true,
          },
          {
            file: 'path/to/file_dml',
            namespace: 'project.dataset.table1',
            destinations: [
              ['project.dataset.table1', 'DML'],
            ],
            // MERGE syntax references to self-table
            dependencies: [
              'project.dataset.table1',
            ],
            shouldDeploy: true,
          },
          {
            file: 'path/to/file_ref',
            namespace: 'project.dataset.table1',
            destinations: [],
            dependencies: ['project.dataset.table1'],
            shouldDeploy: true,
          },
        ],
        expected: [
          'path/to/file_ddl_create_and_drop',
          'path/to/file_dml',
          'path/to/file_ref',
        ],
      },
      {
        input: [
          {
            file: 'path/to/file_dml',
            namespace: 'project.dataset.table1',
            destinations: [
              ['project.dataset.table1', 'DML'],
            ],
            dependencies: [
              'project.dataset.table2',
            ],
            shouldDeploy: true,
          },
          {
            file: 'path/to/file_ref',
            namespace: 'project.dataset.table1',
            destinations: [],
            dependencies: [
              'project.dataset.table1',
            ],
            shouldDeploy: true,
          },
          {
            file: 'path/to/file_ddl_drop_and_create',
            namespace: 'project.dataset.table1',
            destinations: [
              ['project.dataset.table1', 'DDL_DROP'],
              ['project.dataset.table1', 'DDL_CREATE'],
            ],
            dependencies: [],
            shouldDeploy: true,
          },
        ],
        expected: [
          'path/to/file_ddl_drop_and_create',
          'path/to/file_dml',
          'path/to/file_ref',
        ],
      },
      ,
    ];
  it.each(cases)('table test for DDL/DML %#', async (args) => {
    const { input } = args;
    const [actual] = await buildDAG(input, { enableDataLineage: false });
    expect(actual.map((j) => j.file))
      .toMatchObject(args.expected);
  });

  it.fails('Cyclic pattern should throw error', async () => {
    const input: JobConfig[] = [
      {
        file: 'path/to/file_ref',
        namespace: 'project.dataset.table1',
        destinations: [
          ['project.dataset.tmp_table', 'DDL_CREATE'],
          ['project.dataset.tmp_table', 'DDL_DROP'],
        ],
        dependencies: [
          'project.dataset.table1',
          'project.dataset.tmp_table',
        ],
        shouldDeploy: true,
      },
      {
        file: 'path/to/file_ddl_create_and_drop',
        namespace: 'project.dataset.table1',
        destinations: [
          ['project.dataset.tmp_table', 'DDL_CREATE'],
          ['project.dataset.tmp_table', 'DDL_DROP'],
        ],
        dependencies: [
          'project.dataset.table1',
          'project.dataset.tmp_table',
        ],
        shouldDeploy: true,
      },
    ];
    const [actual] = await buildDAG(input, { enableDataLineage: false });
    expect(actual.map((j) => j.file))
      .toThrow();
  });
});
