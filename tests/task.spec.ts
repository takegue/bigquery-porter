import {
  afterAll,
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import path from 'node:path';
import fs from 'node:fs';
import { tmpdir } from 'node:os';
import { BigQuery } from '@google-cloud/bigquery';

import { Task } from '../src/tasks/base.js';
import {
  cleanupBigQueryDataset,
  createCleanupTasks,
} from '../src/tasks/cleanup.js';

describe('Reporter: Task Reporter', () => {
  it('succcess task', async () => {
    const task = new Task(
      'test1',
      () => new Promise((resolve) => setTimeout(() => resolve('hoge'), 50)),
    );

    expect(task.result().status).toBe('pending');

    await task.run();
    expect(task.result().status).toBe('success');
    const result = task.result();
    if (result.status === 'success') {
      expect(result.result).toBe('hoge');
    }
  });

  it('failed task', async () => {
    const task = new Task(
      'test1',
      () =>
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('error msg')), 50)
        ),
    );

    expect(task.result().status).toBe('pending');

    await task.run();
    expect(task.result().status).toBe('failed');
  });
});

describe('Task: Cleanup tasks', () => {
  const bqClient = new BigQuery();

  describe('cleanupBigQueryDataset', () => {
    const _root = fs.mkdtempSync(`${tmpdir()}${path.sep}`);
    const _project = 'bigquery-public-data';
    const dPath = path.join(_root, _project);

    afterAll(async () => {
      fs.rmSync(_root, { recursive: true });
    });

    it('No dataset files', async () => {
      const tasks = await cleanupBigQueryDataset(
        bqClient,
        _root,
        _project,
        'missing_dataset',
        {
          dryRun: true,
          withoutConrimation: true,
        },
      );

      expect(tasks.length).toBe(0);
    });

    it('Not found BigQuery', async () => {
      fs.mkdirSync(path.join(dPath, 'missing_dataset'), { recursive: true });
      const tasks = await cleanupBigQueryDataset(
        bqClient,
        _root,
        _project,
        'missing_dataset',
        {
          dryRun: true,
          withoutConrimation: true,
        },
      );

      expect(tasks.length).toBe(0);
    });
  });

  describe('cleanupBigQueryDataset', () => {
    beforeEach(async (ctx: any) => {
      ctx['settings'] = {
        _root: fs.mkdtempSync(`${tmpdir()}${path.sep}`),
        _project: 'bigquery-public-data',
        _dataset: 'austin_311',
      };
      const dPath = path.join(
        ctx.settings._root,
        ctx.settings._project,
        ctx.settings._dataset,
      );
      fs.mkdirSync(dPath, { recursive: true });

      // Mocking 'prompt'
      vi.mock('../src/prompt.js', async () => {
        const mod = await vi.importActual<typeof import('../src/prompt.js')>(
          '../src/prompt.js',
        );
        return {
          ...mod,
          prompt: async () => 'y',
        };
      });
    });

    afterEach(async (ctx) => {
      if ((ctx as any)['settings']) {
        fs.rmSync((ctx as any).settings._root, { recursive: true });
      }
    });

    it('Should remove table missing in local without prompt', async (ctx) => {
      const { settings: { _root, _project } } = ctx as any;
      const tasks = await createCleanupTasks(
        {
          BigQuery: {
            projectId: _project,
            client: bqClient,
          },
          dryRun: false,
          force: true,
          rootPath: _root,
        },
      );

      expect(tasks.length).toBe(1);
      expect(tasks[0]?.name).toBe(
        'bigquery-public-data/austin_311/TABLE/311_service_requests',
      );
    });

    it('Should remove table missing in local with prompt', async (ctx) => {
      const { settings: { _root, _project } } = ctx as any;

      const tasks = await createCleanupTasks(
        {
          BigQuery: {
            projectId: _project,
            client: bqClient,
          },
          dryRun: false,
          force: false,
          rootPath: _root,
        },
      );

      expect(tasks.length).toBe(1);
      expect(tasks[0]?.name).toBe(
        'bigquery-public-data/austin_311/TABLE/311_service_requests',
      );
    });
  });
});
