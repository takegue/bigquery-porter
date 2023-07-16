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

import {
  cleanupBigQueryDataset,
  createCleanupTasks,
} from '../src/tasks/cleanup.js';

describe('Task: Cleanup tasks', () => {
  const bqClient = new BigQuery();

  describe('cleanupBigQueryDataset', () => {
    const _root = fs.mkdtempSync(`${tmpdir()}${path.sep}`);
    const _project = 'bigquery-public-data';
    const dPath = path.join(_root, _project);

    afterAll(async () => {
      fs.rmSync(_root, { recursive: true });
    });

    it('Not found local files in local', async () => {
      const tasks = await cleanupBigQueryDataset(
        bqClient,
        _root,
        _project,
        'missing_dataset',
        'confirm',
        {
          dryRun: true,
          withoutConrimation: true,
          ignorePrefix: '',
        },
      );

      expect(tasks.length).toBe(0);
    });

    it('Not found Dataset on BigQuery', async () => {
      fs.mkdirSync(path.join(dPath, 'missing_dataset'), { recursive: true });
      const tasks = await cleanupBigQueryDataset(
        bqClient,
        _root,
        _project,
        'missing_dataset',
        'confirm',
        {
          dryRun: true,
          withoutConrimation: true,
          ignorePrefix: '',
        },
      );

      expect(tasks.length).toBe(0);
    });
  });

  describe('bigquery-public-data.austin_311', () => {
    beforeEach(async (ctx: any) => {
      ctx['settings'] = {
        _root: fs.mkdtempSync(`${tmpdir()}${path.sep}`),
        _project: 'bigquery-public-data',
        _dataset: 'austin_bikeshare',
      };
      const dPath = path.join(
        ctx.settings._root,
        ctx.settings._project,
        ctx.settings._dataset,
      );
      fs.mkdirSync(dPath, { recursive: true });
      fs.mkdirSync(path.join(dPath, 'bikeshare_trips'));
      fs.closeSync(
        fs.openSync(path.join(dPath, 'bikeshare_trips', 'metadata.json'), 'w'),
      );

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

    it.concurrent("Shouldn't remove table missing in local without prompt with ignore mode", async (ctx) => {
      const { settings: { _root, _project } } = ctx as any;
      const tasks = await createCleanupTasks(
        {
          BigQuery: {
            projectId: _project,
            client: bqClient,
          },
          dryRun: false,
          rootPath: _root,
          SweepStrategy: {
            mode: 'ignore',
            ignorePrefix: '',
          }
        },
      );

      expect(tasks.length).toBe(0);
    });

    it.concurrent('Should remove table missing in local with prompt', async (ctx) => {
      const { settings: { _root, _project } } = ctx as any;

      const tasks = await createCleanupTasks(
        {
          BigQuery: {
            projectId: _project,
            client: bqClient,
          },
          dryRun: false,
          rootPath: _root,
          SweepStrategy: {
            mode: 'force',
            ignorePrefix: '',
          }
        },
      );

      expect(tasks.length).toBe(1);
      expect(tasks[0]?.name).toBe(
        'bigquery-public-data/austin_bikeshare/(DELETE)/TABLE/bikeshare_stations',
      );
    });

    it.concurrent('Dry run', async (ctx) => {
      const { settings: { _root, _project } } = ctx as any;

      const tasks = await createCleanupTasks(
        {
          BigQuery: {
            projectId: _project,
            client: bqClient,
          },
          dryRun: true,
          rootPath: _root,
          SweepStrategy: {
            mode: 'confirm',
            ignorePrefix: '',
          }
        },
      );

      expect(tasks.length).toBe(1);
      expect(tasks[0]?.name).toBe(
        'bigquery-public-data/austin_bikeshare/(DELETE)/TABLE/bikeshare_stations',
      );
    });

    it.concurrent('Should remove table missing in local with prompt', async (ctx) => {
      const { settings: { _root, _project } } = ctx as any;

      const tasks = await createCleanupTasks(
        {
          BigQuery: {
            projectId: _project,
            client: bqClient,
          },
          dryRun: false,
          rootPath: _root,
          SweepStrategy: {
            mode: 'rename_and_7d_expire',
            ignorePrefix: '',
          }
        },
      );

      expect(tasks.length).toBe(1);
      expect(tasks[0]?.name).toBe(
        'bigquery-public-data/austin_bikeshare/(RENAME)/TABLE/bikeshare_stations',
      );
    });
  });

  describe('Sharding tables', () => {
    beforeEach(async (ctx: any) => {
      ctx['settings'] = {
        _root: fs.mkdtempSync(`${tmpdir()}${path.sep}`),
        _project: 'bigquery-public-data',
        _dataset: 'google_analytics_sample',
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

    it.concurrent('Should remove tables', async (ctx) => {
      const { settings: { _root, _project } } = ctx as any;
      const tasks = await createCleanupTasks(
        {
          BigQuery: {
            projectId: _project,
            client: bqClient,
          },
          dryRun: false,
          rootPath: _root,
          SweepStrategy: {
            mode: 'confirm',
            ignorePrefix: '',
          }
        },
      );

      expect(tasks.length).toBe(366);
      expect(tasks[0]?.name).toBe(
        'bigquery-public-data/google_analytics_sample/(DELETE)/TABLE/ga_sessions_20160801',
      );
    });
  });
});
