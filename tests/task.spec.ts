import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import path from 'node:path';
import fs from 'node:fs';
import { tmpdir } from 'node:os';

import { Task } from '../src/tasks/base.js';
import {
  cleanupBigQueryDataset,
  createCleanupTasks,
} from '../src/tasks/cleanup.js';
import { BigQuery } from '@google-cloud/bigquery';

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
      fs.mkdirSync(dPath, { recursive: true });
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
    const _root = fs.mkdtempSync(`${tmpdir()}${path.sep}`);
    const _project = 'bigquery-public-data';
    const _dataset = 'austin_311';
    const dPath = path.join(_root, _project, _dataset);

    afterAll(async () => {
      fs.rmSync(_root, { recursive: true });
    });

    beforeAll(async () => {
      fs.mkdirSync(dPath, { recursive: true });
    });

    it('clean up tables', async () => {
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
  });
});
