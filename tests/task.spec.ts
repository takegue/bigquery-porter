import { describe, expect, it } from 'vitest';

import { BigQueryJobTask, Task } from '../src/tasks/base.js';
describe('task: simple task ', () => {
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

describe('task: bigquery task ', () => {
  const cases = [
    {
      result: {
        jobID: 'job-id',
        file: 'path/to/filename',
        totalBytesProcessed: 10000,
        totalSlotMs: 10000,
        elapsedTimeMs: 10000,
        isDryRun: false,
      },
    },
  ];

  it.each(cases)('bigquery job task (%#)', async ({ result }) => {
    const task = new BigQueryJobTask(
      'test1',
      () => new Promise((resolve) => resolve(result)),
    );
    await task.run();
    expect(task.toString()).toMatchInlineSnapshot('"processed: 9.8 KiB, slot: 10s, elapsed: 10s, ID: job-id"');
  });
});
