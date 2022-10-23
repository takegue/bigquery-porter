import { describe, expect, it } from 'vitest';
import { Task } from '../src/tasks/base.js';

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
