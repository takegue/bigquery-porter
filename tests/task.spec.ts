import { describe, expect, it } from 'vitest';
import { Task } from '../src/task.js';

describe('Reporter: Task Reporter', () => {
  it('should test', async () => {
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
});
