import { afterEach, describe, expect, it, vi } from 'vitest';
import { DefaultReporter } from '../src/reporter/default.js';
import { JSONReporter } from '../src/reporter/json.js';
import { Task } from '../src/tasks/base.js';

describe('Reporter: Default Reporter', () => {
  it.concurrent('Should test', async () => {
    const reporter = new DefaultReporter();
    const tasks = [
      new Task(
        'test1',
        () => new Promise((resolve) => setTimeout(resolve, 50)),
      ),
    ];
    tasks.forEach((t) => t.run());

    reporter.onInit(tasks);
    await reporter.onUpdate();
    reporter.onFinished();
  });
});

describe('Reporter: JSON Reporter', () => {
  afterEach(() => {
    vi.resetAllMocks();
  });

  it('core', async () => {
    const out = [];
    vi.spyOn(console, 'log').mockImplementation((s) => {
      out.push(s);
    });

    const reporter = new JSONReporter();
    const tasks = [
      new Task(
        'success task',
        () => new Promise((resolve) => resolve('1')),
      ),
      new Task(
        'failed task',
        () => new Promise((_, reject) => reject(new Error('error'))),
      ),
    ];
    tasks.forEach((t) => t.run());

    reporter.onInit(tasks);
    while (tasks.some((t) => !t.done())) {
      await reporter.onUpdate();
    }
    reporter.onFinished();

    expect(out).toMatchSnapshot();
  });
});
