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
        async () => {
          await new Promise((resolve) => setTimeout(resolve, 10));
          return 'test';
        },
      ),
    ];
    tasks.forEach((t) => t.run());

    reporter.onInit(tasks);
    while (tasks.some((t) => !t.done())) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    reporter.onUpdate();
    reporter.onFinished();
  });

  it.concurrent('Too long message tasks', async () => {
    const reporter = new DefaultReporter();
    const tasks = [
      new Task(
        'success'.repeat(100),
        async () => {
          await new Promise((resolve) => setTimeout(resolve, 10));
          return 'msg';
        },
      ),
      new Task(
        'failed',
        async () => {
          throw new Error('msg '.repeat(100));
        },
      ),
    ];
    tasks.forEach((t) => t.run());

    reporter.onInit(tasks);
    while (tasks.some((t) => !t.done())) {
      reporter.onUpdate();
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    reporter.onUpdate();
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
