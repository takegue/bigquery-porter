import { describe, it } from 'vitest';
import { DefaultReporter } from '../src/reporter/default.js';
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
