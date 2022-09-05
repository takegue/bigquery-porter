import process from 'node:process';
import pc from 'picocolors';
import { F_CHECK, F_CROSS } from '../src/figures.js';

const spinnerFrames = process.platform === 'win32'
  ? ['-', '\\', '|', '/']
  : ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

function elegantSpinner() {
  let index = 0;

  return () => {
    index = ++index % spinnerFrames.length;
    return spinnerFrames[index] ?? '';
  };
}

type TaskJob = Promise<string | undefined>;
class Task {
  name: string;
  job: () => TaskJob;
  status: 'pending' | 'running' | 'success' | 'failed';
  spin: () => string;
  runningPromise: TaskJob | undefined;
  error: string | undefined;
  message: string | undefined;

  constructor(name: string, job: () => TaskJob) {
    this.name = name;
    this.job = job;
    this.status = 'pending';
    this.spin = elegantSpinner();
  }

  async run() {
    if (this.status != 'pending') {
      return
    }

    this.status = 'running';
    // start job
    this.runningPromise = this.job();
    await this.runningPromise
      .then((msg) => {
        this.status = 'success';
        this.message = msg;
      })
      .catch((e) => {
        this.status = 'failed';
        this.error = e.message.trim();
      });
  }

  done() {
    return ['success', 'failed'].includes(this.status);
  }

}

class Reporter {
  tasks: Task[];
  separator: string = '/'
  constructor(tasks: Task[]) {
    this.tasks = tasks;
  }

  push(task: Task) {
    this.tasks.push(task);
  }

  report_task(task: Task): string {
    let s = '';
    let c = pc.red;
    switch (task.status) {
      case 'success':
        s = F_CHECK;
        c = pc.green;
        break;

      case 'failed':
        s = `${F_CROSS}`;
        c = pc.red;
        break;

      case 'running':
        s = task.spin();
        c = pc.gray;
        break;

      case 'pending':
        return '';
    }

    const title = c(`${s} ${task.name.split(this.separator).pop()}`);
    if (task.error) {
      return `${title}: ${pc.bold(task.error)}`.trim();
    } else {
      const msg = task.message ? ` (${task.message ?? ''})` : '';
      return `${title} ${msg}`.trim();
    }
  }

  report_tree(tasks: Task[], level = 0, max_level = 4): string {
    const groups = tasks.reduce((acc, t) => {
      const parts = t.name.split(this.separator);
      const key = parts.length === level + 1 ? '#tail' : (parts[level] ?? '#none');
      if (acc.get(key) === undefined) {
        acc.set(key, []);
      }
      acc.get(key)?.push(t);
      return acc
    }, new Map<string, Task[]>());

    let s = '';
    const childStingifier = (tasks: Task[], level: number) => {
      const spaces = '  '.repeat(level);
      const body = tasks.map((t) => this.report_task(t)).filter((s) => s).join('\n' + spaces)

      return `${spaces}${body}\n`
    }
    for (const [group_key, tasks] of groups) {
      if (group_key == '#tail') {
        s += childStingifier(tasks, level)
      }
      else {
        s += '  '.repeat(level) + pc.underline(group_key) + '\n';
        if (level < max_level) {
          s += this.report_tree(tasks, level + 1, max_level);
        } else {
          s += childStingifier(tasks, level + 1)
        }
      }
    }
    return s;
  }

  async *show_until_finished() {
    while (this.tasks.some((t) => !t.done())) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      yield this.report_tree(this.tasks);
    }
  }
}

export { Reporter, Task };
