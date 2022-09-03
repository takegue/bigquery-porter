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

  report() {
    let s = '';
    let c = pc.red;
    switch (this.status) {
      case 'success':
        s = F_CHECK;
        c = pc.green;
        break;

      case 'failed':
        s = ` ${F_CROSS}`;
        c = pc.red;
        break;

      case 'running':
        s = this.spin();
        c = pc.gray;
        break;

      case 'pending':
        return '';
    }

    const title = c(`${s} ${this.name}`);
    if (this.error) {
      return `${title}\n    ${pc.bold(this.error)}`.trim();
    } else {
      const msg = this.message ? ` (${this.message ?? ''})` : '';
      return `${title} ${msg}`.trim();
    }
  }
}

class Reporter {
  tasks: Task[];
  constructor(tasks: Task[]) {
    this.tasks = tasks;
  }

  push(task: Task) {
    this.tasks.push(task);
  }

  async *show_until_finished() {
    while (this.tasks.some((t) => !t.done())) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      yield this.tasks
        .sort((l, r) => l.name.localeCompare(r.name))
        .map((t) => t.report()).filter((s) => s).join('\n  ');
    }
  }
}

export { Reporter, Task };
