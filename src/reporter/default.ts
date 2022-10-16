import process from 'node:process';
import pc from 'picocolors';
import logUpdate from 'log-update';

import { F_CHECK, F_CROSS } from '../../src/figures.js';
import { spyConsole } from '../../src/runtime/console.js';

import type {
  Reporter, ReporterTask,
} from '../../src/types.js';

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

class DefaultReporter implements Reporter {
  tasks: ReporterTask[] = [];
  hooks: (() => void)[] = [];
  separator: string = '/';
  spinnerMap: WeakMap<ReporterTask, () => string> = new WeakMap();

  report_task(task: ReporterTask): string {
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
        if (!this.spinnerMap.has(task)) {
          this.spinnerMap.set(task, elegantSpinner());
        }
        const spin = this.spinnerMap.get(task);
        if (!spin) {
          throw new Error('Unreachable codes');
        }

        s = spin();
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

  report_tree(tasks: ReporterTask[], level = 0, max_level = 4): string {
    const groups = tasks.reduce((acc, t) => {
      const parts = t.name.split(this.separator);
      const key = parts.length === level + 1
        ? '#tail'
        : (parts[level] ?? '#none');
      if (acc.get(key) === undefined) {
        acc.set(key, []);
      }
      acc.get(key)?.push(t);
      return acc;
    }, new Map<string, ReporterTask[]>());

    let s = '';
    const childStingifier = (tasks: ReporterTask[], level: number) => {
      const spaces = '  '.repeat(level);
      const body = tasks.map((t) => this.report_task(t)).filter((s) => s).join(
        '\n' + spaces,
      );

      return `${spaces}${body}\n`;
    };
    for (const [group_key, tasks] of groups) {
      if (group_key == '#tail') {
        s += childStingifier(tasks, level);
      } else {
        s += '  '.repeat(level) + pc.underline(group_key) + '\n';
        if (level < max_level) {
          s += this.report_tree(tasks, level + 1, max_level);
        } else {
          s += childStingifier(tasks, level + 1);
        }
      }
    }
    return s;
  }

  onInit(tasks: ReporterTask[]) {
    this.tasks = tasks;
    this.spinnerMap = new WeakMap();
    logUpdate.done();
    const { buffer, teardown } = spyConsole(() => 'unknown');
    this.hooks.push(
      teardown,
      () => {
        const stdout = buffer.stdout.get('unknown');
        const stderr = buffer.stderr.get('unknown');
        if (stdout && stdout.length > 0) {
          console.log('STDOUT: ');
          for (const data of stdout) {
            console.log(data.toString());
          }
        }

        if (stderr && stderr.length > 0) {
          console.log('STDERR: ');
          for (const data of stderr) {
            console.log(data.toString());
          }
        }
      },
    );
  }

  async onUpdate() {
    const report = this.report_tree(this.tasks);
    const taskCount = this.tasks.filter((t) => !t.done()).length;
    logUpdate(
      `Tasks: remaing ${taskCount}\n` +
      report,
    );
  }

  onFinished() {
    this.hooks.forEach((h) => h());
  }
}

export { DefaultReporter };
