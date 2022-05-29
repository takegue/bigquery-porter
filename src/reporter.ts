
import readline from 'readline';
import process from 'process';
import pc from 'picocolors';
import {F_CHECK, F_CROSS} from '../src/figures.js';

export const clearScreen = () => {
 const repeatCount = (process.stdout?.rows ?? 0) - 2;
 const blank = repeatCount > 0 ? '\n'.repeat(repeatCount) : '';
 console.log(blank)

 readline.cursorTo(process.stdout, 0, 0)
 readline.clearScreenDown(process.stdout)
}

export const spinnerFrames = process.platform === 'win32'
  ? ['-', '\\', '|', '/']
  : ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']

export function elegantSpinner() {
  let index = 0

  return () => {
    index = ++index % spinnerFrames.length
    return spinnerFrames[index] ?? '';
  }
}

export class Task {
  name: string;
  job: () => Promise<void>;
  status: "pending" | "running" | "done" | "failed";
  spin: () => string;
  runningPromise: Promise<void> | undefined;

  constructor(name: string, job: () => Promise<void>) {
    this.name = name;
    this.job = job;
    this.status = 'pending';
    this.spin = elegantSpinner();
  }

  async run() {
    this.status = 'running'
    // start job
    this.runningPromise = this.job();
    await this.runningPromise;
    this.status = 'done';
  }

  report() {
    let s = '';
    let c = pc.red;
    switch (this.status) {
      case 'done':
        s = F_CHECK;
        c = pc.green;
        break;

      case 'failed':
        s = F_CROSS;
        c = pc.red;
        break;

      case 'running':
        s = this.spin();
        c = pc.gray;
        break;

      case 'pending':
        return ''
    }

    return c(`${s} ${this.name}`);
  }
}


