import type {
  Failed,
  Pending,
  ReporterTask,
  Running,
  Stringable,
  Success,
  TaskResult,
} from '../src/types.js';

import { humanFileSize, msToTime } from '../src/util.js';

class BaseTask<T> implements ReporterTask<T> {
  name: string;
  job: () => Promise<T>;
  _result: TaskResult<T> = { status: 'pending' } as Pending;
  runningPromise: Promise<T> | undefined;

  constructor(name: string, job: () => Promise<T>) {
    this.name = name;
    this.job = job;
  }

  async run() {
    if (this._result.status != 'pending') {
      return;
    }

    this._result = { status: 'running' } as Running;
    this.runningPromise = this.job();
    await this.runningPromise
      .then((result) => {
        this._result = { status: 'success', result } as Success<T>;
      })
      .catch((e: unknown) => {
        if (e instanceof Error) {
          this._result = {
            status: 'failed',
            error: e.message.trim(),
          } as Failed<T>;
          if (e.cause) {
            this._result.result = e.cause as unknown as T;
          }
        } else {
          throw e;
        }
      });
  }

  result(): TaskResult<T> {
    return this._result;
  }

  done() {
    return ['success', 'failed'].includes(this._result.status);
  }
}

class Task extends BaseTask<string> {
}

type BQJob = {
  jobID?: string;
  totalBytesProcessed?: number;
  totalSlotMs?: number;
  elapsedTimeMs?: number;
  isDryRun?: boolean;
};

class BigQueryJobTask extends BaseTask<BQJob> implements Stringable {
  override toString(): string {
    const result = this.result();
    if (['pending', 'running'].includes(result.status)) {
      return ``;
    }

    if (result.status === 'failed') {
      // return `(Job ID: ${result.result.jobID}) ${result.error}`;
      return `${result.error}`;
    }

    if (result.status === 'success') {
      const payload = [];
      if (result.result?.jobID) {
        payload.push(`ID: ${result.result.jobID}`);
      }

      if (result.result.totalBytesProcessed !== undefined) {
        const category = result.result.isDryRun ? 'estimated' : 'processed';
        payload.push(
          `${category}: ${humanFileSize(result.result.totalBytesProcessed)}`,
        );
      }

      if (result.result.totalSlotMs !== undefined) {
        payload.push(`slot: ${msToTime(result.result.totalSlotMs)}`);
      }

      if (result.result.elapsedTimeMs !== undefined) {
        payload.push(`elapsed: ${msToTime(result.result.elapsedTimeMs)}`);
      }

      return payload.join(', ');
    }
    return '';
  }
}

export { BigQueryJobTask, BQJob, Task };
