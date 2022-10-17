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
      .catch((e) => {
        this._result = { status: 'failed', error: e.message.trim() } as Failed<
          T
        >;
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
  elapsedTimeMs?: number;
};

class BigQueryJobTask extends BaseTask<BQJob> implements Stringable {
  override toString(): string {
    const result = this.result();
    if (['pending', 'running'].includes(result.status)) {
      return ``;
    }

    if (result.status === 'failed') {
      return `(Job ID: ${result.result.jobID}) ${result.error}`;
    }

    if (result.status === 'success') {
      const payload = [];
      if (result.result.jobID) {
        payload.push(`ID: ${result.result.jobID}`);
      }

      if (result.result.totalBytesProcessed) {
        payload.push(humanFileSize(result.result.totalBytesProcessed));
      }

      if (result.result.elapsedTimeMs) {
        payload.push(msToTime(result.result.elapsedTimeMs));
      }

      return payload.join(', ');
    }
    return '';
  }
}

export { BigQueryJobTask, BQJob, Task };
