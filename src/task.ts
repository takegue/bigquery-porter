import type {
  Failed,
  Pending,
  ReporterTask,
  Running,
  Success,
  TaskResult,
} from '../src/types.js';

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
export { Task };
