import type {
  ReporterTask,
  TaskJob,
  TaskStatus,
} from '../src/types.js';

class Task implements ReporterTask {
  name: string;
  job: () => TaskJob;
  status: TaskStatus;
  runningPromise: TaskJob | undefined;
  error: string | undefined;
  message: string | undefined;

  constructor(name: string, job: () => TaskJob) {
    this.name = name;
    this.job = job;
    this.status = 'pending';
  }

  async run() {
    if (this.status != 'pending') {
      return;
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


export {
  Task
}
