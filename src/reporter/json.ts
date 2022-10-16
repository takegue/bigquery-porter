import type { Reporter, ReporterTask, Seriaziable } from '../types.js';

class JSONReporter<T extends Seriaziable> implements Reporter<T> {
  tasks: ReporterTask<T>[] = [];

  onInit(tasks: ReporterTask<T>[]) {
    this.tasks = tasks;
  }

  async onUpdate() {
  }

  onFinished() {
    for (const task of this.tasks) {
      const result = task.result();
      switch (result.status) {
        case 'success':
          console.log(JSON.stringify({
            name: task.name,
            result: result.result.toObject(),
          }, null));
          break;
        case 'failed':
          console.log(JSON.stringify({
            name: task.name,
            error: result.error,
            result: result.result.toObject(),
          }, null));
          break;
        default:
          throw 'Unexpected task status: ${task} ${result.status}';
      }
    }
  }
}

export { JSONReporter };
