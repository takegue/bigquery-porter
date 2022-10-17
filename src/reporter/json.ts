import type { Reporter, ReporterTask } from '../types.js';

class JSONReporter<T extends Object> implements Reporter<T> {
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
            result: result.result,
          }, null));
          break;
        case 'failed':
          console.log(JSON.stringify({
            name: task.name,
            error: result.error,
            result: result.result,
          }, null));
          break;
        default:
          throw 'Unexpected task status: ${task} ${result.status}';
      }
    }
  }
}

export { JSONReporter };
