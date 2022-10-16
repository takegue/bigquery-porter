import type { Reporter, ReporterTask } from '../types.js';

class JSONReporter implements Reporter {
  tasks: ReporterTask[] = [];

  onInit(tasks: ReporterTask[]) {
    this.tasks = tasks;
  }

  async onUpdate() {
  }

  onFinished() {
    for (const task of this.tasks) {
      console.log(JSON.stringify(
        {
          name: task.name,
          status: task.status,
          result: {
            message: task.message,
            error: task.error,
          },
        },
        null,
      ));
    }
  }
}

export { JSONReporter };
