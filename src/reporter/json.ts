// import type { Reporter, ReporterTask } from '../types.js';

// class JSONReporter implements Reporter {
//   tasks: ReporterTask[] = [];

//   add(task: ReporterTask) {
//     this.tasks.push(task);
//   }

//   async *show_until_finished() {
//     while (this.tasks.some((t) => !t.done())) {
//       await new Promise((resolve) => setTimeout(resolve, 100));
//     }
//   }
// }

// export { JSONReporter };
