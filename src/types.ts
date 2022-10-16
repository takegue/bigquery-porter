type TaskStatus = 'pending' | 'running' | 'success' | 'failed';

type TaskResult = {
  message: string;
  error: string;
};

type TaskJob = Promise<string | undefined>;

interface ReporterTask {
  name: string;
  status: TaskStatus;
  runningPromise: TaskJob | undefined;
  error: string | undefined;
  message: string | undefined;
  run: () => Promise<void>;
  done: () => boolean;
}

interface Reporter {
  onInit: (tasks: ReporterTask[]) => void;
  onUpdate: () => Promise<void>;
  onFinished: () => void;
}

export { Reporter, ReporterTask, TaskJob, TaskResult, TaskStatus };
