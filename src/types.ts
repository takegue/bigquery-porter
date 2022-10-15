type TaskStatus = 'pending' | 'running' | 'success' | 'failed';

type TaskResult = {
  message: string;
  error: string;
};

interface ReporterTask {
  name: string;
  status: TaskStatus;

  error: string | undefined;
  message: string | undefined;
  run: () => Promise<void>;
  done: () => boolean;
}

interface Reporter {
  onInit: (tasks: ReporterTask) => void;
  onUpdate: () => Promise<void>;
  onFinished: () => void;
}

export { Reporter, ReporterTask, TaskResult, TaskStatus };
