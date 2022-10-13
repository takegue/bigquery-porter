type TaskStatus = 'pending' | 'running' | 'success' | 'failed';

interface ReporterTask {
  id: string;
  error: string;
  message: string;
  status: TaskStatus;
  run: () => Promise<void>;
  done: () => boolean;
}

interface Reporter {
  push: (task: ReporterTask) => void;
  show_until_finished: () => AsyncGenerator<string, void, unknown>;
}

export { Reporter, ReporterTask };
