interface ReporterTask {
  id: string;
  message: string;
  status: 'pending' | 'running' | 'success' | 'failed';
  runningPromise: Promise<string | undefined> | undefined;

  run: () => Promise<void>;
  done: () => boolean;
}

interface Reporter {
  push: (task: ReporterTask) => void;
  show_until_finished: () => AsyncGenerator<string, void, unknown>;
}

export { Reporter, ReporterTask };
