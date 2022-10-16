type Success<T> = {
  status: 'success';
  result: T;
};

type Running<T> = {
  status: 'running';
  result: T | undefined;
};

type Pending = {
  status: 'pending';
};

type Failed<T> = {
  status: 'failed';
  result: T;
  error: string;
};

type TaskResult<T> =
  | Success<T>
  | Running<T>
  | Pending
  | Failed<T>;

interface Stringable {
  toString: () => string;
}

interface Seriaziable {
  toObject: () => Object;
}

interface ReporterTask<T> {
  name: string;
  runningPromise: Promise<T> | undefined;
  run: () => Promise<void>;
  done: () => boolean;
  result: () => TaskResult<T>;
}

interface Reporter<T> {
  onInit: (tasks: ReporterTask<T>[]) => void;
  onUpdate: () => Promise<void>;
  onFinished: () => void;
}

export {
  Failed,
  Pending,
  Reporter,
  ReporterTask,
  Seriaziable,
  Stringable,
  Success,
  TaskResult,
};
