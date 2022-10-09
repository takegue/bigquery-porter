import { Console } from 'node:console';
import { Writable } from 'node:stream';

const unknownTestId = 'unknown';

function getWorkerState() {
  return {
    id: 'test',
  };
}

export function spyConsole() {
  const stdoutBuffer = new Map<string, any[]>();
  const stderrBuffer = new Map<string, any[]>();

  const stdout = new Writable({
    write(data, _, callback) {
      const id = getWorkerState()?.id ?? unknownTestId;
      let buffer = stdoutBuffer.get(id);
      if (!buffer) {
        buffer = [];
        stdoutBuffer.set(id, buffer);
      }
      buffer.push(data);
      callback();
    },
  });

  const stderr = new Writable({
    write(data, _, callback) {
      const id = getWorkerState()?.id ?? unknownTestId;
      let buffer = stderrBuffer.get(id);
      if (!buffer) {
        buffer = [];
        stderrBuffer.set(id, buffer);
      }
      buffer.push(data);
      callback();
    },
  });

  globalThis.console = new Console({
    stdout,
    stderr,
    colorMode: true,
    groupIndentation: 2,
  });

  return {
    buffer: {
      stdout: stdoutBuffer,
      stderrBuffer: stdoutBuffer,
    },
    teardown: () => {
      globalThis.console = new Console({
        stdout: process.stdout,
        stderr: process.stderr,
      });
    },
  };
}

/*
const main = function () {
  console.log('start');
  const { buffer, teardown } = spyConsole();

  console.log('log1');
  console.warn('log2');

  teardown();
  console.warn('end');
  for (const data of buffer.stdout.get('test') ?? []) {
    console.log(data.toString());
  }
};

main();
*/
