import readline from 'node:readline';
import { isatty } from 'node:tty';

import { Console } from 'node:console';
import { Writable } from 'node:stream';

const unknownTestId = 'unknown';

async function* fromStdin(): AsyncGenerator<string> {
  const rl = readline.createInterface({
    input: process.stdin,
  });

  for await (const line of rl) {
    yield line;
  }
  rl.close();
}

const getTargetFiles = async () => {
  if (isatty(0)) {
    return undefined;
  }

  const inputFiles: string[] = [];
  for await (const line of fromStdin()) {
    inputFiles.push(line);
  }
  return inputFiles;
};

function spyConsole(whoami: () => string) {
  const stdoutBuffer = new Map<string, any[]>();
  const stderrBuffer = new Map<string, any[]>();

  const stdout = new Writable({
    write(data, _, callback) {
      const id = whoami() ?? unknownTestId;
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
      const id = whoami() ?? unknownTestId;
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
      stderr: stdoutBuffer,
    },
    teardown: () => {
      globalThis.console = new Console({
        stdout: process.stdout,
        stderr: process.stderr,
      });
    },
  };
}

export { getTargetFiles, spyConsole };
