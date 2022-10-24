import readline from 'node:readline';
import * as fs from 'node:fs';

export function prompt(query: string) {
  return new Promise(
    (resolve) => {
      const tty = fs.createReadStream('/dev/tty');
      const rl = readline.createInterface({
        input: tty,
        output: process.stderr,
      });

      rl.question(query, (ret) => {
        // Order matters and rl should close after use once
        tty.close();
        rl.close();
        resolve(ret);
      });
    },
  );
}
