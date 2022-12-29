import { describe, expect, it } from 'vitest';
import { createCLI } from '../../src/commands/cli.js';

import { Command } from 'commander';

describe('CLI: format', () => {
  const setupCommanderForTest = (c: Command, out: string[], err: string[]) => {
    c
      .exitOverride((e: Error) => {
        throw e;
      })
      .configureOutput({
        writeOut: (s) => out.push(s),
        writeErr: (s) => err.push(s),
      });
  };
  it('--help', async () => {
    const out = [];
    const err = [];

    expect(
      () => {
        const cli = createCLI();
        setupCommanderForTest(cli, out, err);
        for (const c of cli.commands) {
          setupCommanderForTest(c, out, err);
        }
        cli.parse(['format', '--help'], { from: 'user' });
      },
    ).toThrow();
    expect(out).toMatchSnapshot();
    expect(err).toMatchSnapshot();
  });

  it('--dyr-run', async () => {
    const out = [];
    const err = [];

    const cli = createCLI();
    setupCommanderForTest(cli, out, err);
    for (const c of cli.commands) {
      setupCommanderForTest(c, out, err);
    }
    await cli.parseAsync(
      'format -C ./examples'.split(' '),
      {
        from: 'user',
      },
    );
    expect(out).toMatchSnapshot();
    expect(err).toMatchSnapshot();
  });
});
