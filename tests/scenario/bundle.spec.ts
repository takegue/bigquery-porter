import { afterEach, describe, expect, it, vi } from 'vitest';
import { createCLI } from '../../src/commands/cli.js';

import { Command } from 'commander';

describe('CLI: bundle', () => {
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
  afterEach(() => {
    vi.resetAllMocks();
  });

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
        cli.parse(['bundle', '--help'], { from: 'user' });
      },
    ).toThrow();
    expect(out).toMatchSnapshot();
    expect(err).toMatchSnapshot();
  });

  it('core', async () => {
    const out = [];
    const err = [];

    const cli = createCLI();
    setupCommanderForTest(cli, out, err);
    for (const c of cli.commands) {
      setupCommanderForTest(c, out, err);
    }
    vi.spyOn(console, 'log')
      .mockImplementation((s: string) => {
        out.push(s);
      });

    await cli.parseAsync(
      'bundle -C ./examples'.split(' '),
      {
        from: 'user',
      },
    );
    expect(out).toMatchSnapshot();
    expect(err).toMatchSnapshot();
  });
});
