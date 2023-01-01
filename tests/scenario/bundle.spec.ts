import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createCLI } from '../../src/commands/cli.js';

import { Command } from 'commander';

interface CLITestContext {
  cli: Command;
  out: string[];
  err: string[];
}

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

  beforeEach<CLITestContext>(async (ctx) => {
    const out: string[] = [];
    const err: string[] = [];
    const cli = createCLI();

    setupCommanderForTest(cli, out, err);
    for (const c of cli.commands) {
      setupCommanderForTest(c, out, err);
    }

    vi.spyOn(console, 'log')
      .mockImplementation((s: string) => {
        out.push(s);
      });
    vi.spyOn(console, 'error')
      .mockImplementation((s: string) => {
        err.push(s);
      });

    ctx.cli = cli;
    ctx.out = out;
    ctx.err = err;
  });

  afterEach(() => {
    vi.resetAllMocks();
  });

  it<CLITestContext>('bundle --help', ({ meta, cli, out, err }) => {
    expect(
      () => {
        cli.parse(meta.name.split(' '), { from: 'user' });
      },
    ).toThrow();
    expect(out).toMatchSnapshot();
    expect(err).toMatchSnapshot();
  });

  it<CLITestContext>(
    'bundle -C ./examples',
    async ({ meta, cli, out, err }) => {
      await cli.parseAsync(meta.name.split(' '), { from: 'user' });
      expect(out).toMatchSnapshot();
      expect(err).toMatchSnapshot();
    },
  );

  it<CLITestContext>(
    'bundle -C ./examples --enable-datalineage',
    async ({ meta, cli, out, err }) => {
      await cli.parseAsync(meta.name.split(' '), { from: 'user' });
      expect(out).toMatchSnapshot();
      expect(err).toMatchSnapshot();
    },
  );
});
