import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createCLI } from '../../src/commands/cli.js';

import { Command } from 'commander';

interface CLITestContext {
  cli: Command;
  out: string[];
  err: string[];
}

describe('CLI: push', () => {
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

  const stableCases = [
    [`push --help`],
    [`push --format=json --dry-run -C ./examples`],
    [`push --format=json --dry-run -C ./examples --parameter flag:bool:true --parameter num:INTGER:1`],
  ];

  it<CLITestContext>(
    `push --help`,
    async ({ meta, cli, out, err }) => {
      await expect(
        async () => {
          await cli.parseAsync(meta.name.split(' '), { from: 'user' });
        },
      ).rejects.toThrow();
      expect(out).toMatchSnapshot();
      expect(err).toMatchSnapshot();
    },
  );

  it<CLITestContext>(
    `push --format=json --dry-run -C ./examples`,
    async ({ meta, cli, out, err }) => {
      await expect(
        async () => {
          await cli.parseAsync(meta.name.split(' '), { from: 'user' });
        },
      ).rejects.toThrow();
      expect(out).toMatchSnapshot();
      expect(err).toMatchSnapshot();
    },
  );

  it<CLITestContext>(
    `push --format=json --dry-run -C ./examples --parameter flag:bool:true --parameter num:INTGER:1`,
    async ({ meta, cli, out, err }) => {
      await expect(
        async () => {
          await cli.parseAsync(meta.name.split(' '), { from: 'user' });
        },
      ).rejects.toThrow();
      expect(out).toMatchSnapshot();
      expect(err).toMatchSnapshot();
    },
  );

  it<CLITestContext>(
    `push --format=json -C ./examples`,
    async ({ meta, cli, out, err }) => {
      await expect(
        async () => {
          await cli.parseAsync(meta.name.split(' '), { from: 'user' });
        },
      ).rejects.toThrow();
      expect(out.length).toMatchSnapshot();
      expect(err).toMatchSnapshot();
    },
  );
});
