import { describe, expect, it, vi } from 'vitest';
import { createCLI } from '../../src/commands/cli.js';

import { Command } from 'commander';

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
        cli.parse(['push', '--help'], { from: 'user' });
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
    vi.spyOn(console, 'log')
      .mockImplementation((s: string) => {
        out.push(s);
      });
    vi.spyOn(console, 'error')
      .mockImplementation((s: string) => {
        err.push(s);
      });

    await expect(
      async () => {
        await cli.parseAsync(
          'push --dry-run --format=json -C ./examples'.split(' '),
          {
            from: 'user',
          },
        );
      },
    ).rejects.toThrow();
    expect(out).toMatchSnapshot();
    expect(err).toMatchSnapshot();
  });

  it('run', async () => {
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
    vi.spyOn(console, 'error')
      .mockImplementation((s: string) => {
        err.push(s);
      });

    await expect(
      async () => {
        await cli.parseAsync(
          'push --format=json -C ./examples'.split(' '),
          {
            from: 'user',
          },
        );
      },
    ).rejects.toThrow();
    expect(out.length).toMatchInlineSnapshot(`31`);
    expect(err).toMatchSnapshot();
  });
});
