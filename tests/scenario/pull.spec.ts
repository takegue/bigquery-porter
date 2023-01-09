import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createCLI } from '../../src/commands/cli.js';

import { tmpdir } from 'node:os';
import * as path from 'node:path';
import * as fs from 'node:fs';

import { Command } from 'commander';

interface CLITestContext {
  cli: Command;
  out: string[];
  err: string[];
  rootPath: string;
}

describe('CLI: pull', () => {
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
        cli.parse(['pull', '--help'], { from: 'user' });
      },
    ).toThrow();
    expect(out).toMatchSnapshot();
    expect(err).toMatchSnapshot();
  });

  it('example', async () => {
    const out = [];
    const err = [];

    const cli = createCLI();
    setupCommanderForTest(cli, out, err);
    for (const c of cli.commands) {
      setupCommanderForTest(c, out, err);
    }
    await cli.parseAsync(
      'pull --format=json -C ./examples'.split(' '),
      {
        from: 'user',
      },
    );
    expect(out).toMatchSnapshot();
    expect(err).toMatchSnapshot();
  });
});

describe('CLIv2: pull', () => {
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

    // vi.spyOn(console, 'log')
    //   .mockImplementation((s: string) => {
    //     out.push(s);
    //   });
    // vi.spyOn(console, 'error')
    //   .mockImplementation((s: string) => {
    //     err.push(s);
    //   });

    ctx.cli = cli;
    ctx.out = out;
    ctx.err = err;
    ctx.rootPath = path.resolve(fs.mkdtempSync(`${tmpdir()}${path.sep}`));
  });

  afterEach(() => {
    vi.resetAllMocks();
  });

  it<CLITestContext>(
    `pull --format=json bigquery-public-data.baseball`,
    async ({ meta, cli, out, err, rootPath }) => {
      console.error(rootPath);
      console.log(rootPath);

      await cli.parseAsync(
        [...meta.name.split(' '), ...['-C', rootPath]],
        {
          from: 'user',
        },
      );
      expect(out.length).toMatchSnapshot();
      expect(err).toMatchSnapshot();
    },
  );
});
