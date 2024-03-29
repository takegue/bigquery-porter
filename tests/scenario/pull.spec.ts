import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { tmpdir } from 'node:os';
import * as path from 'node:path';
import * as fs from 'node:fs';

import { Command } from 'commander';

import { createCLI } from '../../src/commands/cli.js';
import { walk } from '../../src/util.js';

interface CLITestContext {
  cli: Command;
  out: string[];
  err: string[];
  rootPath: string;
}

describe('CLIv2: pull', () => {
  const tempDir = path.join(tmpdir(), `bqport-`);
  const setupCommanderForTest = (c: Command, out: string[], err: string[]) => {
    c
      .exitOverride((e: Error) => {
        throw e;
      })
      .configureOutput({
        writeOut: (s) => out.push(s),
        writeErr: (s) => err.push(s),
      });

    // Fix up display column wdith for comamnder.js
    if (process.stdout.isTTY) {
      process.stdout.columns = 120;
      process.stderr.columns = 120;
    }
  };

  const crawlFs = async (root: string) => {
    const file2content: Map<string, string> = new Map();
    for (const f of await walk(root)) {
      const content = await fs.promises.readFile(f, 'utf-8');
      file2content.set(path.relative(root, f), content);
    }
    return file2content;
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
    ctx.rootPath = path.resolve(fs.mkdtempSync(tempDir));
  });

  afterEach(async () => {
    vi.resetAllMocks();
  });

  it<CLITestContext>(
    `pull --help`,
    async ({ meta, cli, out, err, rootPath }) => {
      await expect(
        cli.parseAsync(meta.name.split(' '), { from: 'user' }),
      ).rejects.toThrow();
      expect(await crawlFs(rootPath)).toMatchSnapshot();
      expect(err).toMatchSnapshot();
      expect(out).toMatchSnapshot();
    },
  );

  it<CLITestContext>(
    `pull`,
    async ({ meta, cli, err, rootPath }) => {
      // Expected sandobx dataset files
      fs.mkdirSync(
        path.join(rootPath, '@default', 'examples'),
        {
          recursive: true,
        },
      );

      await cli.parseAsync([...meta.name.split(' '), ...['-C', rootPath]], {
        from: 'user',
      });
      const files = await crawlFs(path.join(rootPath));
      expect(files.keys()).toMatchSnapshot('Pulled Files: List');
      expect(files).toMatchSnapshot('Pulled Files: Contents');
      expect(err).toMatchSnapshot();
      // expect(out).toMatchSnapshot();
    },
  );

  it<CLITestContext>(
    `pull --all --with-ddl`,
    async ({ meta, cli, err, rootPath }) => {
      await cli.parseAsync([...meta.name.split(' '), ...['-C', rootPath]], {
        from: 'user',
      });
      const files = await crawlFs(path.join(rootPath));
      expect(files.keys()).toMatchSnapshot('Pulled Files: List');
      expect(files).toMatchSnapshot('Pulled Files: Contents');
      expect(err).toMatchSnapshot();
      // expect(out).toMatchSnapshot();
    },
  );

  it<CLITestContext>(
    `pull --format=json bigquery-public-data.missing`,
    async ({ meta, cli, out, err, rootPath }) => {
      await expect(
        cli.parseAsync(meta.name.split(' '), { from: 'user' }),
      ).rejects.toThrow();
      const files = await crawlFs(path.join(rootPath));
      expect(files.keys()).toMatchSnapshot('Pulled Files: List');
      expect(files).toMatchSnapshot('Pulled Files: Contents');

      expect(err).toMatchSnapshot();
      expect(new Set(out)).toMatchSnapshot();
      expect(out.length).toMatchSnapshot();
    },
  );

  it<CLITestContext>(
    `pull --format=json bigquery-public-data.baseball`,
    async ({ meta, cli, out, err, rootPath }) => {
      await cli.parseAsync([...meta.name.split(' '), ...['-C', rootPath]], {
        from: 'user',
      });
      const files = await crawlFs(path.join(rootPath));
      expect(files.keys()).toMatchSnapshot('Pulled Files: List');
      expect(files).toMatchSnapshot('Pulled Files: Contents');

      expect(err).toMatchSnapshot();
      expect(new Set(out)).toMatchSnapshot();
      expect(out.length).toMatchSnapshot();
    },
  );

  it<CLITestContext>(
    `pull --format=json --with-ddl bigquery-public-data.baseball`,
    async ({ meta, cli, out, err, rootPath }) => {
      await cli.parseAsync([...meta.name.split(' '), ...['-C', rootPath]], {
        from: 'user',
      });
      const files = await crawlFs(path.join(rootPath));
      expect(files.keys()).toMatchSnapshot('Pulled Files: List');
      expect(files).toMatchSnapshot('Pulled Files: Contents');

      expect(err).toMatchSnapshot();
      expect(new Set(out)).toMatchSnapshot();
      expect(out.length).toMatchSnapshot();
    },
  );
});
