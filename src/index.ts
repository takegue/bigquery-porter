#!/usr/bin/env node

import readline from 'node:readline';
import { isatty } from 'node:tty';
import { BigQuery, Query } from '@google-cloud/bigquery';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { walk } from '../src/util.js';
import { formatLocalfiles } from '../src/commands/fix.js';
import { pushBigQueryResourecs } from '../src/commands/push.js';
import { pullBigQueryResources } from '../src/commands/pull.js';
import { path2bq } from '../src/bigquery.js';

import 'process';
import { Command } from 'commander';

export async function pushLocalFilesToBigQuery(
  options: {
    rootDir: string;
    projectId?: string;
    concurrency?: number;
    dryRun?: boolean;
    force?: boolean;
    maximumBytesBilled?: string;
    labels?: { [label: string]: string };
    params?: any[] | { [param: string]: any };
  },
) {
  const rootDir = options.rootDir;
  const inputFiles: string[] = await (async () => {
    if (isatty(0)) {
      return await walk(rootDir);
    }
    const rl = readline.createInterface({
      input: process.stdin,
    });

    const buffer: string[] = [];
    for await (const line of rl) {
      buffer.push(line);
    }
    rl.close();
    return buffer;
  })();

  const files = inputFiles
    .filter((p: string) => p.endsWith('sql'))
    .filter((p: string) => p.includes(options.projectId ?? '@default'));

  const jobOption: Query = {};
  if (options.dryRun) {
    jobOption.dryRun = options.dryRun;
  }
  if (options.maximumBytesBilled) {
    jobOption.maximumBytesBilled = options.maximumBytesBilled;
  }
  if (options.labels) {
    jobOption.labels = {
      ...options.labels,
      'bqporter-enable': 'true',
    };
  }
  if (options.params) {
    jobOption.params = options.params;
  }

  await pushBigQueryResourecs(
    rootDir,
    files,
    options.concurrency ?? 1,
    jobOption,
  );
}

function createCLI() {
  const program = new Command();

  program
    .description('Easy and Quick BigQuery Deployment Tool')
    // Global Options
    .option('-n, --threads <threads>', 'API Call Concurrency', '8')
    .option('-C, --root-path <rootPath>', 'Root Directory', './bigquery');

  const pushCommand = new Command('push')
    .description(
      'Deploy your local BigQuery Resources in topological-sorted order',
    )
    .argument('[...projects]')
    .option(
      '--force',
      'Force to apply changes such as deletion without confirmation',
      false,
    )
    .option(
      '--label <key:value>',
      'A label to set on a query job. The format is "key:value"; repeat this option to specify a list of values',
    )
    .option(
      '--parameter <key:value>',
      `Either a file containing a JSON list of query parameters, or a query parameter in the form "name:type:value".` +
        `An empty name produces a positional parameter. The type may be omitted to assume STRING: name::value or ::value.` +
        `The value "NULL" produces a null value. repeat this option to specify a list of values`,
    )
    .option(
      '--maximum_bytes_billed <number of bytes>',
      'The upper limit of bytes billed for the query.',
    )
    .option('--dry-run', 'Dry Run', false)
    .action(async (cmdProjects: string[] | undefined, _, cmd) => {
      const cmdOptions = cmd.optsWithGlobals();
      const projects = cmdProjects ?? [];

      const rootDir = cmdOptions.rootPath;
      if (!rootDir) {
        console.error('CLI Error');
        return;
      }

      const options = {
        rootDir: rootDir,
        projectId: projects.pop() ?? '@default',
        concurrency: parseInt(cmdOptions.threads),
        dryRun: cmdOptions.dryRun ?? false,
        force: cmdOptions.force ?? false,
      };

      if (cmdOptions.parameter) {
        (options as any)['params'] = Object.fromEntries(
          (cmdOptions.parameter as string[])
            .map((s) => {
              const elms = s.split(':');
              const rawValue = elms[2];
              return [elms[0], rawValue];
            }),
        );
      }

      const bqClient = new BigQuery();
      for (
        const dataset of await fs.promises.readdir(
          path.join(rootDir, options.projectId),
        )
      ) {
        await cleanupBigQueryDataset(
          bqClient,
          options.rootDir,
          options.projectId ?? '@default',
          path.basename(dataset),
          {
            dryRun: options.dryRun,
            force: options.force,
          },
        ).catch((e) => {
          console.error(e);
        });
      }

      await pushLocalFilesToBigQuery(options);
    });

  const pullCommand = new Command('pull')
    .description('pull dataset and its tabald and routine information')
    .argument('[...projects]')
    .option('--all', 'Pulling All BugQuery Datasets', false)
    .option('--with-ddl', 'Pulling BigQuery Resources with DDL SQL', false)
    // .option('--ddl-useful-rewrite', "Rewrite DDL in useful", {
    //   default: true,
    //   type: [Boolean],
    // })
    .action(async (cmdProjects: string[] | undefined, _, cmd) => {
      const cmdOptions = cmd.optsWithGlobals();
      const projects = cmdProjects ?? [];

      const options = {
        rootDir: cmdOptions.rootPath,
        withDDL: cmdOptions.withDdl,
        forceAll: cmdOptions.all,
        concurrency: cmdOptions.concurrency,
      };
      if (projects.length > 0) {
        await Promise.allSettled(
          projects.map(async (p) =>
            await pullBigQueryResources({ projectId: p, ...options })
          ),
        );
      } else {
        await pullBigQueryResources(options);
      }
    });

  const formatCommmand = new Command('format')
    .description('Fix reference in local DDL files')
    .option('--dry-run', 'dry run', false)
    .action(async (_, cmd) => {
      const cmdOptions = cmd.optsWithGlobals();
      const options = {
        dryRun: cmdOptions.dryRun,
      };
      await formatLocalfiles(cmdOptions.rootPath ?? './bigquery/', options);
    });

  program.addCommand(pushCommand);
  program.addCommand(pullCommand);
  program.addCommand(formatCommmand);

  program.parse();
}

const cleanupBigQueryDataset = async (
  bqClient: BigQuery,
  rootDir: string,
  projectId: string,
  datasetId: string,
  options?: {
    dryRun?: boolean;
    force?: boolean;
  },
): Promise<void> => {
  const defaultProjectId = await bqClient.getProjectId();

  const datasetPath = path.join(rootDir, projectId, datasetId);
  if (!fs.existsSync(datasetPath)) {
    return;
  }

  const routines = await bqClient.dataset(datasetId).getRoutines()
    .then(([rr]) =>
      new Map(
        rr.map((r) => [
          (({ metadata: { routineReference: r } }) =>
            `${r.projectId}.${r.datasetId}.${r.routineId}`)(r),
          r,
        ]),
      )
    );
  const models = await bqClient.dataset(datasetId).getModels()
    .then(([rr]) =>
      new Map(
        rr.map((r) => [
          (({ metadata: { modelReference: r } }) =>
            `${r.projectId}.${r.datasetId}.${r.modelId}`)(r),
          r,
        ]),
      )
    );
  const tables = await bqClient.dataset(datasetId).getTables()
    .then(([rr]) =>
      new Map(
        rr.map((r) => [
          (({ metadata: { tableReference: r } }) =>
            `${r.projectId}.${r.datasetId}.${r.tableId}`)(r),
          r,
        ]),
      )
    );

  // Marks for deletion
  (await walk(datasetPath))
    .filter((p: string) => p.endsWith('sql'))
    .filter((p: string) => p.includes('@default'))
    .forEach((f) => {
      const bqId = path2bq(f, rootDir, defaultProjectId);
      if (f.match(/@routine/) && routines.has(bqId)) {
        // Check Routine
        routines.delete(bqId);
      } else if (f.match(/@model/) && models.has(bqId)) {
        // Check Model
        models.delete(bqId);
      } else {
        if (tables.has(bqId)) {
          // Check Table or Dataset
          tables.delete(bqId);
        }
      }
    });

  const isDryRun = options?.dryRun ?? true;
  const isForce = options?.force ?? false;

  for (const kind of [tables, routines, models]) {
    if (kind.size == 0) {
      continue;
    }

    if (!isForce && !isDryRun) {
      const ans = await prompt(
        [
          `Found BigQuery reousrces with no local files. Do you delete these resources? (y/n)`,
          `  ${[...kind.keys()].join('\n  ')}`,
          'Ans>',
        ].join('\n'),
      ) as string;
      if (!ans.replace(/^\s+|\s+$/g, '').startsWith('y')) {
        continue;
      }
    }
    for (const [bqId, resource] of kind) {
      console.error(`${isDryRun ? '(DRYRUN) ' : ''}Deleting ${bqId}`);
      if (isDryRun) {
        continue;
      }

      await resource.delete();
    }
  }
};

// Use current tty for pipe input
const prompt = (query: string) =>
  new Promise(
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

const main = async () => {
  createCLI();
};

main();
