#!/usr/bin/env node

import { formatLocalfiles } from '../src/commands/fix.js';
import { pushLocalFilesToBigQuery } from '../src/commands/push.js';
import { pullBigQueryResources } from '../src/commands/pull.js';

import { buildThrottledBigQueryClient } from '../src/bigquery.js';
import type { Query } from '@google-cloud/bigquery';

import 'process';
import { Command } from 'commander';

function createCLI() {
  const program = new Command();

  program
    .description('Easy and Quick BigQuery Deployment Tool')
    // Global Options
    .option('-n, --threads <threads>', 'API Call Concurrency', '8')
    .option('-C, --root-path <rootPath>', 'Root Directory', './bigquery')
    .option(
      '--format <reporter>',
      'formatter option: console., json',
      'console',
    );

  const pushCommand = new Command('push')
    .description(
      'Deploy your local BigQuery Resources in topological-sorted order',
    )
    .argument('[projects...]')
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
      const projects = cmdProjects && cmdProjects.length > 0
        ? cmdProjects
        : ['@default'];

      const rootDir = cmdOptions.rootPath;
      if (!rootDir) {
        console.error('CLI Error');
        return;
      }

      const jobOption: Query = {};
      jobOption.dryRun = cmdOptions.dryRun ?? false;
      if (cmdOptions.maximumBytesBilled) {
        jobOption.maximumBytesBilled = cmdOptions.maximumBytesBilled;
      }

      if (cmdOptions.labels) {
        jobOption.labels = {
          ...(cmdOptions.labels ?? []),
          'bqporter-enable': 'true',
        };
      }

      if (cmdOptions.parameter) {
        jobOption.params = cmdOptions.parameter = Object.fromEntries(
          (cmdOptions.parameter as string[])
            .map((s) => {
              const elms = s.split(':');
              const rawValue = elms[2];
              return [elms[0], rawValue];
            }),
        );
      }

      const bqClient = buildThrottledBigQueryClient(
        parseInt(cmdOptions.threads),
        500,
      );
      for (const project of projects) {
        const ctx = {
          BigQuery: {
            client: bqClient,
            projectId: project ?? await bqClient.getProjectId(),
          },
          rootPath: rootDir,
          dryRun: cmdOptions.dryRun ?? false,
          force: cmdOptions.force ?? false,
          reporter: cmdOptions.format ?? 'console',
        };

        await pushLocalFilesToBigQuery(ctx, jobOption);
      }
    });

  const pullCommand = new Command('pull')
    .description('pull dataset and its tabald and routine information')
    .argument('[projects...]')
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

const main = async () => {
  createCLI();
};

main();
