#!/usr/bin/env node
import * as fs from 'node:fs';

import { formatLocalfiles } from '../src/commands/fix.js';
import {
  createBundleSQL,
  pushLocalFilesToBigQuery,
} from '../src/commands/push.js';
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
      '--label <key:value...>',
      'A label to set on a query job. The format is "key:value"; repeat this option to specify a list of values',
    )
    .option(
      '-p, --parameter <key:value...>',
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

      if (cmdOptions.label) {
        jobOption.labels = {
          ...Object.fromEntries(
            (cmdOptions.label as string[]).map((l) => l.split(':')),
          ),
          'bqporter-enable': 'true',
        };
      }

      if (cmdOptions.parameter) {
        jobOption.params = {} as { [param: string]: any };
        for (const param of cmdOptions.parameter as string[]) {
          if (fs.existsSync(param) && fs.lstatSync(param).isFile()) {
            jobOption.params = {
              ...jobOption.params,
              ...JSON.parse(fs.readFileSync(param, 'utf8')),
            };
            continue;
          }

          const elms = param.split(':');
          const name = elms[0];
          if (name === undefined) {
            console.error(`Invalid Parameter: ${param}`);
            return;
          }
          const rawValue = elms[2];
          if (rawValue === undefined) {
            console.error(`Invalid Parameter: ${param}`);
            return;
          }

          (jobOption.params as { [param: string]: any })[name] = (() => {
            if (!elms[1]) {
              return rawValue;
            }

            if (elms[1]?.toUpperCase() === 'STRING') {
              return rawValue;
            }

            if (elms[1]?.toUpperCase() === 'INTGER') {
              return parseInt(rawValue);
            }

            if (elms[1]?.toUpperCase() === 'BOOL') {
              return rawValue.toLowerCase() == 'true';
            }

            if (elms[1]?.toUpperCase() === 'TIMESTAMP') {
              const p = new Date(Date.parse(rawValue));
              if (!isNaN(p.valueOf())) {
                return p;
              }
            }

            if (elms[1]) {
              try {
                return JSON.parse(rawValue);
              } catch (e) {
                return rawValue;
              }
            }
            return rawValue;
          })();
        }
      }

      const bqClient = buildThrottledBigQueryClient(
        parseInt(cmdOptions.threads),
        500,
      );

      for (const project of projects) {
        const ctx = {
          BigQuery: {
            client: bqClient,
            projectId: project,
          },
          rootPath: rootDir,
          dryRun: cmdOptions.dryRun ?? false,
          force: cmdOptions.force ?? false,
          reporter: cmdOptions.format ?? 'console',
        };

        const failed = await pushLocalFilesToBigQuery(ctx, jobOption);
        if (failed > 0) {
          program.error(`Some tasks are failed(Failed: ${failed})`, {
            exitCode: 1,
          });
        }
      }
    });

  const pullCommand = new Command('pull')
    .description('pull dataset and its tabald and routine information')
    .argument('[projects...]')
    .option('--all', 'Pulling All BugQuery Datasets', false)
    .option('--with-ddl', 'Pulling BigQuery Resources with DDL SQL', false)
    .action(async (cmdProjects: string[] | undefined, _, cmd) => {
      const cmdOptions = cmd.optsWithGlobals();
      const projects = cmdProjects ?? [];

      const options = {
        rootDir: cmdOptions.rootPath,
        withDDL: cmdOptions.withDdl,
        forceAll: cmdOptions.all,
        concurrency: cmdOptions.concurrency,
      };

      const failed = await (async () => {
        if (projects.length > 0) {
          return await Promise.allSettled(
            projects.map(async (p) =>
              p == '@default'
                ? await pullBigQueryResources({ ...options })
                : await pullBigQueryResources({ projectId: p, ...options })
            ),
          ).then((results) =>
            results.filter((r) => r.status !== 'fulfilled' || r.value > 0)
              .length
          );
        } else {
          return await pullBigQueryResources(options);
        }
      })();

      if (failed > 0) {
        program.error(`Some tasks are failed(Failed: ${failed})`, {
          exitCode: 1,
        });
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

  const bundleCommand = new Command('bundle')
    .description(
      'Bundle SQLs into single execuatale BigQuery Script',
    )
    .argument('[projects...]')
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

      const bqClient = buildThrottledBigQueryClient(
        parseInt(cmdOptions.threads),
        500,
      );
      for (const project of projects) {
        const ctx = {
          BigQuery: {
            client: bqClient,
            projectId: project ?? '@default',
          },
          rootPath: rootDir,
          dryRun: cmdOptions.dryRun ?? false,
          force: cmdOptions.force ?? false,
          reporter: cmdOptions.format ?? 'console',
        };

        const sql = await createBundleSQL(ctx);
        console.log(sql);
      }
    });

  program.addCommand(pushCommand);
  program.addCommand(pullCommand);
  program.addCommand(formatCommmand);
  program.addCommand(bundleCommand);

  program.parse();
}

const main = async () => {
  createCLI();
};

main();
