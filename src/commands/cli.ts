import * as fs from 'node:fs';

import { formatLocalfiles } from '../../src/commands/fix.js';
import { PushContext, pushLocalFilesToBigQuery } from '../../src/commands/push.js';
import { createBundleSQL } from '../../src/commands/bundle.js';
import { pullBigQueryResources } from '../../src/commands/pull.js';
import { importBigQueryResources } from '../../src/commands/import.js';

import { buildThrottledBigQueryClient } from '../../src/bigquery.js';
import type { Query } from '@google-cloud/bigquery';

import 'process';
import { Command } from 'commander';

export function createCLI() {
  const program = new Command();

  program
    .description('Easy and Quick BigQuery Deployment Tool')
    // Global Options
    .option('-n, --threads <threads>', 'API Call Concurrency', '8')
    .option('-C, --root-path <rootPath>', 'Root Directory', './bigquery')
    .option(
      '--enable-datalineage',
      `Enable Data Lineage integration to resolve DAG dependency`,
      false,
    )
    .option(
      '--format <reporter>',
      'formatter option: console, json',
      'console',
    );

  const pushCommand = new Command('push')
    .description(
      'Deploy your local BigQuery Resources in topological-sorted order',
    )
    .argument('[projects...]')
    .option(
      '--sweeping-mode <mode>',
      'Strategy for undefined/orphans BigQuery resources. option: confirm(default), ignore, rename',
      'confirm',
    )
    .option(
      '--label <key:value...>',
      'A label to set on a query job. The format is "key:value"; repeat this option to specify a list of values',
    )
    .option(
      '-p, --parameter <key:value...>',
      `Either a file containing a JSON list of query parameters, or a query parameter in the form "name:type:value". ` +
      `An empty name produces a positional parameter. The type may be omitted to assume STRING: name::value or ::value. ` +
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

      jobOption.labels = { 'bqporter-enable': 'true' };
      if (cmdOptions.label) {
        jobOption.labels = {
          ...jobOption.labels,
          ...Object.fromEntries(
            (cmdOptions.label as string[]).map((l) => l.split(':')),
          ),
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

      const sweepMode: PushContext['SweepStrategy']['mode'] = (() => {
        if(!cmdOptions.sweepingMode) {
          return 'confirm'
        }
        switch (cmdOptions.sweepingMode) {
          case 'ignore':
          case 'rename_and_7d_expire':
          case 'confirm':
          case 'force':
            return cmdOptions.sweepingMode
          default:
            // Error for invalid option
            throw Error(`Invalid --sweeping-mode option: ${cmdOptions.sweepingMode}`);
        }
      })();

      for (const project of projects) {
        const ctx = {
          BigQuery: {
            client: bqClient,
            projectId: project,
          },
          rootPath: rootDir,
          dryRun: cmdOptions.dryRun ?? false,
          SweepStrategy: {
            mode: sweepMode,
            ignorePrefix: 'zorphan__',
          },
          reporter: cmdOptions.format ?? 'console',
          enableDataLineage: cmdOptions.enableDatalineage ?? false,
        } as PushContext;

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
    .argument('[ids...]')
    .option('--all', 'Pulling All BugQuery Datasets', false)
    .option('--with-ddl', 'Pulling BigQuery Resources with DDL SQL', false)
    .action(async (cmdBQIDs: string[] | undefined, _, cmd) => {
      const cmdOptions = cmd.optsWithGlobals();
      const BQIDs = cmdBQIDs && cmdBQIDs.length > 0 ? cmdBQIDs : ['@default'];

      const failed = await (async () => {
        return await pullBigQueryResources({
          BQIDs,
          BigQuery: buildThrottledBigQueryClient(20, 500),
          rootPath: cmdOptions.rootPath,
          withDDL: cmdOptions.withDdl,
          forceAll: cmdOptions.all,
          // concurrency: cmdOptions.concurrency,
          reporter: cmdOptions.format ?? 'console',
        });
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
          enableDataLineage: cmdOptions.enableDatalineage ?? false,
        };

        const sql = await createBundleSQL(ctx);
        console.log(sql);
      }
    });

  const importCommand = new Command('import')
    .description(
      'Import other dataset UDF into specified dataset',
    )
    .argument('<destination>')
    .argument('[targets...]')
    .action(
      async (destination: string, cmdTargets: string[] | undefined, _, cmd) => {
        const cmdOptions = cmd.optsWithGlobals();
        const rootDir = cmdOptions.rootPath;
        if (!rootDir) {
          console.error('CLI Error');
          return;
        }

        const bqClient = buildThrottledBigQueryClient(
          parseInt(cmdOptions.threads),
          500,
        );

        // Parse targets 'bqutils.fn.sure_nonnull' into [{'project': 'bqutils', dataset: 'fn', routine_id: 'sure_nonnull'}]
        const targets = cmdTargets?.map((target) => {
          const elms = target.split('.');
          if (elms.length !== 3) {
            throw new Error(`Invalid target: ${target}`);
          }
          return {
            project: elms[0] as string,
            dataset: elms[1] as string,
            routine_id: elms[2] as string,
          };
        }) ?? [];

        let paramDestination: { project: string; dataset: string };
        const [projectOrDataset, destinationDataset] = destination.split('.');
        if (destinationDataset) {
          paramDestination = {
            project: destinationDataset,
            dataset: destinationDataset,
          };
        } else if (projectOrDataset) {
          paramDestination = {
            project: '@default',
            dataset: projectOrDataset,
          };
        } else {
          throw new Error(`Invalid destination: ${destination}`);
        }

        const ctx = {
          bigQuery: bqClient,
          rootPath: rootDir,
          destination: paramDestination,
          importTargets: targets,
          options: {
            is_update: true,
          },
        };

        await importBigQueryResources(ctx);
      },
    );

  program.addCommand(pushCommand);
  program.addCommand(pullCommand);
  program.addCommand(formatCommmand);
  program.addCommand(bundleCommand);
  program.addCommand(importCommand);

  return program;
}
