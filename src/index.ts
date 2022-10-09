#!/usr/bin/env node

import readlinePromises from 'node:readline';
import { isatty } from 'node:tty';
import {
  BigQuery,
  Dataset,
  GetDatasetsOptions,
  Model,
  Query,
  Routine,
  Table,
} from '@google-cloud/bigquery';
import type { ServiceObject } from '@google-cloud/common';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { walk } from '../src/util.js';
import { formatLocalfiles } from '../src/commands/fix.js';
import { pushBigQueryResourecs } from '../src/commands/push.js';
import { syncMetadata } from '../src/metadata.js';
import {
  BigQueryResource,
  bq2path,
  buildThrottledBigQueryClient,
  path2bq,
} from '../src/bigquery.js';
import logUpdate from 'log-update';

import { Reporter, Task } from '../src/reporter.js';
import 'process';
import { Command } from 'commander';

const sqlDDLForSchemata = (projectId: string) => `
select
  'SCHEMA' as type
  , schema_name as name
  , ddl
from \`${projectId}.INFORMATION_SCHEMA.SCHEMATA\`
`;

//FIXME: BigQuery Runtime Error on many datasets
//FIXME: BigQuery Runtime Error on many tables in one dataset
//This sql can get only resources in default location.
const sqlDDLForProject = `
declare catalog string default @projectId;
declare schemas array<string> default @datasets;
if schemas is null then
  execute immediate format(
    "select as struct array_agg(distinct schema_name) as schemas from \`%s.INFORMATION_SCHEMA.SCHEMATA\`"
    , catalog
  ) into schemas;
end if;

execute immediate (
  select as value
    string_agg(template, "union distinct")
  from unnest(schemas) schema
  left join unnest([format("%s.%s", catalog, schema)]) identifier
  left join unnest([format(
"""-- SQL TEMPLATE
select
  'ROUTINE' as type
  , routine_name as name
  , ddl
from \`%s.INFORMATION_SCHEMA.ROUTINES\`
union distinct
select distinct
  'TABLE' as type
  , table_name as name
  , ddl
from \`%s.INFORMATION_SCHEMA.TABLES\`
"""
  , identifier, identifier
)]) template
)`;

export async function pullBigQueryResources({
  projectId,
  rootDir,
  withDDL,
  forceAll,
}: {
  projectId?: string;
  rootDir: string;
  withDDL?: boolean;
  forceAll?: boolean;
}) {
  type ResultBQResource = {
    type: string;
    path: string;
    name: string;
    ddl: string | undefined;
    resource_type: string;
  };
  const bqClient = buildThrottledBigQueryClient(20, 500);

  const defaultProjectId = await bqClient.getProjectId();

  let bqObj2DDL: any = {};
  const fsWriter = async (
    bqObj: Dataset | Model | Table | Routine,
  ) => {
    const fsPath = bq2path(bqObj as BigQueryResource, projectId === undefined);
    const pathDir = `${rootDir}/${fsPath}`;
    const catalogId = (
      bqObj.metadata?.datasetReference?.projectId ??
        (bqObj.parent as Dataset).metadata.datasetReference.projectId ??
        defaultProjectId
    ) as string;
    bqObj['projectId'] = catalogId;
    const retFiles: string[] = [];

    if (!fs.existsSync(pathDir)) {
      await fs.promises.mkdir(pathDir, { recursive: true });
    }
    const modified = await syncMetadata(bqObj, pathDir, { push: false })
      .catch((e) => {
        console.log('syncerror', e, bqObj);
        throw e;
      });

    retFiles.push(...modified.map((m) => path.basename(m)));

    if (bqObj instanceof Table) {
      retFiles.push('schema.json');
    }

    if (bqObj.metadata.type == 'VIEW') {
      let [metadata] = await bqObj.getMetadata();
      if (metadata?.view) {
        const pathView = `${pathDir}/view.sql`;
        await fs.promises.writeFile(
          pathView,
          metadata.view.query
            .replace(/\r\n/g, '\n'),
        );
      }
      retFiles.push('view.sql');
      // View don't capture ddl
      return retFiles;
    }

    if (!withDDL || !bqObj.id) {
      return retFiles;
    }

    const ddlStatement = bqObj2DDL[bqObj?.id ?? bqObj.metadata?.id]?.ddl;
    if (!ddlStatement) {
      return retFiles;
    }

    const pathDDL = `${pathDir}/ddl.sql`;
    const regexp = new RegExp(`\`${defaultProjectId}\`.`);
    const cleanedDDL = ddlStatement
      .replace(/\r\n/g, '\n')
      .replace('CREATE PROCEDURE', 'CREATE OR REPLACE PROCEDURE')
      .replace(
        'CREATE TABLE FUNCTION',
        'CREATE OR REPLACE TABLE FUNCTION',
      )
      .replace('CREATE FUNCTION', 'CREATE OR REPLACE FUNCTION')
      .replace(/CREATE TABLE/, 'CREATE TABLE IF NOT EXISTS')
      .replace(/CREATE MODEL/, 'CREATE MODEL IF NOT EXISTS')
      .replace(/CREATE SCHEMA/, 'CREATE SCHEMA IF NOT EXISTS')
      .replace(/CREATE VIEW/, 'CREATE OR REPLACE VIEW')
      .replace(
        /CREATE MATERIALIZED VIEW/,
        'CREATE MATERIALIZED VIEW IF NOT EXISTS ',
      )
      .replace(regexp, '');

    await fs.promises.writeFile(pathDDL, cleanedDDL);
    retFiles.push('ddl.sql');

    return retFiles;
  };

  const [datasets] = await bqClient
    .getDatasets({ projectId } as GetDatasetsOptions);

  const projectDir = `${rootDir}/${bq2path(bqClient, projectId === undefined)}`;
  if (!fs.existsSync(projectDir)) {
    await fs.promises.mkdir(projectDir, { recursive: true });
  }

  const fsDatasets = forceAll
    ? undefined
    : await fs.promises.readdir(projectDir);
  if (withDDL) {
    const ddlFetcher = async (
      sql: string,
      params: { [param: string]: any },
    ) => {
      // Import BigQuery dataset Metadata
      const [job] = await bqClient
        .createQueryJob({
          query: sql,
          params,
        })
        .catch((e) => {
          console.log(e.message);
          return [];
        });
      if (!job) {
        return undefined;
      }
      const [records] = await job.getQueryResults();
      return Object.fromEntries(
        records.map((r: ResultBQResource) => [r.name, r]),
      );
    };

    const schemaDDL = await ddlFetcher(
      sqlDDLForSchemata(projectId ?? defaultProjectId),
      {},
    );
    const resourceDDL = await ddlFetcher(sqlDDLForProject, {
      projectId: projectId ?? defaultProjectId,
      datasets: datasets
        .filter((d) => d.location == 'US')
        .map((d) => d?.id ?? d.metadata?.id),
    });

    bqObj2DDL = { ...schemaDDL, ...resourceDDL };
  }

  const reporter = new Reporter([]);
  const registerTask = (bqObj: Dataset | Table | Routine | Model) => {
    const parent = (bqObj.parent as ServiceObject);
    const projectId = bqObj.projectId ?? parent.projectId;
    const bqId = bqObj.metadata.id ?? `${projectId}:${parent.id}.${bqObj.id}`;
    const task = new Task(
      bqId.replace(/:|\./g, '/') + '/fetch metadata',
      async () => {
        const updated = await fsWriter(bqObj);
        return `Updated: ${updated.join(', ')}`;
      },
    );
    reporter.push(task);
    task.run();
  };

  const allowedDatasets = datasets
    .filter((d) => forceAll || (d.id && fsDatasets?.includes(d.id)));

  const task = new Task('# Check All Dataset and Resources', async () => {
    let cnt = 0;
    await Promise.allSettled(allowedDatasets
      .map(async (dataset: Dataset) => {
        registerTask(dataset);
        cnt++;
        return await Promise.allSettled([
          await dataset.getTables().then(([rets]) => {
            cnt += rets.length;
            rets.forEach(registerTask);
          }),
          await dataset.getRoutines().then(([rets]) => {
            cnt += rets.length;
            rets.forEach(registerTask);
          }),
          await dataset.getModels().then(([rets]) => {
            cnt += rets.length;
            rets.forEach(registerTask);
          }),
        ]);
      }));
    return `Total ${cnt}`;
  });

  reporter.push(task);
  task.run();

  logUpdate.done();
  for await (let report of reporter.show_until_finished()) {
    logUpdate(report);
  }
}

export async function pushLocalFilesToBigQuery(
  options: {
    rootDir: string;
    projectId?: string;
    concurrency?: number;
    dryRun?: boolean;
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
    const rl = readlinePromises.createInterface({
      input: process.stdin,
    });
    const buffer: string[] = [];
    for await (const line of rl) {
      buffer.push(line);
    }
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
        dryRun: cmdOptions.dryRun,
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

  const cleanCommand = new Command('clean')
    .description(
      'Clean up remote BigQuery resources whose local files are not found',
    )
    .argument('<project>')
    .argument('<dataset>')
    .option('--dry-run', 'dry run', false)
    .option(
      '--force',
      'Force to remove BigQuery resources without confirmation',
      false,
    )
    .action(async (_: string, dataset: string, localCmdOptions: any, cmd) => {
      const cmdOptions = cmd.optsWithGlobals();
      const options = {
        // rootDir: cmdOptions.rootPath,
        // forceAll: cmdOptions.force,
        dryRun: localCmdOptions.dryRun,
      };
      const bqClient = new BigQuery();
      await cleanupBigQueryDataset(
        bqClient,
        cmdOptions.rootPath ?? './bigquery/',
        dataset,
        options,
      );
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
  program.addCommand(cleanCommand);
  program.addCommand(formatCommmand);

  program.parse();
}

const cleanupBigQueryDataset = async (
  bqClient: BigQuery,
  rootDir: string,
  datasetId: string,
  options?: { dryRun?: string },
): Promise<void> => {
  const defaultProjectId = await bqClient.getProjectId();
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

  const datasetPath = path.join(rootDir, datasetId);
  if (!fs.existsSync(datasetPath)) {
    console.error(`Dataset ${datasetId} is not found in ${rootDir}`);
    return;
  }

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

  for (const kind of [tables, routines, models]) {
    for (const [bqId, resource] of kind) {
      console.log(`Deleting ${bqId}`);
      if (!options?.dryRun ?? true) {
        await resource.delete();
      }
    }
  }
};

const main = async () => {
  createCLI();
};

main();
