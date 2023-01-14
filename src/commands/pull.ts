import type {
  BigQuery,
  Dataset,
  GetDatasetsOptions,
  Model,
  Routine,
  Table,
} from '@google-cloud/bigquery';
import type { ServiceObject } from '@google-cloud/common';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { syncMetadata } from '../../src/metadata.js';
import {
  BigQueryResource,
  bq2path,
  buildThrottledBigQueryClient,
  normalizeShardingTableId,
} from '../..//src/bigquery.js';

import { ReporterMap } from '../../src/reporter/index.js';
import { Task } from '../../src/tasks/base.js';
import 'process';

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
    ifnull("union distinct " || string_agg(template, "union distinct"), "")
  from unnest(schemas) schema
  left join unnest([format("%s.%s", catalog, schema)]) as identifier
  left join unnest([struct(
    format("""
      select
        'SCHEMA' as type
        , schema_name as name
        , ddl
      from \`%s.INFORMATION_SCHEMA.SCHEMATA\`
    """, catalog
    ) as sql_schema
    , format(
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
    ) as template
  )])
)`;

type PullContext = {
  forceAll: boolean;
  rootPath: string;
  BigQuery: BigQuery;
  defaultProjectId: string | undefined;
  withDDL: boolean;
};

type ResultBQResource = {
  type: string;
  path: string;
  name: string;
  ddl: string | undefined;
  resource_type: string;
};

type NormalProject = {
  kind: 'normal';
  value: string;
};
type SpecialProject = {
  kind: 'special';
  value: '@default';
  resolved_value: string;
};

type BQPPRojectID = NormalProject | SpecialProject;

const parseProjectID = async (
  ctx: PullContext,
  projectID: string,
): Promise<BQPPRojectID> => {
  if (projectID === '@default') {
    return {
      kind: 'special',
      value: '@default',
      resolved_value: await ctx.BigQuery.getProjectId(),
    };
  } else {
    return {
      kind: 'normal',
      value: projectID,
    };
  }
};

const buildDDLFetcher = (
  bqClient: BigQuery,
  projectId: string,
  datasets: string[],
): {
  job: Promise<unknown>;
  reader: (bqId: string) => Promise<string | undefined>;
} => {
  const ddlFetcher = async (
    sql: string,
    params: { [param: string]: any },
  ) => {
    // Import BigQuery dataset Metadata
    const [job] = await bqClient
      .createQueryJob({
        query: sql,
        params,
        jobPrefix: `bqport-metadata_import-`,
      })
      .catch((e) => {
        console.log(e.message);
        return [];
      });
    if (!job) {
      throw Error('buildDDLFetcher: Exception');
    }
    const [records] = await job.getQueryResults();
    return Object.fromEntries(
      records.map((r: ResultBQResource) => [r.name, r]),
    );
  };

  const promise = ddlFetcher(sqlDDLForProject, {
    projectId: projectId,
    datasets: datasets,
  });

  return {
    job: promise,
    reader: async (bqId: string) => {
      const ddlMap = await promise;
      return ddlMap?.[bqId]?.ddl;
    },
  };
};

const fsWriter = async (
  ctx: PullContext,
  bqObj: Dataset | Model | Table | Routine,
  ddlReader?: (bqId: string) => Promise<string | undefined>,
) => {
  const fsPath = bq2path(
    bqObj as BigQueryResource,
    ctx.defaultProjectId !== bqObj.projectId,
  );
  const pathDir = `${ctx.rootPath}/${fsPath}`;
  const retFiles: string[] = [];

  if (!fs.existsSync(pathDir)) {
    await fs.promises.mkdir(pathDir, { recursive: true });
  }
  console.error(pathDir);

  const modified = await syncMetadata(bqObj, pathDir, { push: false })
    .catch((e) => {
      console.error('syncerror', e, bqObj);
      throw e;
    });

  retFiles.push(...modified.map((m) => path.basename(m)));

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

  if (!ctx.withDDL || !bqObj.id || !ddlReader) {
    return retFiles;
  }

  const ddlStatement = await ddlReader(bqObj?.id ?? bqObj.metadata?.id);
  if (!ddlStatement) {
    return retFiles;
  }

  const pathDDL = `${pathDir}/ddl.sql`;
  const regexp = new RegExp(`\`${ctx.defaultProjectId}\`.`);
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

async function* crawlBigQueryDataset(
  dataset: Dataset,
): AsyncGenerator<Table | Model | Routine> {
  const [models] = await dataset.getModels();
  for (const model of models) {
    yield model;
  }

  const [routines] = await dataset.getRoutines();
  for (const routine of routines) {
    yield routine;
  }

  const registeredShards = new Set<string>();

  for await (
    const table of dataset.getTablesStream()
      .on('error', console.error)
  ) {
    const sharedName = normalizeShardingTableId(table.id);
    if (table.id != sharedName) {
      if (registeredShards.has(sharedName)) {
        continue;
      }
      registeredShards.add(sharedName);
    }
    yield table;
  }
}

const pullMetadataTaskBuilder = (
  ctx: PullContext,
  ddlFetcher?: (bqId: string) => Promise<string | undefined>,
): ((bqObj: Dataset | Table | Routine | Model) => Promise<Task>) => {
  return async (bqObj) => {
    const parent = (bqObj.parent as ServiceObject);
    const projectId = bqObj?.projectId ?? parent?.projectId;
    const bqId = bq2path(bqObj as BigQueryResource, projectId === undefined);

    const task = new Task(
      bqId.replace(/:|\./g, '/') + '/fetch metadata',
      async () => {
        const updated = await fsWriter(ctx, bqObj, ddlFetcher);
        return `Updated: ${updated.join(', ')}`;
      },
    );
    task.run();
    return task;
  };
};

async function crawlBigQueryProject(
  ctx: PullContext,
  project: BQPPRojectID,
  allowDatasets: string[],
  cb: (t: Task) => void,
) {
  const bqProjectId = project.kind == 'special'
    ? project.resolved_value
    : project.value;
  const projectDir = `${ctx.rootPath}/${project.value}`;
  if (!fs.existsSync(projectDir)) {
    await fs.promises.mkdir(projectDir, { recursive: true });
  }

  const fsDatasets = ctx.forceAll
    ? undefined
    : (allowDatasets.length > 0
      ? allowDatasets
      : await fs.promises.readdir(projectDir));

  const crawlTask = new Task(
    '# Check All Dataset and Resources',
    async () => {
      let cnt = 0;

      const opt = project.kind === 'special'
        ? {}
        : { projectId: project.value };
      const [datasets] = await ctx.BigQuery.getDatasets(
        opt as GetDatasetsOptions,
      );
      const actualDatasets = datasets
        .filter((d) =>
          ctx.forceAll || !fsDatasets || (d.id && fsDatasets?.includes(d.id))
        );

      let fetcher = undefined;
      if (ctx.withDDL) {
        const { job, reader } = buildDDLFetcher(
          ctx.BigQuery,
          bqProjectId,
          actualDatasets
            .map((d) => d.id)
            .filter((d): d is string => d != undefined),
        );
        cb(
          new Task('# Check All Dataset and Resources/DDL', async () => {
            await job;
            return `DDL Job Done`;
          }),
        );
        fetcher = reader;
      }
      const buildTask = pullMetadataTaskBuilder(ctx, fetcher);

      const p = Promise.allSettled(
        actualDatasets
          .map(async (dataset: Dataset) => {
            cnt++;
            // dataset['projectId'] = projectId ?? ctx.defaultProjectId;
            for await (const bqObj of crawlBigQueryDataset(dataset)) {
              cnt++;
              cb(await buildTask(bqObj));
            }
          }),
      );
      await p;
      return `Total ${cnt} resources`;
    },
  );
  cb(crawlTask);
}

const groupByProject = (BQIDs: string[]): Map<string, Set<string>> => {
  return BQIDs.reduce((acc, c) => {
    const elms = c.split('.');
    // Allow Dataset or Project
    if (elms.length > 2) {
      throw Error(`Invalid BQID: ${c}`);
    }
    const [p, d] = elms;
    if (!acc.has(p)) {
      acc.set(p, new Set());
    }
    acc.get(p).add(d);
    return acc;
  }, new Map());
};

async function pullBigQueryResources({
  BQIDs,
  projectId,
  rootDir,
  withDDL,
  forceAll,
}: {
  BQIDs: string[];
  projectId?: string;
  rootDir: string;
  withDDL?: boolean;
  forceAll?: boolean;
}): Promise<number> {
  const bqClient = buildThrottledBigQueryClient(20, 500);
  const ctx: PullContext = {
    defaultProjectId: projectId ?? '@default',
    rootPath: rootDir,
    withDDL: withDDL ?? false,
    forceAll: forceAll ?? false,
    BigQuery: bqClient,
  };

  const projectDir = `${rootDir}/${projectId ?? '@default'}`;
  if (!fs.existsSync(projectDir)) {
    await fs.promises.mkdir(projectDir, { recursive: true });
  }

  // Grouping BQIDs by project
  const targets: Map<string, Set<string>> = groupByProject(BQIDs);

  const tasks: Task[] = [];
  const appendTask = (t: Task) => {
    t.run();
    tasks.push(t);
  };

  for (const [project, allowDatasets] of targets) {
    const p = await parseProjectID(ctx, project);
    await crawlBigQueryProject(ctx, p, [...allowDatasets], appendTask);
  }

  const reporter = new ReporterMap['json']();
  try {
    reporter.onInit(tasks);
    tasks.forEach((t) => t.run());
    while (tasks.some((t) => !t.done())) {
      reporter.onUpdate();
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    reporter.onUpdate();
  } catch (e: unknown) {
  } finally {
    reporter.onFinished();
  }

  const failedTasks =
    tasks.filter((t) => t.result().status !== 'success').length;
  return failedTasks;
}

export { pullBigQueryResources };
