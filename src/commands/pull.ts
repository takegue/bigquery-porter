import type {
  BigQuery,
  Dataset,
  DatasetOptions,
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
  getProjectId,
  normalizeShardingTableId,
} from '../../src/bigquery.js';

import { BuiltInReporters, ReporterMap } from '../../src/reporter/index.js';
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

begin
  execute immediate format("""
  create or replace temporary table bqport_ddl_schema
    as select 'SCHEMA' as type
    , format('%%s:%%s', catalog_name, schema_name) as name
    , ddl
    from \`%s.INFORMATION_SCHEMA.SCHEMATA\`;
  """, catalog);
exception when error then
  create temporary table bqport_ddl_schema(type STRING, name STRING, ddl STRING);
end;

execute immediate (
  select as value
    ifnull("select * from bqport_ddl_schema union distinct " || string_agg(template, "union distinct"), "")
  from unnest(schemas) schema
  left join unnest([format("%s.%s", catalog, schema)]) as identifier
  left join unnest([struct(
    format(
      """-- SQL TEMPLATE
      select
        'ROUTINE' as type
        , format('%%s:%%s.%%s', routine_catalog, routine_schema, routine_name) as name
        , ddl
      from \`%s.INFORMATION_SCHEMA.ROUTINES\`
      union distinct
      select distinct
        'TABLE' as type
        , format('%%s:%%s.%%s', table_catalog, table_schema, table_name) as name
        , ddl
      from \`%s.INFORMATION_SCHEMA.TABLES\`
      """
      , identifier, identifier
    ) as template
  )])
)`;

type PullContext = {
  BQIDs: string[];
  forceAll: boolean;
  rootPath: string;
  BigQuery: BigQuery;
  withDDL: boolean;
  reporter: string;
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
type BQDatasetType = 'DEFAULT' | 'LINKED';

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
  datasets: Dataset[],
): {
  job: Promise<unknown>;
  reader: (bqId: string) => Promise<string | undefined>;
} => {
  const ddlFetcher = async (
    sql: string,
    params: { [param: string]: any },
    location?: string,
  ) => {
    // Import BigQuery dataset Metadata
    const [job] = await bqClient
      .createQueryJob({
        query: sql,
        params,
        jobPrefix: `bqport-metadata_import-`,
        location: location ?? 'US',
      })
      .catch((e) => {
        console.error(e.message);
        return [];
      });
    if (!job) {
      throw Error('buildDDLFetcher: Exception');
    }
    const [records] = await job.getQueryResults();
    return new Map(
      records.map((r: ResultBQResource) => [r.name, r]),
    );
  };

  const groupByLoctaion: Map<string, Dataset[]> = datasets.reduce(
    (acc, c) => {
      const location = c.metadata.location;
      if (!acc.has(location)) {
        acc.set(location, []);
      }
      acc.get(location).push(c);
      return acc;
    },
    new Map(),
  );

  const promise = Promise.allSettled(
    Array.from(groupByLoctaion.entries())
      .map(async ([location, datasets]: [string, Dataset[]]) => {
        return await ddlFetcher(sqlDDLForProject, {
          projectId: projectId,
          datasets: datasets.map((d) => d.id),
        }, location);
      }),
  );

  return {
    job: promise,
    reader: async (bqId: string) => {
      const payloads = await promise;
      const ddlMap = new Map<string, ResultBQResource>();
      for (const p of payloads) {
        if (p.status === 'fulfilled') {
          // Merge p.value into ddlMap
          for await (const [k, v] of p.value) {
            if (k) {
              ddlMap.set(k, v);
            }
          }
        }
      }
      return ddlMap.get(bqId)?.ddl;
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
    (await ctx.BigQuery.getProjectId()) == getProjectId(bqObj),
  );
  const pathDir = `${ctx.rootPath}/${fsPath}`;
  const retFiles: string[] = [];

  if (!fs.existsSync(pathDir)) {
    await fs.promises.mkdir(pathDir, { recursive: true });
  }
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

  if (!ctx.withDDL || !bqObj.metadata?.id || !bqObj.id || !ddlReader) {
    return retFiles;
  }

  const ddlStatement = await ddlReader(bqObj.metadata?.id ?? bqObj.id);
  if (!ddlStatement) {
    return retFiles;
  }

  const pathDDL = `${pathDir}/ddl.sql`;
  const regexp = new RegExp(`\`${await ctx.BigQuery.getProjectId()}\`.`);
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
  const buff: (Table | Model | Routine)[] = [];
  const cb = async (resource: Model | Routine) => {
    buff.push(resource);
  };

  try {
    // Metadata Update
    await dataset.get();
  } catch (e) {
    return;
  }

  const registeredShards = new Set<string>();
  const cb4table = async (table: Table) => {
    if (!table.id) {
      return;
    }
    const sharedName = normalizeShardingTableId(table.id);
    if (table.id != sharedName) {
      if (registeredShards.has(sharedName)) {
        return;
      }
      registeredShards.add(sharedName);
    }
    buff.push(table);
  };

  const promises = [
    new Promise(
      (resolve, reject) => {
        dataset.getTablesStream()
          .on('error', reject)
          .on('data', cb4table)
          .on('end', resolve);
      },
    ),
    new Promise((resolve, reject) => {
      dataset
        .getModelsStream()
        .on('error', reject)
        .on('data', cb)
        .on('end', resolve);
    }),
  ];

  // Analytics Hub dataset(type=LINKED) don't implement routines
  const datasetType = (dataset.metadata?.type ?? 'DEFAULT') as BQDatasetType;
  if (datasetType !== 'LINKED') {
    promises.push(
      new Promise(
        (resolve, reject) => {
          dataset.getRoutinesStream()
            .on('error', reject)
            .on('data', cb)
            .on('end', resolve);
        },
      ),
    );
  }
  const p = Promise.allSettled(promises);

  const pool = async function*() {
    while (true) {
      try {
        await Promise.race([
          new Promise((_, rj) => setTimeout(() => rj(), 200)),
          p,
        ]);
        break;
      } catch {
        yield true;
      }
    }
    for (const e of await p) {
      if (e.status === 'rejected') {
        console.error(e.reason);
      }
    }
    yield true;
  };

  for await (const _ of pool()) {
    while (buff.length > 0) {
      const r = buff.pop();
      if (r) {
        yield r;
      }
    }
  }
}

const pullMetadataTaskBuilder = (
  ctx: PullContext,
  ddlFetcher?: (bqId: string) => Promise<string | undefined>,
): ((bqObj: Dataset | Table | Routine | Model) => Promise<Task>) => {
  return async (bqObj) => {
    const parent = (bqObj.parent as ServiceObject);
    const projectId = bqObj?.projectId ?? parent?.projectId;
    const bqId = bq2path(
      bqObj as BigQueryResource,
      projectId === undefined ||
      projectId === await ctx.BigQuery.getProjectId(),
    );

    const task = new Task(
      bqId.replace(/:|\./g, '/') + '/fetch metadata',
      async () => {
        const [b] = await bqObj.get();
        const updated = await fsWriter(ctx, b, ddlFetcher);
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

      const datasets = await (async (): Promise<Dataset[]> => {
        if (fsDatasets && fsDatasets?.length > 0) {
          const datasets = fsDatasets.map(
            async (id) => {
              const dataset = ctx.BigQuery.dataset(id, opt as DatasetOptions);
              await dataset.get();
              return dataset;
            },
          );
          return await Promise.all(datasets);
        }

        const [datasets] = await ctx.BigQuery.getDatasets(
          opt as GetDatasetsOptions,
        );
        return datasets;
      })();

      let fetcher = undefined;
      if (ctx.withDDL) {
        const { job, reader } = buildDDLFetcher(
          ctx.BigQuery,
          bqProjectId,
          datasets,
        );
        cb(
          new Task(
            `# Check All Dataset and Resources/DDL/${bqProjectId}`,
            async () => {
              await job;
              return `Fetching DDL`;
            },
          ),
        );
        fetcher = reader;
      }
      const buildTask = pullMetadataTaskBuilder(ctx, fetcher);

      const p = Promise.allSettled(
        datasets
          .map(async (dataset: Dataset) => {
            cnt++;
            cb(await buildTask(dataset));
            for await (const bqObj of crawlBigQueryDataset(dataset)) {
              cnt++;
              cb(await buildTask(bqObj));
            }
          }),
      );
      for (const e of await p) {
        if (e.status === 'rejected') {
          console.error(e);
        }
      }
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
    if (d) {
      acc.get(p).add(d);
    }
    return acc;
  }, new Map());
};

async function pullBigQueryResources(
  ctx: PullContext,
): Promise<number> {
  // Grouping BQIDs by project
  const targets: Map<string, Set<string>> = groupByProject(ctx.BQIDs);

  const tasks: Task[] = [];
  const appendTask = (t: Task) => {
    t.run();
    tasks.push(t);
  };

  for (const [project, allowDatasets] of targets) {
    const p = await parseProjectID(ctx, project);
    await crawlBigQueryProject(ctx, p, [...allowDatasets], appendTask);
  }

  const reporterType: BuiltInReporters =
    (ctx.reporter ?? 'console') as BuiltInReporters;
  const reporter = new ReporterMap[reporterType]();
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
