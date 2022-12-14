import type {
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

async function pullBigQueryResources({
  projectId,
  rootDir,
  withDDL,
  forceAll,
}: {
  projectId?: string;
  rootDir: string;
  withDDL?: boolean;
  forceAll?: boolean;
}): Promise<number> {
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

  const projectDir = `${rootDir}/${projectId ?? '@default'}`;
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
          jobPrefix: `bqport-metadata_import-`,
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

  const tasks: Task[] = [];
  const registeredShards = new Set<string>();
  const registerTask = async (bqObj: Dataset | Table | Routine | Model) => {
    if (!bqObj.id) {
      return;
    }

    const sharedName = normalizeShardingTableId(bqObj.id);
    if (bqObj.id != sharedName) {
      if (registeredShards.has(sharedName)) {
        return;
      }
      registeredShards.add(sharedName);

      [bqObj] = await (bqObj.parent as Dataset)
        .table(sharedName.replace('@', '*'))
        .get();
    }

    const parent = (bqObj.parent as ServiceObject);
    const projectId = bqObj?.projectId ?? parent?.projectId;
    const bqId = bq2path(bqObj as BigQueryResource, projectId === undefined);

    const task = new Task(
      bqId.replace(/:|\./g, '/') + '/fetch metadata',
      async () => {
        const updated = await fsWriter(bqObj);
        return `Updated: ${updated.join(', ')}`;
      },
    );

    tasks.push(task);
    task.run();
  };

  const allowedDatasets = datasets
    .filter((d) => forceAll || (d.id && fsDatasets?.includes(d.id)));

  const task = new Task(
    '# Check All Dataset and Resources',
    async () => {
      let cnt = 0;
      await Promise.allSettled(allowedDatasets
        .map(async (dataset: Dataset) => {
          cnt++;
          registerTask(dataset);
          dataset['projectId'] = projectId ?? defaultProjectId;
          return await Promise.allSettled([
            await dataset.getRoutines()
              .then(([rets]) => {
                cnt += rets.length;
                rets.forEach(registerTask);
              })
              .catch((e) => console.log(e)),
            ,
            await dataset.getModels()
              .then(([rets]) => {
                cnt += rets.length;
                rets.forEach(registerTask);
              })
              .catch((e) => console.log(e)),
            await (async () => {
              for await (
                const table of dataset.getTablesStream()
                  .on('error', console.error)
              ) {
                cnt += 1;
                registerTask(table);
              }
            })(),
          ]);
        }));
      return `Total ${cnt}`;
    },
  );

  tasks.push(task);
  task.run();

  const reporter = new ReporterMap['default']();
  try {
    reporter.onInit(tasks);
    tasks.forEach((t) => t.run());
    while (tasks.some((t) => !t.done())) {
      reporter.onUpdate();
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    reporter.onUpdate();
  } catch (e: unknown) {
    console.error(e);
  } finally {
    reporter.onFinished();
  }

  const failedTasks =
    tasks.filter((t) => t.result().status !== 'success').length;
  return failedTasks;
}

export { pullBigQueryResources };
