#!/usr/bin/env node

// Imports the Google Cloud client library
import readlinePromises from 'node:readline';
import { isatty } from 'node:tty';
import {
  BigQuery,
  Dataset,
  GetDatasetsOptions,
  GetJobsOptions,
  Job,
  Model,
  Query,
  Routine,
  Table,
} from '@google-cloud/bigquery';
import type { ServiceObject } from '@google-cloud/common';
import { ApiError } from '@google-cloud/common';
import * as fs from 'node:fs';
import * as path from 'node:path';
import pLimit from 'p-limit';
import {
  extractRefenrences,
  extractDestinations,
  fixDestinationSQL,
  humanFileSize,
  msToTime,
  topologicalSort,
  walk,
} from '../src/util.js';
import {
  BigQueryResource,
  bq2path,
  path2bq,
  normalizedBQPath,
  buildThrottledBigQueryClient,
} from '../src/bigquery.js';
import logUpdate from 'log-update';

import { Reporter, Task } from '../src/reporter.js';
import 'process';
import { Command } from 'commander';

const jsonSerializer = (obj: any) => JSON.stringify(obj, null, 4);

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

type BigQueryJobResource = {
  file: string;
  namespace: string;
  dependencies: string[];
  destinations: string[];
};

// type Labels = Map<string, string>;
const syncMetadata = async (
  bqObject: Dataset | Table | Routine | Model,
  dirPath: string,
  options?: { versionhash?: string; push?: boolean },
) => {
  type systemDefinedLabels = {
    'bqport-versionhash': string;
  };

  const metadataPath = path.join(dirPath, 'metadata.json');
  const fieldsPath = path.join(dirPath, 'schema.json');
  const syncLabels: systemDefinedLabels = {
    'bqport-versionhash': `${Math.floor(Date.now() / 1000)}-${options?.versionhash ?? 'HEAD'
      }`,
  };
  const jobs: Promise<any>[] = [];

  const projectId = bqObject.metadata?.datasetReference?.projectId ??
    (bqObject.parent as ServiceObject).metadata?.datasetReference?.projectId;

  const apiQuery: { projectId: string; location?: string } = { projectId };
  if (bqObject instanceof Dataset && bqObject.location) {
    apiQuery.location = bqObject.location;
  }
  // const location =  ?  : (bqObject.parent as Dataset).location;
  const [metadata] = await bqObject.getMetadata(apiQuery);

  // schema.json: local file <---> BigQuery Table
  if (metadata?.schema?.fields) {
    // Merge upstream and downstream schema description
    // due to some bigquery operations like view or materialized view purge description
    if (fs.existsSync(fieldsPath)) {
      const oldFields = await fs.promises.readFile(fieldsPath)
        .then((s) => JSON.parse(s.toString()))
        .catch((err: Error) => console.error(err));
      // Update
      Object.entries(metadata.schema.fields).map(
        ([k, v]: [string, any]) => {
          if (
            k in oldFields &&
            metadata.schema.fields[k].description &&
            metadata.schema.fields[k].description != v.description
          ) {
            metadata.schema.fields[k].description = v.description;
          }
        },
      );
    }

    jobs.push(fs.promises.writeFile(
      fieldsPath,
      jsonSerializer(metadata.schema.fields),
    ));
  }

  const newMetadata = Object.fromEntries(
    Object.entries({
      type: metadata.type,
      // etag: metadata.etag,
      routineType: metadata.routineType,
      modelType: metadata.modelType,

      description: metadata.description,
      // Filter predefined labels
      labels: Object.fromEntries(
        Object.entries(metadata?.labels ?? [])
          .filter(([k]) => !(k in syncLabels)),
      ),
      // Dataset attribute
      access: metadata?.access,
      location: bqObject instanceof Dataset ? metadata?.location : undefined,

      // Routine Common atributes
      language: metadata?.language,
      arguments: metadata?.arguments,

      // SCALAR FUNCTION / PROCEDURE attribute
      returnType: metadata?.returnType,

      // TABLE FUNCTION attribute
      returnTableType: metadata?.returnTableType,

      // MODEL attribute
      featureColumns: metadata?.featureColumns,
      labelColumns: metadata?.labelColumns,
      trainingRuns: metadata?.trainingRuns,
    })
      // Remove invalid fields
      .filter(([_, v]) => !!v && Object.keys(v).length > 0),
  );

  if (fs.existsSync(metadataPath)) {
    const local = await fs.promises.readFile(metadataPath)
      .then((s) => JSON.parse(s.toString()));

    if (options?.push) {
      Object.entries(newMetadata).forEach(([attr]) => {
        newMetadata[attr] = local[attr];
      });
      jobs.push((bqObject as any).setMetadata(newMetadata));
    } else {
      Object.entries(newMetadata).forEach(([attr]) => {
        newMetadata[attr] = local[attr] ?? metadata[attr];
      });
    }
  }
  jobs.push(fs.promises.writeFile(metadataPath, jsonSerializer(newMetadata)));

  await Promise.all(jobs);
};

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
  const bqClient = buildThrottledBigQueryClient(20, 500)

  const defaultProjectId = await bqClient.getProjectId();

  let bqObj2DDL: any = {};
  const fsWriter = async (
    bqObj: Dataset | Model | Table | Routine,
  ) => {
    const path = bq2path(bqObj as BigQueryResource, projectId === undefined);
    const pathDir = `${rootDir}/${path}`;
    const catalogId = (
      bqObj.metadata?.datasetReference?.projectId ??
      (bqObj.parent as Dataset).metadata.datasetReference.projectId ??
      defaultProjectId
    ) as string;
    bqObj['projectId'] = catalogId;
    const retFiles = [];

    if (!fs.existsSync(pathDir)) {
      await fs.promises.mkdir(pathDir, { recursive: true });
    }
    await syncMetadata(bqObj, pathDir, { push: false })
      .catch((e) => {
        console.log('syncerror', e, bqObj);
        throw e;
      });
    retFiles.push(['metadata.json']);

    if (bqObj instanceof Table) {
      retFiles.push(['schema.json']);
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
      retFiles.push(['view.sql']);
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
    retFiles.push(['ddl.sql']);

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
  })

  reporter.push(task);
  task.run();

  logUpdate.done();
  for await (let report of reporter.show_until_finished()) {
    logUpdate(report);
  }
}

const deployBigQueryResouce = async (
  bqClient: BigQuery,
  rootPath: string,
  p: string,
  BigQueryJobOptions?: Query,
) => {
  const msgWithPath = (msg: string) => `${path.dirname(p)}: ${msg}`;
  const defaultProjectId = await bqClient.getProjectId();
  // const jsonSerializer = (obj) => JSON.stringify(obj, null, 4);

  if (p && !p.endsWith('sql')) return undefined;

  const [_, schemaId, name] = path2bq(p, rootPath, defaultProjectId).split('.');
  const query = await fs.promises.readFile(p)
    .then((s: any) => s.toString())
    .catch((err: any) => {
      throw new Error(msgWithPath(err));
    });

  if (!schemaId) {
    throw new Error('Invalid SchemaId');
  }

  const fetchBQJobResource = async (
    job: Job,
  ): Promise<Dataset | Routine | Table | undefined> => {
    await job.promise()
      .catch((e) => e);
    await job.getMetadata();
    if (job.metadata.status.errorResult) {
      throw new Error(job.metadata.status.errorResult.message);
    }

    if (!job.id) {
      throw new Error('Invalid SchemaId');
    }

    const schema = bqClient.dataset(schemaId);
    switch (job.metadata.statistics.query.statementType) {
      case 'SCRIPT':
        const [childJobs] = await bqClient.getJobs(
          { parentJobId: job.id } as GetJobsOptions,
        );
        for (const ix in childJobs) {
          const stat = childJobs[ix]?.metadata.statistics;
          try {
            if (stat.query?.ddlTargetRoutine) {
              const [routine] = await schema.routine(
                stat.query.ddlTargetRoutine.routineId,
              ).get();
              return routine;
            }
            if (stat.query?.ddlTargetTable) {
              const [table] = await schema.table(
                stat.query.ddlTargetTable.tableId,
              ).get();
              return table;
            }
          } catch (e: unknown) {
            // ignore error: Not Found Table or Routine
            if (e instanceof ApiError) {
              if (e.code === 404) {
                continue;
              }
              throw new Error(e.message)
            }
          }
        }
        return undefined;
      case 'CREATE_SCHEMA':
      case 'DROP_SCHEMA':
      case 'ALTER_SCHEMA':
        const [dataset] = await schema.get();
        return dataset;
      case 'CREATE_ROW_ACCESS_POLICY':
      case 'DROP_ROW_ACCESS_POLICY':
        //TODO: row access policy
        // console.log(job.metadata.statistics);
        break;
      case 'CREATE_MODEL':
      case 'EXPORT_MODEL':
        //TODO: models
        break;
      case 'CREATE_FUNCTION':
      case 'CREATE_TABLE_FUNCTION':
      case 'DROP_FUNCTION':
      case 'CREATE_PROCEDURE':
      case 'DROP_PROCEDURE':
        const routineId = name;
        if (!routineId) {
          throw new Error('Invalid routineId');
        }
        const [routine] = await schema.routine(routineId).get();
        return routine;
      case 'CREATE_TABLE':
      case 'CREATE_VIEW':
      case 'CREATE_TABLE_AS_SELECT':
      case 'DROP_TABLE':
      case 'DROP_VIEW':
      case 'ALTER_TABLE':
      case 'ALTER_VIEW':
      case 'INSERT':
      case 'UPDATE':
      case 'DELETE':
      case 'MERGE':
      case 'CREATE_MATERIALIZED_VIEW':
      case 'DROP_MATERIALIZED_VIEW':
        if (!name) {
          throw new Error('Invalid tableId');
        }
        const [table] = await schema.table(name).get();
        return table;

      default:
        console.info(`Not Supported: ${job} ${job.metadata.statistics.query.statementType}`)
        break;
    }
    return undefined;
  };

  switch (path.basename(p)) {
    case 'view.sql':
      const schema = bqClient.dataset(schemaId);
      const tableId = name;
      if (!tableId) {
        return;
      }
      if (BigQueryJobOptions?.dryRun) {
        const [_, ret] = await bqClient.createQueryJob({
          ...BigQueryJobOptions,
          query:
            `CREATE OR REPLACE VIEW \`${schema.id}.${tableId}\` as\n${query}`,
          priority: 'BATCH',
        });

        if (ret.statistics?.totalBytesProcessed !== undefined) {
          return humanFileSize(parseInt(ret.statistics.totalBytesProcessed));
        }
      }

      const api = schema.table(tableId);
      const [isExist] = await api.exists();

      const [view] = await (
        isExist ? api.get() : schema.createTable(tableId, {
          view: query,
        })
      );
      await syncMetadata(view, path.dirname(p), { push: true });
      break;
    default:
      // https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration
      const [job, ijob] = await bqClient.createQueryJob({
        ...BigQueryJobOptions,
        query,
        priority: 'BATCH',
      });

      if (
        ijob.configuration?.dryRun &&
        ijob.statistics?.totalBytesProcessed !== undefined
      ) {
        return humanFileSize(parseInt(ijob.statistics.totalBytesProcessed));
      }
      await fetchBQJobResource(job);

      if (job.metadata.statistics?.totalBytesProcessed !== undefined) {
        const stats = job.metadata?.statistics;
        const elpasedTime = stats.endTime !== undefined && stats.startTime !== undefined
          ? msToTime(parseInt(stats.endTime) - parseInt(stats.startTime))
          : undefined;

        const totalBytes = humanFileSize(parseInt(job.metadata.statistics?.totalBytesProcessed))

        return [totalBytes, elpasedTime].filter(s => s !== undefined).join(', ')
      }
      break;
  }
  return;
};

const extractBigQueryDependencies = async (
  rootPath: string,
  fpath: string,
  bqClient: BigQuery,
) => {
  const defaultProjectId = await bqClient.getProjectId();
  const [projectID, schema, resource] = path2bq(fpath, rootPath, defaultProjectId).split('.');
  const sql: string = await fs.promises.readFile(fpath)
    .then((s: any) => s.toString());

  const refs = [
    ...new Set(
      extractRefenrences(sql)
        .map((ref) => normalizedBQPath(ref, projectID)),
    ),
  ];
  const refs_schemas = [...new Set(refs)].map((n) => n.replace(/\.[^.]+$/, ''));

  // Add schema as explict dependencies without self
  const additionals =
    ((schema !== undefined && resource !== undefined)
      ? [normalizedBQPath(schema, projectID, true)]
      : []);
  return [...new Set(refs_schemas.concat(refs).concat(additionals))];
};

const extractBigQueryDestinations = async (
  rootPath: string,
  fpath: string,
  bqClient: BigQuery,
) => {
  const defaultProjectId = await bqClient.getProjectId();
  const [projectID] = path2bq(fpath, rootPath, defaultProjectId).split('.');
  const sql: string = await fs.promises.readFile(fpath)
    .then((s: any) => s.toString());

  const refs = [
    ...new Set(
      extractDestinations(sql)
        .map(([ref, type]) => normalizedBQPath(ref, projectID, type == 'SCHEMA')),
    ),
  ];
  return refs
}

const buildDAG = async (
  rootPath: string,
  files: string[],
  concurrency: number,
  jobOption: Query,
) => {
  const limit = pLimit(concurrency);
  const bqClient = new BigQuery();
  const defaultProjectId = await bqClient.getProjectId();

  const results = await Promise.all(
    files
      .map(async (n: string) => ({
        file: n,
        namespace: path2bq(n, rootPath, defaultProjectId),
        dependencies: (await extractBigQueryDependencies(rootPath, n, bqClient))
          .filter((n) => n !== path2bq(n, rootPath, defaultProjectId)),
        destinations: await extractBigQueryDestinations(rootPath, n, bqClient),
      } as BigQueryJobResource)),
  );

  const relations = [
    ...results
      .reduce((ret, { namespace: ns, dependencies: _deps, destinations: _dsts }) => {
        ret.add(JSON.stringify([ns, '#sentinal']));

        const dsts = new Set<string>(_dsts);
        _dsts
          .forEach(
            (dst: string) => {
              ret.add(JSON.stringify([dst, '#sentinal']));
              _deps
                //  Intra-file dependencies will ignore
                .filter((s) => !dsts.has(s))
                .forEach(
                  (src: string) => {
                    ret.add(JSON.stringify([dst, src]));
                  },
                );
            }
          )
        return ret;
      }, new Set())
  ]
    .map((n) => (typeof n === 'string') ? JSON.parse(n) : {})
    .filter(([src, dst]) => src !== dst);

  const bigquery2Objs = results.reduce(
    (ret, obj) => {
      ret.set(
        obj.namespace,
        //FIXME: Sort by sql kind (DDL > DML > QUERY)
        [...ret.get(obj.namespace) ?? [], obj]
      )
      return ret
    }
    , new Map<string, BigQueryJobResource[]>);

  const DAG: Map<string, {
    tasks: Task[];
    // bigquery: BigQueryJobResource;
  }> = new Map(
    topologicalSort(relations)
      .map((bq: string) =>
        [bq, (bigquery2Objs.get(bq) ?? [])] as [string, BigQueryJobResource[]]
      )
      .filter(([_, tasks]) => (tasks.length ?? 0) > 0)
      .map(
        ([ns, jobs]) => [
          ns,
          {
            // bigquery: ns,
            tasks: jobs.map(
              (job: BigQueryJobResource, ix) =>
                new Task(
                  path.relative(rootPath, job.file).replace(/@default/, defaultProjectId),
                  async () => {
                    await Promise.all(
                      job.dependencies
                        .map(
                          (d: string) => DAG.get(d)?.tasks.map(t => t.runningPromise),
                        )
                        .flat()
                        .concat(
                          // Intra-directory tasks
                          DAG.get(ns)?.tasks.slice(0, ix).map(t => t.runningPromise) ?? [],
                        )
                    ,
                    ).catch(() => {
                      const msg = job.dependencies
                        .map((t) => DAG.get(t)?.tasks)
                        .flat()
                        .filter((t) => t && t.status == 'failed')
                        .map((t) => t?.name).join(', ');
                      throw Error('Suspended: Parent job is faild: ' + msg);
                    });
                    return await deployBigQueryResouce(
                      bqClient,
                      rootPath,
                      job.file,
                      jobOption,
                    )
                  }),
            )
          },
        ],
      ),
  );

  // Validation: All files should included
  const namespaces = new Set(DAG.keys())
  for (const key of bigquery2Objs.keys()) {
    if (!namespaces.has(key)) {
      console.warn(`Warning: No deployment files: ${key}`)
    }
  }

  const tasks = [...DAG.values()]
    .map(({ tasks }) => {
      tasks.forEach(task => limit(async () => await task.run()));
      return tasks;
    }).flat();

  const reporter = new Reporter(tasks);
  logUpdate.done();
  for await (let report of reporter.show_until_finished()) {
    logUpdate(
      `Tasks: remaing ${limit.pendingCount + limit.activeCount}\n` +
      report,
    );
  }
};

export async function pushBigQueryResources(
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
    jobOption.labels = options.labels;
  }
  if (options.params) {
    jobOption.params = options.params;
  }

  await buildDAG(rootDir, files, options.concurrency ?? 1, jobOption);
}

function createCLI() {
  const program = new Command();

  program
    .description('Easy and Quick BigQuery Deployment Tool')
    // Global Options
    .option('-n, --threads <threads>', 'API Call Concurrency', '8')
    .option('-C, --root-path <rootPath>', 'Root Directory', './bigquery');

  const pushCommand = new Command('push')
    .description('Deploy your local BigQuery Resources in topological-sorted order')
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
      const cmdOptions = cmd.optsWithGlobals()
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

      await pushBigQueryResources(options);
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
      const cmdOptions = cmd.optsWithGlobals()
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
    })

  const cleanCommand = new Command('clean')
    .description('Clean up remote BigQuery resources whose local files are not found')
    .argument('<project>')
    .argument('<dataset>')
    .option('--dry-run', 'dry run', false)
    .option('--force', 'Force to remove BigQuery resources without confirmation', false)
    .action(async (project: string, dataset: string, cmdOptions: any) => {
      const options = {
        // rootDir: cmdOptions.rootPath,
        // forceAll: cmdOptions.force,
        dryRun: cmdOptions.dryRun,
      };
      console.log(project, dataset)
      const bqClient = new BigQuery();
      await cleanupBigQueryDataset(bqClient, cmdOptions.rootDir, dataset, options);
    });

  const formatCommmand = new Command('format')
    .description('Fix reference in local DDL files')
    .option('--dry-run', 'dry run', false)
    .action(async (_, cmd) => {
      const cmdOptions = cmd.optsWithGlobals()
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

const formatLocalfiles = async (
  rootPath: string,
  options?: { dryRun?: string },
) => {
  const files = (await walk(rootPath))
    .filter((p: string) => p.endsWith('ddl.sql'))
    .filter((p: string) => p.includes('@default'))
    .filter(async (p: string) => {
      const sql: string = await fs.promises.readFile(p)
        .then((s: any) => s.toString());
      const destinations = extractDestinations(sql)
      return destinations.length > 0
    });

  const bqClient = new BigQuery();
  const defaultProjectId = await bqClient.getProjectId();
  for (const file of files) {
    const sql: string = await fs.promises.readFile(file)
      .then((s: any) => s.toString());
    const ns = path2bq(file, rootPath, defaultProjectId).split('.').slice(1).join('.')
    const newSQL = fixDestinationSQL(ns, sql)

    if (newSQL !== sql) {
      console.log(file)
      if (options?.dryRun ?? false) {
      } else {
        await fs.promises.writeFile(file, newSQL)
      }
    }
  }
}

const cleanupBigQueryDataset = async (
  bqClient: BigQuery,
  rootDir: string,
  datasetId: string,
  options?: { dryRun?: string },
): Promise<void> => {
  const defaultProjectId = await bqClient.getProjectId();
  const routines = await bqClient.dataset(datasetId).getRoutines()
    .then(([rr]) => new Map(rr.map(r => [(({ metadata: { routineReference: r } }) => `${r.projectId}.${r.datasetId}.${r.routineId}`)(r), r])));
  const models = await bqClient.dataset(datasetId).getModels()
    .then(([rr]) => new Map(rr.map(r => [(({ metadata: { modelReference: r } }) => `${r.projectId}.${r.datasetId}.${r.modelId}`)(r), r])));
  const tables = await bqClient.dataset(datasetId).getTables()
    .then(([rr]) => new Map(rr.map(r => [(({ metadata: { tableReference: r } }) => `${r.projectId}.${r.datasetId}.${r.tableId}`)(r), r])));

  // Marks for deletion
  (await walk(rootDir))
    .filter((p: string) => p.endsWith('sql'))
    .filter((p: string) => p.includes('@default'))
    .forEach(f => {
      const bqId = path2bq(f, rootDir, defaultProjectId);
      if (f.match(/@routine/) && bqId in routines) {
        // Check Routine
        routines.delete(bqId);
      } else if (f.match(/@model/) && bqId in routines) {
        // Check Model
        models.delete(bqId);
      } else {
        if (bqId in tables) {
          // Check Table or Dataset
          tables.delete(bqId);
        }
      }
    })

  for (const kind of [tables, routines, models]) {
    for (const [bqId, resource] of kind) {
      console.log(`Deleting ${bqId}`);
      if (!options?.dryRun ?? true) {
        await resource.delete()
      }
    }
  }
}

const main = async () => {
  createCLI();
};

main();
