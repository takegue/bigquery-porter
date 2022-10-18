import * as fs from 'node:fs';
import * as path from 'node:path';
import { ApiError } from '@google-cloud/common';
import type {
  BigQuery,
  Dataset,
  GetJobsOptions,
  Job,
  JobMetadata,
  // Model,
  Query,
  Routine,
  Table,
} from '@google-cloud/bigquery';

import type { Reporter } from '../../src/types.js';
import { DefaultReporter, JSONReporter } from '../../src/reporter/index.js';
import { BigQueryJobTask, BQJob } from '../../src/task.js';
import { syncMetadata } from '../../src/metadata.js';
import {
  extractDestinations,
  extractRefenrences,
  topologicalSort,
} from '../../src/util.js';

import {
  buildThrottledBigQueryClient,
  normalizedBQPath,
  path2bq,
} from '../../src/bigquery.js';

type JobConfig = {
  file: string;
  namespace: string;
  dependencies: string[];
  destinations: string[];
};

const buildBQJobFromMetadata = (job: JobMetadata): BQJob => {
  const ret: BQJob = {};
  const stats = job.statistics;
  if (job.jobReference?.jobId) {
    ret.jobID = job.jobReference?.jobId;
  }

  if (stats?.totalBytesProcessed !== undefined) {
    ret.totalBytesProcessed = parseInt(stats.totalBytesProcessed);
  }

  if (job.configuration?.dryRun !== undefined) {
    ret.isDryRun = job.configuration.dryRun;
  }

  if (stats?.totalSlotMs !== undefined) {
    ret.totalSlotMs = parseInt(stats.totalSlotMs);
  }

  const elapsedTimeMs =
    stats?.endTime !== undefined && stats?.startTime !== undefined
      ? parseInt(stats.endTime) - parseInt(stats.startTime)
      : undefined;

  if (elapsedTimeMs !== undefined) {
    ret['elapsedTimeMs'] = elapsedTimeMs;
  }
  return ret;
};

const fetchBQJobResource = async (
  job: Job,
): Promise<Dataset | Routine | Table | undefined> => {
  await job.getMetadata();
  if (job.metadata.status.errorResult) {
    throw new Error(job.metadata.status.errorResult.message);
  }

  if (!job.id) {
    throw new Error('Invalid Job ID');
  }
  switch (job.metadata.statistics.query.statementType) {
    case 'SCRIPT':
      const [childJobs] = await job.bigQuery.getJobs(
        { parentJobId: job.id } as GetJobsOptions,
      );
      for (const ix in childJobs) {
        const stat = childJobs[ix]?.metadata.statistics;
        try {
          if (stat.query?.ddlTargetRoutine) {
            const schema = job.bigQuery.dataset(
              stat.query.ddlTargetRoutine.datasetId,
            );
            const [routine] = await schema.routine(
              stat.query.ddlTargetRoutine.routineId,
            ).get();
            return routine;
          }
          if (stat.query?.ddlTargetTable) {
            const schema = job.bigQuery.dataset(
              stat.query.ddlTargetTable.datasetId,
            );
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
            throw new Error(`e.message ${JSON.stringify(stat.query)}`);
          }
        }
      }
      return undefined;
    case 'CREATE_SCHEMA':
    case 'DROP_SCHEMA':
    case 'ALTER_SCHEMA':
      const [dataset] = await job.bigQuery.dataset(
        job.metadata.statistics.query.ddlTargetDataset.datasetId,
      ).get();
      return dataset;
    case 'CREATE_ROW_ACCESS_POLICY':
    case 'DROP_ROW_ACCESS_POLICY':
      //TODO: row access policy
      throw new Error(
        `Not Supported: ROW_ACCES_POLICY ${job.metadata.statistics}`,
      );
    case 'CREATE_MODEL':
    case 'EXPORT_MODEL': {
      const schema = job.bigQuery.dataset(
        job.metadata.statistics.query.ddlTargetTable.datasetId,
      );
      const modelId = job.metadata.statistics.query.ddlTargetTable.tableId;
      if (!modelId) {
        throw new Error('Invalid modelId');
      }
      const [model] = await schema.model(modelId).get();
      return model;
    }
    case 'CREATE_FUNCTION':
    case 'CREATE_TABLE_FUNCTION':
    case 'DROP_FUNCTION':
    case 'CREATE_PROCEDURE':
    case 'DROP_PROCEDURE': {
      const schema = job.bigQuery.dataset(
        job.metadata.statistics.query.ddlTargetRoutine.datasetId,
      );
      const routineId =
        job.metadata.statistics.query.ddlTargetRoutine.routineId;
      if (!routineId) {
        throw new Error('Invalid routineId');
      }
      const [routine] = await schema.routine(routineId).get();
      return routine;
    }
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
    case 'DROP_MATERIALIZED_VIEW': {
      const tableId = job.metadata.statistics.query.ddlTargetTable.tableId;
      if (!tableId) {
        throw new Error('Invalid tableId');
      }
      const schema = job.bigQuery.dataset(
        job.metadata.statistics.query.ddlTargetTable.datasetId,
      );
      const [table] = await schema.table(tableId).get();
      return table;
    }
    default:
      const stats = job.metadata.statistics;
      throw new Error(
        `Not Supported: ${stats.query.statementType} (${job.id}, ${
          JSON.stringify(stats)
        })`,
      );
  }
};

export const deployBigQueryResouce = async (
  bqClient: BigQuery,
  rootPath: string,
  p: string,
  BigQueryJobOptions?: Query,
): Promise<BQJob> => {
  const msgWithPath = (msg: string) => `${path.dirname(p)}: ${msg}`;
  const defaultProjectId = await bqClient.getProjectId();

  if (p && !p.endsWith('sql')) {
    throw new Error(`Invalid file: ${p}`);
  }

  const [_, datasetId, name] = path2bq(p, rootPath, defaultProjectId)
    .split('.');
  const query = await fs.promises.readFile(p)
    .then((s: any) => s.toString())
    .catch((err: any) => {
      throw new Error(msgWithPath(err));
    });

  if (!datasetId) {
    throw new Error(`Invalid SchemaId: ${datasetId}`);
  }

  const jobPrefix = (() => {
    if (name) {
      return `bqporter-${datasetId}_${name}-`;
    }
    return `bqporter-${datasetId}-`;
  })();

  switch (path.basename(p)) {
    case 'view.sql':
      const schema = bqClient.dataset(datasetId);
      const tableId = name;
      if (!tableId) {
        throw new Error(`Invalid tableID: ${tableId}`);
      }
      if (BigQueryJobOptions?.dryRun) {
        const [_, ijob] = await bqClient.createQueryJob({
          ...BigQueryJobOptions,
          query:
            `CREATE OR REPLACE VIEW \`${schema.id}.${tableId}\` as\n${query}`,
          priority: 'BATCH',
          jobPrefix,
        });

        return buildBQJobFromMetadata(ijob);
      }

      const api = schema.table(tableId);
      const [isExist] = await api.exists();

      const [view] = await (
        isExist ? api.get() : schema.createTable(tableId, {
          view: query,
        })
      );
      await syncMetadata(view, path.dirname(p), { push: true });
      return {};

    default:
      // https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration
      const [job, ijob] = await bqClient.createQueryJob({
        ...BigQueryJobOptions,
        query,
        priority: 'BATCH',
        jobPrefix,
      });

      if (ijob.configuration?.dryRun) {
        return buildBQJobFromMetadata(ijob);
      }

      try {
        await job.promise();
      } catch (e: unknown) {
        if (e instanceof ApiError) {
          throw new Error(
            `${job.metadata.status.errorResult.message}`,
            // custom error options
            {
              cause: {
                ...job.metadata.status.errorResult,
                ...buildBQJobFromMetadata(ijob),
              },
            },
          );
        }
      }

      try {
        const resource = await fetchBQJobResource(job);

        if (resource !== undefined && resource.id == path.dirname(p)) {
          await syncMetadata(resource, path.dirname(p), { push: true });
        }
      } catch (e: unknown) {
        console.warn((e as Error).message);
      }

      return buildBQJobFromMetadata(job.metadata);
  }
};

const extractBigQueryDependencies = async (
  rootPath: string,
  fpath: string,
  bqClient: BigQuery,
) => {
  const defaultProjectId = await bqClient.getProjectId();
  const [projectID, schema, resource] = path2bq(
    fpath,
    rootPath,
    defaultProjectId,
  ).split('.');
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
  const bqID = path2bq(fpath, rootPath, defaultProjectId);
  const [projectID] = bqID.split('.');

  if (fpath.endsWith(`${path.sep}view.sql`)) {
    return [bqID];
  }

  const sql: string = await fs.promises.readFile(fpath, 'utf-8');
  const refs = [
    ...new Set(
      extractDestinations(sql)
        .map(([ref, type]) =>
          normalizedBQPath(ref, projectID, type == 'SCHEMA')
        ),
    ),
  ];

  return refs;
};

const buildDAG = (
  jobs: JobConfig[],
): [JobConfig[], WeakMap<JobConfig, JobConfig[]>] => {
  const job2deps = new WeakMap<JobConfig, JobConfig[]>();
  const file2job = Object.fromEntries(jobs.map((j) => [j.file, j]));

  const bq2files = new Map<string, { dsts: string[]; srcs: string[] }>();
  for (const { destinations, dependencies, file } of jobs) {
    for (const dst of destinations) {
      if (!bq2files.has(dst)) {
        bq2files.set(dst, { dsts: [], srcs: [] });
      }
      bq2files.get(dst)?.dsts.push(file);
    }
    for (const dep of dependencies) {
      if (!bq2files.has(dep)) {
        bq2files.set(dep, { dsts: [], srcs: [] });
      }
      bq2files.get(dep)?.srcs.push(file);
    }
  }

  // job => deps
  for (const { dsts, srcs } of bq2files.values()) {
    for (const dst of dsts) {
      const dstJob = file2job[dst];
      if (dstJob === undefined) {
        throw new Error(`Job not found: ${dst}`);
      }

      if (!job2deps.has(dstJob)) {
        job2deps.set(dstJob, []);
      }
    }

    for (const src of srcs) {
      const srcJob = file2job[src];
      if (srcJob === undefined) {
        throw new Error(`Job not found: ${src}`);
      }
      if (!job2deps.has(srcJob)) {
        job2deps.set(srcJob, []);
      }

      for (const dst of dsts) {
        const dstJob = file2job[dst];
        if (dstJob === undefined) {
          throw new Error(`Job not found: ${dst}`);
        }

        job2deps.get(srcJob)?.push(dstJob);
      }
    }
  }

  // ordered jobs
  const relations = new Set<string>();
  for (const { dsts, srcs } of bq2files.values()) {
    for (const dst of dsts) {
      relations.add(JSON.stringify([dst, '#root']));
      for (const src of srcs) {
        relations.add(JSON.stringify(['#leaf', dst]));
        relations.add(JSON.stringify([src, dst]));
      }
    }
  }

  const orderdJobs = topologicalSort(
    Array.from(relations.values())
      .map((n: string) => JSON.parse(n))
      .filter(([src, dst]) => src !== dst),
  )
    .filter((s) => !['#root', '#leaf'].includes(s))
    .map((s) => file2job[s])
    .filter((s): s is JobConfig => s !== undefined);

  return [orderdJobs, job2deps];
};

const buildTasks = (
  jobs: JobConfig[],
  jobDeps: WeakMap<JobConfig, JobConfig[]>,
  taskbuilder: (file: string) => Promise<BQJob>,
): BigQueryJobTask[] => {
  const tasks: BigQueryJobTask[] = [];
  const job2task = new WeakMap<JobConfig, BigQueryJobTask>();

  const inquiryTasks = (
    j: JobConfig,
  ) => (
    (jobDeps.get(j) ?? [])
      .map((t) => job2task.get(t))
      .filter((t): t is BigQueryJobTask => t !== undefined)
  );

  for (const job of jobs) {
    const task = new BigQueryJobTask(
      `${job.namespace.replace(/\./g, path.sep)}/${path.basename(job.file)}`,
      async () => {
        const deps: BigQueryJobTask[] = inquiryTasks(job);
        await Promise.all(
          deps.map((d) => d.runningPromise).flat(),
        ).catch(() => {
          const msg = deps
            .filter((t) => t && t.result().status == 'failed')
            .map((t) => t?.name).join(', ');
          throw Error('Suspended: Parent job is faild: ' + msg);
        });

        return await taskbuilder(job.file);
      },
    );

    tasks.push(task);
    job2task.set(job, task);
    task.run();
  }
  return tasks;
};

export async function pushBigQueryResourecs(
  rootPath: string,
  files: string[],
  concurrency: number,
  jobOption: Query,
  reporterType: 'console' | 'json',
) {
  const bqClient = buildThrottledBigQueryClient(concurrency, 500);
  const defaultProjectId = await bqClient.getProjectId();

  const targets: JobConfig[] = await Promise.all(
    files
      .map(async (n: string) => ({
        namespace: path2bq(n, rootPath, defaultProjectId),
        file: n,
        destinations: await extractBigQueryDestinations(
          rootPath,
          n,
          bqClient,
        ),
        dependencies: (await extractBigQueryDependencies(rootPath, n, bqClient))
          .filter((n) => n !== path2bq(n, rootPath, defaultProjectId)),
      })),
  );

  const [orderdJobs, jobDeps] = buildDAG(targets);

  // DAG Validation: All files should included
  for (const target of targets) {
    if (!jobDeps.has(target)) {
      console.warn(`Warning: No deployment files for ${target.file}`);
    }

    if (
      !target.destinations.includes(target.namespace) &&
      !target.dependencies.includes(target.namespace)
    ) {
      console.error(
        `Warning: Irrelevant SQL file located in ${target.file} for ${target.namespace}`,
      );
    }
  }

  const tasks = buildTasks(
    orderdJobs,
    jobDeps,
    (file: string) =>
      deployBigQueryResouce(bqClient, rootPath, file, jobOption),
  );

  let reporter: Reporter<BQJob> = new DefaultReporter();
  if (reporterType === 'json') {
    reporter = new JSONReporter<BQJob>();
  }

  try {
    reporter.onInit(tasks);
    tasks.forEach((t) => t.run());
    while (tasks.some((t) => !t.done())) {
      reporter.onUpdate();
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    reporter.onUpdate();
  } finally {
    reporter.onFinished();
  }
}
