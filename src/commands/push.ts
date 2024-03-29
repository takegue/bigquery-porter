import * as fs from 'node:fs';
import * as path from 'node:path';

import { ApiError } from '@google-cloud/common';
import type {
  BigQuery,
  Dataset,
  DatasetOptions,
  GetJobsOptions,
  Job,
  JobMetadata,
  Model,
  Query,
  Routine,
  Table,
} from '@google-cloud/bigquery';

import type { Reporter } from '../../src/types.js';
import { BuiltInReporters, ReporterMap } from '../../src/reporter/index.js';
import { BigQueryJobTask, BQJob } from '../../src/tasks/base.js';
import { buildDAG, JobConfig } from '../../src/tasks/dag.js';
import { syncMetadata } from '../../src/metadata.js';
import { getTargetFiles } from '../../src/runtime/console.js';
import { walk } from '../../src/util.js';
import {
  extractBigQueryDependencies,
  extractBigQueryDestinations,
  normalizeShardingTableId,
  path2bq,
} from '../../src/bigquery.js';
import { SweepStrategy, createCleanupTasks } from '../../src/tasks/cleanup.js';

type PushContext = {
  dryRun: boolean;
  rootPath: string;
  enableDataLineage: boolean;
  BigQuery: {
    projectId: string;
    client: BigQuery;
  };
  SweepStrategy: SweepStrategy;
  reporter: BuiltInReporters;
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
      .filter((t) => t.shouldDeploy)
      .map((t) => job2task.get(t))
      .filter((t): t is BigQueryJobTask => t !== undefined)
  );

  for (const job of jobs) {
    const taskName = job.file.endsWith('.sql')
      ? path.basename(job.file)
      : '(Metadata)';
    const task = new BigQueryJobTask(
      `${job.namespace.replace(/\./g, path.sep)}/${taskName}`,
      async () => {
        const deps: BigQueryJobTask[] = inquiryTasks(job);
        await Promise.all(
          deps.map((d) => d.runningPromise).flat(),
        ).catch(() => {
          const msg = deps
            .filter((t) => t && t.result().status == 'failed')
            .map((t) => t?.name).join(', ');
          throw Error('Suspended: Parent job is failed: ' + msg);
        });

        return await taskbuilder(job.file);
      },
    );

    tasks.push(task);
    job2task.set(job, task);
  }
  return tasks;
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
): Promise<Dataset | Routine | Table | Model | undefined> => {
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
        [
          `Not Supported: ${stats.query.statementType}`,
          `(${job.id}, ${JSON.stringify(stats)})`,
        ].join(),
      );
  }
};

const deployBigQueryResouce = async (
  bqClient: BigQuery,
  rootPath: string,
  p: string,
  BigQueryJobOptions?: Query,
): Promise<BQJob> => {
  const msgWithPath = (msg: string) => `${path.dirname(p)}: ${msg}`;
  const executionProject = await bqClient.getProjectId();

  if (p && !p.endsWith('sql')) {
    throw new Error(`Invalid file: ${p}`);
  }

  const [project, datasetId, name] = path2bq(p, rootPath, executionProject)
    .split('.');
  const query = await fs.promises.readFile(p)
    .then((s: any) => s.toString())
    .catch((err: any) => {
      throw new Error(msgWithPath(err));
    });

  const jobPrefix = (() => {
    if (datasetId && name) {
      return `bqporter-${datasetId}_${name.replace('*', '')}-`;
    }

    if (datasetId) {
      return `bqporter-${datasetId}-`;
    }

    if (name) {
      return `bqporter-${name.replace('*', '')}-`;
    }

    return `bqporter-`;
  })();

  switch (path.basename(p)) {
    case 'view.sql':
      if (!datasetId) {
        throw new Error(`Invalid SchemaId: ${datasetId}`);
      }

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

        return { file: p, ...buildBQJobFromMetadata(ijob) };
      }

      const api = schema.table(tableId);
      const [isExist] = await api.exists();

      if (isExist) {
        // Retrieve existing view
        const [view] = await api.get();

        // Retrieve existing view metadata
        const [metadata] = await view.getMetadata();

        // Update view query
        metadata.view = query;
        await view.setMetadata(metadata);
      } else {
        await schema.createTable(tableId, {
          view: query,
        });
      }

      return { file: p };

    default:
      // https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration
      const modifiedQuery: string = (() => {
        if (project == executionProject) {
          return query;
        }
        return [
          `set @@dataset_project_id = "${project}";`,
          query,
        ].join('\n');
      })();
      const [job, ijob] = await bqClient.createQueryJob({
        ...BigQueryJobOptions,
        query: modifiedQuery,
        priority: 'BATCH',
        jobPrefix,
      });

      if (ijob.configuration?.dryRun) {
        return { file: p, ...buildBQJobFromMetadata(ijob) };
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
                file: p,
              },
            },
          );
        }
      }
      try {
        await fetchBQJobResource(job);
      } catch (e: unknown) {
        console.warn((e as Error).message);
      }

      return { file: p, ...buildBQJobFromMetadata(job.metadata) };
  }
};

const createDeployTasks = async (
  ctx: PushContext,
  files: string[],
  ctxFiles: string[],
  jobOption: Query,
) => {
  const defaultProjectID = await ctx.BigQuery.client.getProjectId();
  const toBQID = (p: string) => path2bq(p, ctx.rootPath, defaultProjectID);
  const toBQNS = (p: string) =>
    path.relative(
      ctx.rootPath,
      path.dirname(
        p
          .replace(/@default/, defaultProjectID)
          .replace(/@\w+/, (s) => s.toUpperCase()),
      ),
    ).replaceAll('/', '.').replace(/@$/, '*');

  const targets: JobConfig[] = [
    ...await Promise.all(
      Array.from(new Set(ctxFiles.concat(files)))
        .filter((p) => p.endsWith('.sql'))
        .map(async (n: string) => ({
          namespace: toBQNS(n),
          shouldDeploy: files.includes(n),
          file: path.normalize(n),
          destinations: await extractBigQueryDestinations(
            ctx.rootPath,
            n,
            ctx.BigQuery.client,
          ),
          dependencies: (await extractBigQueryDependencies(
            ctx.rootPath,
            n,
            ctx.BigQuery.client,
          ))
            .filter(async (n) => n !== toBQID(n)),
        })),
    ),
    // For Metadata Update
    ...(ctx.dryRun ? [] : Array.from(new Set(files.map((f) => f)))
      .filter((n) =>
        !toBQNS(n).includes('@') ||
        // Allow to fetch metadat for ROUTINES or MODELS
        toBQNS(n).includes('@ROUTINES') ||
        toBQNS(n).includes('@MODELS')
      )
      .map((n) => ({
        namespace: toBQNS(n),
        shouldDeploy: files.includes(n),
        file: path.normalize(path.join(path.dirname(n), 'metadata.json')),
        destinations: [],
        dependencies: [normalizeShardingTableId(toBQID(n)).replace(/@$/, '*')],
      }))),
  ];

  const [orderdJobs, jobDeps] = await buildDAG(targets, {
    enableDataLineage: ctx.enableDataLineage,
  });

  return buildTasks(
    orderdJobs.filter((t) => t.shouldDeploy),
    jobDeps,
    (file: string) => {
      // Deployment for SQL files
      if (file.endsWith('.sql')) {
        return deployBigQueryResouce(
          ctx.BigQuery.client,
          ctx.rootPath,
          file,
          jobOption,
        );
      }

      // Deployment for metadata files
      if (ctx.dryRun) {
        throw Error('Metadata update is not supported in dry-run mode');
      }

      const [projectId, dataset, tableOrRoutineOrModel] = toBQID(file).split(
        '.',
      );
      const fileDir = path.dirname(file);
      if (dataset === undefined) {
        throw Error('Unreachable code');
      }

      const ret: BQJob = { file };
      if (tableOrRoutineOrModel === undefined) {
        // Dataset
        return (async () => {
          const [model] = await ctx.BigQuery.client
            .dataset(dataset, { projectId } as DatasetOptions)
            .get();
          await syncMetadata(model, fileDir, { push: true });
          return ret;
        })();
      } else if (file.match('@routine')) {
        return (async () => {
          const [routine] = await ctx.BigQuery.client
            .dataset(dataset, { projectId } as DatasetOptions)
            .routine(tableOrRoutineOrModel)
            .get();
          await syncMetadata(routine, fileDir, { push: true });
          return ret;
        })();
      } else if (file.match('@model')) {
        return (async () => {
          const [model] = await ctx.BigQuery.client
            .dataset(dataset, { projectId } as DatasetOptions)
            .model(tableOrRoutineOrModel)
            .get();
          await syncMetadata(model, fileDir, { push: true });
          return ret;
        })();
      } else {
        // Table
        return (async () => {
          const [table] = await ctx.BigQuery.client
            .dataset(dataset, { projectId } as DatasetOptions)
            .table(tableOrRoutineOrModel)
            .get();
          await syncMetadata(table, fileDir, { push: true });
          return ret;
        })();
      }
    },
  );
};

async function pushLocalFilesToBigQuery(
  ctx: PushContext,
  jobOption: Query,
): Promise<number> {
  const targetProject = ctx.BigQuery.projectId ?? '@default';
  const findDir = path.join(ctx.rootPath, targetProject);
  const reporterType: BuiltInReporters = ctx.reporter;

  const predFilter = (p: string) =>
    (p.endsWith('.sql') || p.endsWith('metadata.json')) &&
    p.includes(targetProject);
  const ctxFiles = (await walk(findDir)).filter(predFilter);
  const inputFiles: string[] = ((await getTargetFiles()) ?? ctxFiles);

  const tasks: BigQueryJobTask[] = [
    // Deletion tasks
    ...await createCleanupTasks(ctx),
    // Deploy tasks
    ...await createDeployTasks(ctx, inputFiles, ctxFiles, jobOption),
  ];

  // Task Execution
  const reporter: Reporter<BQJob> = new ReporterMap[reporterType]();
  try {
    reporter.onInit(tasks);
    tasks.forEach((t) => t.run());
    while (tasks.some((t) => !t.done())) {
      reporter.onUpdate();
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    reporter.onUpdate();
  } catch (e) {
    console.error(e);
    return 1;
  } finally {
    reporter.onFinished();
  }

  const failedTasks =
    tasks.filter((t) => t.result().status !== 'success').length;
  return failedTasks;
}

export { buildDAG, pushLocalFilesToBigQuery, PushContext};
