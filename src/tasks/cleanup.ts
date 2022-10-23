import * as fs from 'node:fs';
import * as path from 'node:path';
import readline from 'node:readline';

import type { BigQuery, Dataset } from '@google-cloud/bigquery';

import { BigQueryJobTask } from '../../src/tasks/base.js';

import { path2bq } from '../../src/bigquery.js';
import { walk } from '../../src/util.js';

type PushContext = {
  dryRun: boolean;
  force: boolean;
  rootPath: string;
  BigQuery: {
    projectId: string;
    client: BigQuery;
  };
  // reporter: BuiltInReporters;
};

const cleanupBigQueryDataset = async (
  bqClient: BigQuery,
  rootDir: string,
  projectId: string,
  datasetId: string,
  options?: {
    dryRun?: boolean;
    withoutConrimation: boolean;
  },
): Promise<BigQueryJobTask[]> => {
  const defaultProjectId = await bqClient.getProjectId();

  const datasetPath = path.join(rootDir, projectId, datasetId);
  if (!fs.existsSync(datasetPath)) {
    return [];
  }

  let dataset: Dataset;
  try {
    [dataset] = await bqClient.dataset(datasetId, { projectId })
      .get();
  } catch (e: unknown) {
    return [];
  }

  const [routines, models, tables] = await Promise.all([
    await dataset.getRoutines()
      .then(([rr]) =>
        new Map(
          rr.map((r) => [
            (({ metadata: { routineReference: r } }) =>
              `${r.projectId}.${r.datasetId}.${r.routineId}`)(r),
            r,
          ]),
        )
      ),
    await dataset.getModels()
      .then(([rr]) =>
        new Map(
          rr.map((r) => [
            (({ metadata: { modelReference: r } }) =>
              `${r.projectId}.${r.datasetId}.${r.modelId}`)(r),
            r,
          ]),
        )
      ),
    await dataset.getTables()
      .then(([rr]) =>
        new Map(
          rr.map((r) => [
            (({ metadata: { tableReference: r } }) =>
              `${r.projectId}.${r.datasetId}.${r.tableId}`)(r),
            r,
          ]),
        )
      ),
  ]);

  // Leave reousrces to delete
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

  const isDryRun = options?.dryRun ?? true;
  const isForce = options?.withoutConrimation ?? false;

  const tasks = [];
  const nsProject = projectId != '@default' ? projectId : defaultProjectId;
  for (const kind of [tables, routines, models]) {
    if (kind.size == 0) {
      continue;
    }

    if (!isForce && !isDryRun) {
      const ans = await prompt(
        [
          `Found BigQuery reousrces with no local files. Do you delete these resources? (y/n)`,
          `  ${[...kind.keys()].join('\n  ')}`,
          'Ans>',
        ].join('\n'),
      ) as string;
      if (!ans.replace(/^\s+|\s+$/g, '').startsWith('y')) {
        continue;
      }
    }

    for (const [bqId, resource] of kind) {
      const resourceType = resource.metadata.type;
      const task = new BigQueryJobTask(
        [nsProject, datasetId, resourceType, bqId.split('.').pop()].join('/'),
        async () => {
          try {
            console.error(`${isDryRun ? '(DRYRUN) ' : ''}Deleting ${bqId}`);
            if (!isDryRun) {
              await resource.delete();
            }
          } catch (e) {
          }
          return { isDryRun };
        },
      );
      tasks.push(task);
    }
  }

  return tasks;
};

// Use current tty for pipe input
const prompt = (query: string) =>
  new Promise(
    (resolve) => {
      const tty = fs.createReadStream('/dev/tty');
      const rl = readline.createInterface({
        input: tty,
        output: process.stderr,
      });

      rl.question(query, (ret) => {
        // Order matters and rl should close after use once
        tty.close();
        rl.close();
        resolve(ret);
      });
    },
  );

const createCleanupTasks = async (
  ctx: PushContext,
) => {
  let tasks = [];
  for (
    const dataset of await fs.promises.readdir(
      path.join(ctx.rootPath, ctx.BigQuery.projectId),
    )
  ) {
    let deleteTasks = await cleanupBigQueryDataset(
      ctx.BigQuery.client,
      ctx.rootPath,
      ctx.BigQuery.projectId,
      path.basename(dataset),
      {
        dryRun: ctx.dryRun,
        withoutConrimation: ctx.force,
      },
    ).catch((e) => {
      console.error(e);
    });

    tasks.push(...(deleteTasks ?? []));
  }

  return tasks;
};

export { cleanupBigQueryDataset, createCleanupTasks };
