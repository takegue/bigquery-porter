import * as fs from 'node:fs';
import * as path from 'node:path';
import type { BigQuery, Dataset } from '@google-cloud/bigquery';

import { BigQueryJobTask } from '../../src/tasks/base.js';

import { normalizeShardingTableId, path2bq } from '../../src/bigquery.js';
import { walk } from '../../src/util.js';
import { prompt } from '../../src/prompt.js';

type PushContext = {
  dryRun: boolean;
  rootPath: string;
  BigQuery: {
    projectId: string;
    client: BigQuery;
  };
  SweepStrategy: SweepStrategy;
  // reporter: BuiltInReporters;
};

type SweepStrategy = {
  mode: 'confirm' | 'force' | 'ignore' | 'rename_and_7d_expire';
  ignorePrefix: string;
};

const cleanupBigQueryDataset = async (
  bqClient: BigQuery,
  rootDir: string,
  projectId: string,
  datasetId: string,
  mode: SweepStrategy['mode'],
  options?: {
    dryRun?: boolean;
    withoutConrimation: boolean;
    ignorePrefix: string;
  },
): Promise<BigQueryJobTask[]> => {
  if(mode === 'ignore') {
    return [];
  }

  const defaultProjectId = await bqClient.getProjectId();
  const nsProject = projectId != '@default' ? projectId : defaultProjectId;

  const datasetPath = path.join(rootDir, projectId, datasetId);
  if (!fs.existsSync(datasetPath)) {
    return [];
  }

  let dataset: Dataset;
  try {
    [dataset] = await bqClient.dataset(datasetId, { projectId: nsProject })
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
            (({ metadata: { tableReference: r } }) => {
              return `${r.projectId}.${r.datasetId}.${r.tableId}`;
            })(r),
            r,
          ]),
        )
      ),
  ]);

  // Leave reousrces to delete
  (await walk(datasetPath))
    .filter((p: string) => p.includes(projectId))
    .forEach((f) => {
      const bqId = path2bq(f, rootDir, defaultProjectId);
      if (f.match(/@routine/) && routines.has(bqId)) {
        // Check Routine
        routines.delete(bqId);
      } else if (f.match(/@model/) && models.has(bqId)) {
        // Check Model
        models.delete(bqId);
      } else {
        const targetTables = Array.from(tables.keys()).filter(
          (k) => bqId === normalizeShardingTableId(k),
        );
        for (const table of targetTables) {
          tables.delete(table);
        }
      }
    });

  const isDryRun = options?.dryRun ?? true;
  const isForce = options?.withoutConrimation ?? false;

  const tasks = [];
  for (const kind of [tables, routines, models]) {
    if (kind.size == 0) {
      continue;
    }
    const resourceType = kind.values().next().value.constructor.name;

    if (!isForce && !isDryRun) {
      const ans = await prompt(
        [
          `Found BigQuery reousrces with no definition by local files. Do you delete these resources? (y/n)`,
          `[${resourceType}s]`,
          `  ${[...kind.keys()].join('\n  ')} `,
          'Ans>',
        ].join('\n'),
      ) as string;
      if (!ans.replace(/^\s+|\s+$/g, '').startsWith('y')) {
        continue;
      }
    }

    for (const [bqId, resource] of kind) {
      // Skip process prefixed by ignorePrefix
      const task: BigQueryJobTask = (() => {
        switch(mode) {
          case 'rename_and_7d_expire':
            return new BigQueryJobTask(
              [
                nsProject,
                datasetId,
                '(RENAME)',
                resourceType.toUpperCase(),
                bqId.split('.').pop(),
              ]
                .join('/'),
              async () => {
                if (options?.ignorePrefix && (bqId.split('.').pop() ?? 'undefined').startsWith(options.ignorePrefix)) {
                  return {isDryRun};
                }

                if(resourceType === 'Table') {
                  const dataset = (resource?.parent as Dataset);
                  const client: BigQuery = (dataset.parent as BigQuery);
                  const newTableName = `${(options?.ignorePrefix ?? 'zz_')}${resource.id}`
                  const tableRenameTo = `${dataset.projectId}.${dataset.id}.${newTableName}`;
                  const query = `
                    -- BigQuery Porter: Rename table
                    ALTER TABLE \`${bqId}\` RENAME TO \`${newTableName}\`;
                    ALTER TABLE \`${tableRenameTo}\` SET OPTIONS(
                      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
                    );
                  `
                  try {
                    console.error(`${isDryRun ? '(DRYRUN) ' : ''}  Renaming ${bqId} to ${tableRenameTo}`);
                    const [job] = await client.createQueryJob({
                      query,
                      jobPrefix: `bqport-metadata_rename-`,
                    })
                    if (!job) {
                      throw Error('Renamign Exception: Exception');
                    }
                    return { isDryRun }
                  } catch (e) {
                    console.error(e);
                  }

                }

                return { isDryRun };
              },
            );
          default:
            return new BigQueryJobTask(
              [
                nsProject,
                datasetId,
                '(DELETE)',
                resourceType.toUpperCase(),
                bqId.split('.').pop(),
              ]
              .join('/'),
              async () => {
                try {
                  console.error(`${isDryRun ? '(DRYRUN) ' : '🗑️'} Deleting ${bqId}`);
                  if (!isDryRun) {
                    // switch (mode) {
                    //   case 'rename': 
                    //     break;
                    //   default: 
                    await resource.delete();
                  }
                } catch (e) {
                }
                return { isDryRun };
              },
              );
          }
      })();

      tasks.push(task);
    }
  }

  return tasks;
};

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
      ctx.SweepStrategy.mode,
      {
        dryRun: ctx.dryRun,
        withoutConrimation: ctx.SweepStrategy.mode !== 'confirm',
        ignorePrefix: ctx.SweepStrategy.ignorePrefix,
      },
    ).catch((e) => {
      console.error(e);
    });

    tasks.push(...(deleteTasks ?? []));
  }

  return tasks;
};

export { cleanupBigQueryDataset, createCleanupTasks, SweepStrategy};
