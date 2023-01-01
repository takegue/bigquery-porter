import * as path from 'node:path';
import * as fs from 'node:fs';

import type { BigQuery } from '@google-cloud/bigquery';

import { getTargetFiles } from '../../src/runtime/console.js';
import { buildDAG } from '../../src/tasks/dag.js';
import { StatementType, walk } from '../../src/util.js';
import {
  extractBigQueryDependencies,
  extractBigQueryDestinations,
  path2bq,
} from '../../src/bigquery.js';

type JobConfig = {
  file: string;
  namespace: string;
  dependencies: string[];
  destinations: [string, StatementType][];
  shouldDeploy: boolean;
};

type BundleContext = {
  force: boolean;
  rootPath: string;
  enableDataLineage: boolean;
  BigQuery: {
    projectId: string;
    client: BigQuery;
  };
};

export const createBundleSQL = async (
  ctx: BundleContext,
) => {
  const rootDir = ctx.rootPath;

  const predFilter = (p: string) =>
    p.endsWith('.sql') && p.includes(ctx.BigQuery.projectId ?? '@default');
  const ctxFiles = (await walk(rootDir)).filter(predFilter);
  const inputFiles: string[] = (await getTargetFiles()) ?? ctxFiles;

  const defaultProjectID = await ctx.BigQuery.client.getProjectId();
  const toBQID = (p: string) => path2bq(p, ctx.rootPath, defaultProjectID);
  const targets: JobConfig[] = await Promise.all(
    Array.from(new Set(ctxFiles.concat(inputFiles)))
      .map(async (n: string) => ({
        namespace: toBQID(n),
        shouldDeploy: inputFiles.includes(n),
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
  );

  const [orderdJobs] = await buildDAG(targets, {
    enableDataLineage: ctx.enableDataLineage,
  });

  return orderdJobs
    .filter((t) => t.shouldDeploy)
    .map((j) => ({ job: j, sql: fs.readFileSync(j.file, 'utf-8') }))
    .map(({ job: j, sql }) =>
      [
        'begin',
        `-- BQPORTER: ${j.namespace} from ${j.file}`,
        sql.replace(/;\s*$/, ''),
        'exception when error then',
        'end',
      ].join('\n')
    )
    .join('\n\n');
};
