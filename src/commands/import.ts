import type {
  BigQuery,
  // DatasetOptions,
  // GetDatasetsOptions,
  Routine,
} from '@google-cloud/bigquery';

import {
  BigQueryResource,
  bq2path,
  constructDDLfromBigQueryObject,
} from '../../src/bigquery.js';
import { pullMetadataTaskBuilder } from '../../src/commands/pull.js';
import { Task } from '../../src/tasks/base.js';
import { ReporterMap } from '../../src/reporter/index.js';
import * as fs from 'node:fs';
import * as path from 'node:path';

type ImportContext = {
  bigQuery: BigQuery;
  rootPath: string;
  destination: {
    project: string;
    dataset: string;
  };
  importTargets: {
    project: string;
    dataset: string;
    routine_id: string;
  }[];
  options: {
    is_update: boolean;
  };
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
  ctx: ImportContext,
  projectID: string,
): Promise<BQPPRojectID> => {
  if (projectID === '@default') {
    return {
      kind: 'special',
      value: '@default',
      resolved_value: await ctx.bigQuery.getProjectId(),
    };
  } else {
    return {
      kind: 'normal',
      value: projectID,
    };
  }
};

const importRoutine = async (
  ctx: ImportContext,
  client: BigQuery,
  destination: { project: string; dataset: string },
  routine: Routine,
) => {
  const [metadata] = await routine.getMetadata();

  let imported;
  try {
    const _imported = client.dataset(destination.dataset, {
      projectId: destination.project,
    }).routine(metadata.routineReference.routineId);
    await _imported.get();
    imported = _imported;
  } catch (e) {
    const [_imported, _] = await client.dataset(destination.dataset, {
      projectId: destination.project,
    })
      .createRoutine(
        metadata.routineReference.routineId,
        {
          arguments: metadata.arguments,
          definitionBody: metadata.definitionBody,
          description: metadata.description,
          determinismLevel: metadata.determinismLevel,
          language: metadata.language,
          returnType: metadata.returnType,
          routineType: metadata.routineType,
        },
      );
    imported = _imported;
  }

  const parsed: BQPPRojectID = await parseProjectID(
    ctx,
    ctx.destination.project,
  );
  const d = bq2path(
    imported as BigQueryResource,
    parsed.kind === 'special',
  );

  fs.mkdirSync(path.dirname(d), { recursive: true });
  const importPath = path.join(ctx.rootPath, d, '_imported.json');
  fs.promises.writeFile(
    importPath,
    JSON.stringify(routine.metadata.routineReference, null, 2),
  );
  return imported;
};

async function importBigQueryResources(
  ctx: ImportContext,
): Promise<number> {
  const tasks: Task[] = [];

  const genPullTask = pullMetadataTaskBuilder(
    {
      BQIDs: [],
      BigQuery: ctx.bigQuery,
      rootPath: ctx.rootPath,
      withDDL: true,
      forceAll: false,
      reporter: 'json',
    },
    async (bqId: string) => {
      const [datasetAndProject, routineId] = bqId.split('.');
      if (!datasetAndProject) {
        return undefined;
      }
      const [projectId, datasetId] = datasetAndProject.split(':');
      if (!projectId || !datasetId || !routineId) {
        return undefined;
      }

      const routine = ctx.bigQuery.dataset(datasetId, { projectId })
        .routine(routineId);

      return constructDDLfromBigQueryObject(routine);
    },
  );

  for (const target of ctx.importTargets) {
    const parsed: BQPPRojectID = await parseProjectID(
      ctx,
      ctx.destination.project,
    );
    const parsedDestination = {
      project: (parsed.kind == 'special' ? parsed.resolved_value : undefined) ??
        parsed.value,
      dataset: ctx.destination.dataset,
    };
    const task1 = new Task(
      `${ctx.destination.project}/${ctx.destination.dataset}/(import)/${target.project}.${target.dataset}.${target.routine_id}`,
      async () => {
        const importedRoutine: Routine = await importRoutine(
          ctx,
          ctx.bigQuery,
          parsedDestination,
          ctx.bigQuery
            .dataset(target.dataset, { projectId: target.project })
            .routine(target.routine_id),
        );

        const task = await genPullTask(importedRoutine);
        task.run();
        tasks.push(task);
        return 'success';
      },
    );
    tasks.push(task1);
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

export { importBigQueryResources };
