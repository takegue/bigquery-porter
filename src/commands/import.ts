import type {
  BigQuery,
  // DatasetOptions,
  // GetDatasetsOptions,
  Routine,
} from '@google-cloud/bigquery';

import { pullMetadataTaskBuilder } from '../../src/commands/pull.js';
import { Task } from '../../src/tasks/base.js';
import { ReporterMap } from '../../src/reporter/index.js';

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
  client: BigQuery,
  destination: { project: string; dataset: string },
  routine: Routine,
) => {
  /*
   * This function will fetch routine from BigQuery and return it as a string.
   *
   * This procses is done by following steps:
   * 1. Fetch routine DDL from BigQuery and write a file to local.
   * 2. Replace routine dataset or project ids in routine with destination
   * 3. Deploy routines to destinatino
   * 4. Update metadata in local
   */
  const [metadata] = await routine.getMetadata();

  const [imported, _] = await client.dataset(destination.dataset, {
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

      const [metadata] = await ctx.bigQuery.dataset(datasetId, { projectId })
        .routine(routineId)
        .getMetadata();

      if (metadata.routineReference === undefined) {
        return undefined;
      }
      return [
        `create or replace function ${projectId}.${datasetId}.${routineId}` +
        `(${metadata.arguments.map((arg: any) =>
          `${arg.name} ${arg.dataType ?? arg.argumentKind.replace('ANY_TYPE', 'ANY TYPE')
          }`
        )
          .join(
            ', ',
          )
        })`,
        metadata.language == 'js' ? `language ${metadata.language}` : '',
        metadata.returnType ? `return ${metadata.returnType}` : '',
        `as (${metadata.definitionBody})`,
      ]
        .join('\n');
    },
  );

  const jobs = ctx.importTargets.map(async (target) => {
    const parsed: BQPPRojectID = await parseProjectID(
      ctx,
      ctx.destination.project,
    );
    const parsedDestination = {
      project: (parsed.kind == 'special' ? parsed.resolved_value : undefined) ??
        parsed.value,
      dataset: ctx.destination.dataset,
    };
    const importedRoutine: Routine = await importRoutine(
      ctx.bigQuery,
      parsedDestination,
      ctx.bigQuery
        .dataset(target.dataset, { projectId: target.project })
        .routine(target.routine_id),
    );
    const task = await genPullTask(importedRoutine);
    task.run();
    tasks.push(task);
    return 0;
  });
  await Promise.all(jobs);

  // const reporterType: BuiltInReporters = 'console';
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
