import * as fs from 'node:fs';
import type { Metadata } from '@google-cloud/common';

import {
  BigQuery,
  BigQueryOptions,
  Dataset,
  Model,
  Routine,
  Table,
} from '@google-cloud/bigquery';

import pThrottle from 'p-throttle';
import * as path from 'node:path';
import {
  extractDestinations,
  extractRefenrences,
  StatementType,
} from '../src/util.js';

// import * as fs from 'node:fs';

interface BigQueryResource {
  id?: string;
  baseUrl?: string;
  metadata?: Metadata;
  projectId?: string;
  parent?: BigQueryResource;
}

function getProjectId(dataset: Dataset): string;
function getProjectId(model: Model): string;
function getProjectId(table: Table): string;
function getProjectId(routine: Routine): string;

function getProjectId(
  bqObj: Dataset | Table | Routine | Model,
): string {
  if (bqObj?.projectId) {
    return bqObj.projectId;
  }

  if (bqObj instanceof Model) {
    return bqObj.metadata.modelReference.projectId;
  }

  if (bqObj instanceof Table) {
    return bqObj.metadata.tableReference.projectId;
  }

  if (bqObj instanceof Routine) {
    return bqObj.metadata.routineReference.projectId;
  }

  if (bqObj instanceof Dataset) {
    return bqObj.metadata.datasetReference.projectId;
  }

  throw new Error(`Cannot find projectId ${bqObj}`);
}

function getFullResourceId(dataset: Dataset): string;
function getFullResourceId(model: Model): string;
function getFullResourceId(table: Table): string;
function getFullResourceId(routine: Routine): string;
function getFullResourceId(bqObj: Dataset | Table | Routine | Model): string {
  if (bqObj instanceof Model) {
    return `${bqObj.dataset.projectId}:${bqObj.dataset.id}.${bqObj.id}`;
  }

  if (bqObj instanceof Table) {
    return `${bqObj.dataset.projectId}:${bqObj.dataset.id}.${bqObj.id}`;
  }

  if (bqObj instanceof Routine) {
    const dataset = bqObj.parent as Dataset;
    return `${dataset.projectId}:${dataset.id}.${bqObj.id}`;
  }

  if (bqObj instanceof Dataset) {
    return `${bqObj.projectId}:${bqObj.id}`;
  }

  throw new Error(`Cannot find projectId ${bqObj}`);
}

const buildThrottledBigQueryClient = (
  concurrency: number,
  interval_limit: number,
  bigqueryOptions?: BigQueryOptions,
) => {
  const throttle = pThrottle({
    limit: concurrency,
    interval: interval_limit,
  });

  return new Proxy<BigQuery>(
    new BigQuery(bigqueryOptions),
    {
      get: (obj: BigQuery, sKey: string | symbol) => {
        const member = (obj as any)[sKey];
        // Request Throttling
        if (member instanceof Function && sKey == 'request') {
          return async (...args: any[]) => {
            let result: any;
            await throttle(async () => {
              result = member.apply(obj, args);
            })();
            return result;
          };
        }
        return member;
      },
    },
  );
};

const normalizeShardingTableId = (tableId: string) => {
  const regexTableSuffix = /\d+$/;
  const maybe_tableSuffix = tableId.match(regexTableSuffix);
  if (
    maybe_tableSuffix && maybe_tableSuffix[0] &&
    !isNaN(
      new Date(
        parseInt(maybe_tableSuffix[0].substring(0, 4)),
        parseInt(maybe_tableSuffix[0].substring(4, 2)),
        parseInt(maybe_tableSuffix[0].substring(6, 2)),
      )
        .getTime(),
    )
  ) {
    return tableId.replace(regexTableSuffix, '*');
  }
  return tableId;
};

const bq2path = (bqObj: BigQueryResource, asDefaultProject: boolean) => {
  let tree: string[] = [];
  let it: BigQueryResource = bqObj;
  let depth = 0;

  while (true) {
    depth += 1;
    if (it.id) {
      const shardName = normalizeShardingTableId(it.id).replace('*', '@');
      // Check BigQuery sharding table format
      if (depth == 1 && it.id != shardName) {
        tree.push(shardName);
      } else {
        // Ordinal id
        tree.push(it.id);
      }
    }

    if (!it.parent) {
      break;
    }
    it = it.parent;
  }

  if (asDefaultProject) {
    tree.push('@default');
  } else if (bqObj.metadata?.datasetReference?.projectId) {
    tree.push(bqObj.metadata?.datasetReference.projectId);
  } else if (bqObj.projectId) {
    tree.push(bqObj.projectId);
  } else if (bqObj.parent?.projectId) {
    tree.push(bqObj.parent.projectId);
  } else if (bqObj.parent?.parent?.projectId) {
    tree.push(bqObj.parent.parent.projectId);
  } else {
    throw new Error(`Cannot find projectId ${bqObj}`);
  }

  const ns = bqObj.baseUrl?.replace('/', '@');
  // @routines or @modeles
  if (ns && depth == 3 && !['/tables'].includes(bqObj.baseUrl as string)) {
    tree.splice(1, 0, ns);
  }
  return tree.reverse().join('/');
};

const path2bq = (
  fpath: string,
  rootPath: string,
  defaultProjectId: string,
) => {
  const rootDir = path.normalize(rootPath);
  const [catalogId, schemaId, namespace_or_name, name_or_missing] = path
    .dirname(
      path.relative(rootDir, fpath.replace('@default', defaultProjectId)),
    ).split('/');
  const name = (() => {
    const ordinalName = name_or_missing ?? namespace_or_name;
    if (ordinalName?.startsWith('@')) {
      return undefined;
    }
    if (!ordinalName?.endsWith('@')) {
      return ordinalName;
    }
    return ordinalName.replace(/@$/, '*');
  })();
  return [catalogId, schemaId?.startsWith('@') ? undefined : schemaId, name]
    .filter((n) => n).join('.');
};

const normalizedBQPath = (
  bqPath: string,
  defaultProject?: string,
  isDataset: boolean = false,
): string => {
  const cleanedPath = bqPath.replace(/`/g, '');
  const parts = cleanedPath.split('.');

  if (parts.length == 2) {
    if (isDataset) {
      return cleanedPath;
    }
    const [dst_schema, dst_name] = parts;
    const dst_project = defaultProject;
    return `${dst_project}.${dst_schema}.${dst_name}`;
  } else if (parts.length == 1) {
    // FIXME: Possible dataset, temporary table or CTE view name
    if (isDataset) {
      const [dst_schema] = parts;
      return `${defaultProject}.${dst_schema}`;
    }
    return cleanedPath;
  } else {
    const [dst_project, dst_schema, dst_name] = parts;
    return `${dst_project}.${dst_schema}.${dst_name}`;
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

  if (!await fs.promises.lstat(fpath).then((s) => s.isFile())) {
    return [path2bq(fpath, rootPath, defaultProjectId)];
  }

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
): Promise<[string, StatementType][]> => {
  const defaultProjectId = await bqClient.getProjectId();
  const bqID = path2bq(fpath, rootPath, defaultProjectId);
  const [projectID] = bqID.split('.');

  if (!await fs.promises.lstat(fpath).then((s) => s.isFile())) {
    return [];
  }

  if (fpath.endsWith(`${path.sep}view.sql`)) {
    return [[bqID, 'DDL_CREATE']];
  }

  const sql: string = await fs.promises.readFile(fpath, 'utf-8');
  const refs = [
    ...new Set(
      extractDestinations(sql)
        .filter(([_, type]) => !type.startsWith('TEMPORARY'))
        .map((
          [ref, type],
        ) =>
          JSON.stringify([
            normalizedBQPath(ref, projectID, type == 'SCHEMA'),
            type,
          ])
        ),
    ),
  ];

  return refs.map((r) => JSON.parse(r));
};

const constructDDLfromBigQueryObject = async (
  bqObj: Routine,
): Promise<string> => {
  const [metadata, _] = await bqObj.getMetadata();
  const id = getFullResourceId(bqObj).replace(':', '.');

  const _argumentsString = metadata.arguments
    ? metadata.arguments.map((arg: any) =>
      `${arg.name} ${arg.dataType ?? arg.argumentKind.replace('ANY_TYPE', 'ANY TYPE')
      }`
    )
      .join(
        ', ',
      )
    : '';

  return [
    `create or replace function \`${id}\`(${_argumentsString})`,
    metadata.language == 'js' ? `language ${metadata.language}` : '',
    metadata.returnType ? `return ${metadata.returnType}` : '',
    `as (${metadata.definitionBody})`,
  ].filter((s) => s).join('\n');
};

export {
  BigQueryResource,
  bq2path,
  buildThrottledBigQueryClient,
  constructDDLfromBigQueryObject,
  extractBigQueryDependencies,
  extractBigQueryDestinations,
  getFullResourceId,
  getProjectId,
  normalizedBQPath,
  normalizeShardingTableId,
  path2bq,
};
