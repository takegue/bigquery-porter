import type { Metadata } from '@google-cloud/common';
import { BigQuery } from '@google-cloud/bigquery';
import pThrottle from 'p-throttle';
import * as path from 'node:path';

interface BigQueryResource {
  id?: string;
  baseUrl?: string;
  metadata?: Metadata;
  projectId?: string;
  parent?: BigQueryResource;
}

const buildThrottledBigQueryClient = (concurrency: number, interval_limit: number) => {
  const throttle = pThrottle({
    limit: concurrency,
    interval: interval_limit,
  });

  return new Proxy<BigQuery>(
    new BigQuery(),
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
}

const bq2path = (bqObj: BigQueryResource, asDefaultProject: boolean) => {
  let tree: string[] = [];
  let it: BigQueryResource = bqObj;
  let depth = 0;

  while (true) {
    depth += 1;
    if (it.id) {
      tree.push(it.id);
    }

    if (!it.parent) {
      break;
    }
    it = it.parent;
  }

  if (asDefaultProject) {
    tree.push('@default');
  } else if (bqObj.projectId) {
    tree.push(bqObj.projectId);
  } else if (bqObj.parent?.projectId) {
    tree.push(bqObj.parent.projectId);
  }

  const ns = bqObj.baseUrl?.replace('/', '@');
  if (ns && depth == 3 && !['/tables'].includes(bqObj.baseUrl as string)) {
    tree.splice(1, 0, ns);
  }
  return tree.reverse().join('/');
};

const path2bq = (
  fpath: string,
  rootPath: string,
  defaultProjectId: string
) => {
  const rootDir = path.normalize(rootPath);
  const [catalogId, schemaId, namespace_or_name, name_or_missing] = path
    .dirname(
      path.relative(rootDir, fpath.replace('@default', defaultProjectId)),
    ).split('/');
  const name = name_or_missing ?? namespace_or_name;
  return [catalogId, schemaId, name].filter((n) => n).join('.');
};


const normalizedBQPath = (
  bqPath: string
  , defaultProject?: string
  , isDataset: boolean = false
): string => {
  const cleanedPath = bqPath.replace(/`/g, '');
  const parts = cleanedPath.split('.');

  if (parts.length == 2) {
    if (isDataset) {
      return cleanedPath
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
    return cleanedPath
  } else {
    const [dst_project, dst_schema, dst_name] = parts;
    return `${dst_project}.${dst_schema}.${dst_name}`;
  }
};

export {
  BigQueryResource,
  bq2path,
  path2bq,
  normalizedBQPath,
  buildThrottledBigQueryClient
};
