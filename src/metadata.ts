import * as path from 'node:path';
import * as fs from 'node:fs';

import type { ServiceObject } from '@google-cloud/common';
import { Dataset, Model, Routine, Table } from '@google-cloud/bigquery';
import type { TableField, TableSchema } from '@google-cloud/bigquery';

import { fetchRowAccessPolicy } from '../src/rowAccessPolicy.js';

const jsonSerializer = (obj: any) => JSON.stringify(obj, null, 2);
const cleanupObject = (obj: Object) => JSON.parse(JSON.stringify(obj));

const mergeSchema = async (
  localFieldPath: string,
  upstream: TableSchema | undefined,
  overwrite: boolean,
): Promise<TableSchema> => {
  if (!upstream) {
    throw new Error('No upstream schema found');
  }

  if (!fs.existsSync(localFieldPath)) {
    return upstream;
  }

  const localFields = await fs.promises.readFile(localFieldPath, 'utf-8')
    .then((s) => JSON.parse(s.toString()))
    .then((M) => Object.fromEntries(M.map((e: TableField) => [e['name'], e])));

  if (localFields === undefined) {
    return upstream;
  }

  if (!upstream.fields) {
    throw new Error('upstream fields is not found');
  }

  return ({
    fields: upstream.fields.map(
      (entry: TableField) => {
        const merged = entry;
        if (!merged.name) {
          return merged;
        }

        if (merged.name in localFields) {
          if (overwrite) {
            merged.description = localFields[merged.name].description;
          } else {
            // Merge upstream and downstream schema description
            // due to some bigquery operations like view or materialized view purge description
            merged.description = (entry as TableField).description ??
              localFields[merged.name].description;
          }
        }
        return merged;
      },
    ),
  }) as TableSchema;
};

const mergeDescription = async (
  localREADMEPath: string,
  upstream: String | undefined,
  overwrite: boolean,
): Promise<String | undefined> => {
  if (!fs.existsSync(localREADMEPath)) {
    return upstream;
  }

  if (!overwrite) {
    return upstream;
  }

  const local = await fs.promises.readFile(localREADMEPath, 'utf-8');
  return local;
};

const syncMetadata = async (
  bqObject: Dataset | Table | Routine | Model,
  dirPath: string,
  options?: { versionhash?: string; push?: boolean },
): Promise<string[]> => {
  type systemDefinedLabels = {
    'bqport-versionhash': string;
  };

  const metadataPath = path.join(dirPath, 'metadata.json');
  const readmePath = path.join(dirPath, 'README.md');
  const fieldsPath = path.join(dirPath, 'schema.json');
  const syncLabels: systemDefinedLabels = {
    'bqport-versionhash': `${options?.versionhash ?? 'HEAD'}`,
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

  const newMetadata = Object.fromEntries(
    Object.entries({
      type: metadata.type,
      // etag: metadata.etag,
      routineType: metadata.routineType,
      modelType: metadata.modelType,

      // Filter predefined labels
      labels: Object.fromEntries(
        Object.entries(metadata?.labels ?? [])
          .filter(([k]) => !(k in syncLabels)),
      ),
      // Table attribute
      timePartitioning: metadata?.timePartitioning,
      clustering: metadata?.clustering,

      // Snapshot attribute
      snapshotDefinition: metadata?.snapshotDefinition,

      // cloneDefinition attribute
      cloneDefinition: metadata?.snapshotDefinition,

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

  /* -----------------------------
  / Merge local and remote metadata
  */

  // metadata.json: local file <---> BigQuery Table
  if (fs.existsSync(metadataPath)) {
    const local = await fs.promises.readFile(metadataPath)
      .then((s) => JSON.parse(s.toString()));

    if (options?.push) {
      // TODO: Add allowlist check to write
      Object.entries(newMetadata).forEach(([attr]) => {
        newMetadata[attr] = local[attr];
      });
    } else {
      Object.entries(newMetadata).forEach(([attr]) => {
        newMetadata[attr] = local[attr] ?? metadata[attr];
      });
    }
  }

  // schema.json: local file <---> BigQuery Table
  let newSchema = await mergeSchema(
    fieldsPath,
    metadata.schema,
    options?.push ?? false,
  ).catch((e) => {
    console.warn(`Warning: ${e.message}`);
  });
  if (newSchema) {
    jobs.push(
      fs.promises.writeFile(
        fieldsPath,
        jsonSerializer(newSchema.fields),
      ).then(() => fieldsPath),
    );
  }

  let newDescription = await mergeDescription(
    readmePath,
    metadata.description,
    options?.push ?? false,
  );
  if (newDescription) {
    jobs.push(
      fs.promises.writeFile(readmePath, newDescription)
        .then(() => readmePath),
    );
  }

  if (options?.push) {
    jobs.push(
      (bqObject as any)
        .setMetadata(
          cleanupObject({
            ...newMetadata,
            ...{ description: newDescription },
            ...{ schema: newSchema },
          }),
        )
        .then(() => undefined)
        .catch((e: Error) => {
          console.warn('Warning: Failed to update metadata.' + e.message);
        }),
    );
  }

  // metadata.json
  const rowAccessPolicies = bqObject instanceof Table
    ? (await fetchRowAccessPolicy(
      bqObject.bigQuery,
      metadata?.tableReference.datasetId,
      metadata?.tableReference.tableId,
      metadata?.tableReference.projectId,
    ).catch((err: Error) => {
      console.warn(err.message);
      return [];
    }))
    : [];

  if (rowAccessPolicies.length > 0) {
    const entryRowAccessPolicies = rowAccessPolicies.map((p) => ({
      policyId: p.rowAccessPolicyReference.policyId,
      filterPredicate: p.filterPredicate,
    }));
    jobs.push(
      fs.promises.writeFile(
        metadataPath,
        jsonSerializer({
          ...newMetadata,
          rowAccessPolicies: entryRowAccessPolicies,
        }),
      ).then(() => metadataPath),
    );
  } else {
    jobs.push(
      fs.promises.writeFile(
        metadataPath,
        jsonSerializer(newMetadata),
      ).then(() => metadataPath),
    );
  }

  return (await Promise.all(jobs)).filter((n) => n);
};

export { syncMetadata };
