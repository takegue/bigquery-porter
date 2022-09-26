import * as path from 'node:path';
import * as fs from 'node:fs';

import type { ServiceObject } from '@google-cloud/common';
import { Dataset, Model, Routine, Table } from '@google-cloud/bigquery';

import { fetchRowAccessPolicy } from '../src/rowAccessPolicy.js';

const jsonSerializer = (obj: any) => JSON.stringify(obj, null, 2);

// type Labels = Map<string, string>;
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
    'bqport-versionhash': `${Math.floor(Date.now() / 1000)}-${
      options?.versionhash ?? 'HEAD'
    }`,
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
  if (metadata?.schema?.fields) {
    // Merge upstream and downstream schema description
    // due to some bigquery operations like view or materialized view purge description
    if (fs.existsSync(fieldsPath)) {
      const oldFields = await fs.promises.readFile(fieldsPath)
        .then((s) => JSON.parse(s.toString()))
        .catch((err: Error) => console.error(err));
      // Update
      Object.entries(metadata.schema.fields).map(
        ([k, v]: [string, any]) => {
          if (
            k in oldFields &&
            metadata.schema.fields[k].description &&
            metadata.schema.fields[k].description != v.description
          ) {
            metadata.schema.fields[k].description = v.description;
          }
        },
      );
    }

    jobs.push(fs.promises.writeFile(
      fieldsPath,
      jsonSerializer(metadata.schema.fields),
    ));
  }

  // README.md
  if (metadata.description !== undefined) {
    const upstream = metadata.description;
    newMetadata['description'] = upstream;
    if (fs.existsSync(readmePath)) {
      const local = await fs.promises.readFile(readmePath)
        .then((s) => s.toString());
      newMetadata['description'] = local;
      if (upstream !== undefined && local != upstream) {
        console.warn(
          'Warning: Local README.md file cannot be updated due to already exists. Please remove README.md file to update it.',
        );
      }
    }

    if (newMetadata['description'] !== undefined) {
      jobs.push(fs.promises.writeFile(readmePath, newMetadata['description']));
    }
  }

  if (options?.push) {
    jobs.push((bqObject as any).setMetadata(newMetadata));
  }

  // metadata.json
  const rowAccessPolicies = bqObject instanceof Table
    ? (await fetchRowAccessPolicy(
      bqObject.bigQuery,
      metadata?.tableReference.datasetId,
      metadata?.tableReference.tableId,
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
      ),
    );
  } else {
    jobs.push(
      fs.promises.writeFile(
        metadataPath,
        jsonSerializer(newMetadata),
      ),
    );
  }

  await Promise.all(jobs);
  return [];
};

export { syncMetadata };
