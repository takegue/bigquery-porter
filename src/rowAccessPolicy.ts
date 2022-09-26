import type { BigQuery } from '@google-cloud/bigquery';

type RowAccessPolicy = {
  rowAccessPolicyReference: {
    tableId: string;
    datasetId: string;
    projectId: string;
    policyId: string;
  };
  filterPredicate: string;
  creationTime: string;
  lastModifiedTime: string;
};

async function fetchRowAccessPolicy(
  client: BigQuery,
  datasetId: string,
  tableId: string,
  _projectId?: string,
): Promise<RowAccessPolicy[]> {
  let ret: RowAccessPolicy[][] = [];
  const projectId = _projectId ?? (await client.getProjectId());
  await new Promise((resolve, reject) => {
    client.dataset(datasetId, { projectId }).request({
      method: 'GET',
      uri: `/tables/${tableId}/rowAccessPolicies`,
    }, (err, resp) => {
      if (err) {
        reject(err);
        return;
      }

      if (resp.nextPageToken) {
        throw Error('Not implemented');
      }

      if (resp.rowAccessPolicies) {
        ret.push(resp.rowAccessPolicies);
      }
      resolve(ret);
    });
  });

  return ret.flat();
}

export { fetchRowAccessPolicy, RowAccessPolicy };
