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
): Promise<RowAccessPolicy[]> {
  let ret: RowAccessPolicy[][] = [];
  await new Promise((resolve, reject) => {
    client.request({
      method: 'GET',
      uri: `/datasets/${datasetId}/tables/${tableId}/rowAccessPolicies`,
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
