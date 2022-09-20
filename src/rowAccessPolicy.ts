
import {
  BigQuery,
} from '@google-cloud/bigquery';


type RowAccessPolicy = {
  rowAccessPolicyReference: {
    tableId: string,
    datasetId: string,
    projectId: string,
    policyId: string,
  },
  filterPredicate: string,
  creationTime: string,
  lastModifiedTime: string,
}

async function fetchRowAccessPolicy(client: BigQuery, datasetId: string, tableId: string) {
  let ret: RowAccessPolicy[][] = [];
  await new Promise((resolve, reject) => {
    client.request({
      method: 'GET',
      uri: `/datasets/${datasetId}/tables/${tableId}/rowAccessPolicies`
    },
      (err, resp) => {
        if (err) {
          reject(err)
          return
        }
        let nextQuery = null;
        if (resp.nextPageToken) {
          nextQuery = Object.assign({}, {
            pageToken: resp.nextPageToken,
          });
        }

        ret.push(resp.rowAccessPolicies)
        resolve(ret)
      }
    )
  })
  return ret.flat()
}

export {
  RowAccessPolicy,
  fetchRowAccessPolicy,
}
