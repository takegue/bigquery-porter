import { describe, it, expect, vi } from 'vitest';

import { BigQuery } from '@google-cloud/bigquery';
import { fetchRowAccessPolicy } from '../..//src/rowAccessPolicy.js';

const payload = [{
  rowAccessPolicyReference: {
    projectId: 'example_project',
    datasetId: 'sandbox',
    tableId: 'sample_table',
    policyId: 'sales_us_filter'
  },
  filterPredicate: 'a = 2',
  creationTime: '2022-09-19T23:38:49.575882Z',
  lastModifiedTime: '2022-09-19T23:38:49.575882Z'
}]

describe('fetchRowAccessPolicy', () => {
  it('fetchRowAccessPolicy should call request', async () => {
    const bqClient = new BigQuery()
    // mocking
    const mock = vi.fn().mockImplementation(bqClient.request)
    bqClient.request = mock
    mock.mockImplementationOnce((_, cb) => {
      cb(null, { nextPageToken: null, rowAccessPolicies: payload })
      return
    })

    const data = await fetchRowAccessPolicy(bqClient, 'sandbox', 'sample_table');
    expect(data).toEqual(payload);
    expect(mock).toHaveBeenCalledTimes(1)
  })
});
