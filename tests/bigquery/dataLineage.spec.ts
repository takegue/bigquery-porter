import { afterEach, describe, expect, it, vi } from 'vitest';

import { DataLineage } from '../../src/dataLineage.js';
import { ApiError } from '@google-cloud/common';

describe('dataLineageAPI', () => {
  afterEach(() => {
    vi.resetAllMocks();
  });

  it.skip('Captureing request payload for testing', async () => {
    const tgt = 'bigquery-public-data.austin_311.311_service_requests';
    const client = new DataLineage();
    const origin = client.request.bind(client);
    vi.spyOn(client, 'request')
      .mockImplementation((req, cb) => {
        expect(req).toMatchSnapshot();
        origin(req, (err: ApiError, resp: any) => {
          // Keeep empty argument to capture codes
          expect(req).toMatchInlineSnapshot();
          expect(err).toMatchInlineSnapshot();
          expect(resp).toMatchInlineSnapshot();
          return cb(err, resp);
        });
      });

    await client.getSearchLinks(tgt);
  });

  it('When dissabled dataLineageAPI', async () => {
    const client = new DataLineage();
    const apiDisabledMsg =
      'Data Lineage API has not been used in project <project-id> before or it is disabled. Enable it by visiting https://console.developers.google.com/apis/api/datalineage.googleapis.com/overview?project=<project-id> then retry. If you enabled this API recently, wait a few minutes for the action to propagate to our systems and retry.';
    let reqCaptured: Object;
    vi.spyOn(client, 'request')
      .mockImplementation((req, cb) => {
        reqCaptured = req;
        const err = new ApiError(apiDisabledMsg);
        cb(err, null);
      });

    await expect(async () => {
      await client.getSearchLinks('any_table_id');
    })
      .rejects
      .toThrowError(apiDisabledMsg);
    expect(reqCaptured).toMatchSnapshot();
  });

  it('project-id-7288898082930342315.sandbox.sample_clone_table', async () => {
    const tgt = 'project-id-7288898082930342315.sandbox.sample_clone_table';
    const client = new DataLineage();
    const idealResp = {
      'links': [
        {
          'endTime': '2022-12-21T23:40:46.050Z',
          'name':
            'projects/854542409859/locations/us/links/p:bc9d0fa982f9a77a12110ad76c22c458',
          'source': {
            'fullyQualifiedName':
              'bigquery:project-id-7288898082930342315.sandbox.sample_table',
          },
          'startTime': '2022-12-14T10:00:42.907Z',
          'target': {
            'fullyQualifiedName':
              'bigquery:project-id-7288898082930342315.sandbox.sample_clone_table',
          },
        },
      ],
    };

    let reqCaptured: Object;
    vi.spyOn(client, 'request')
      .mockImplementation((req, cb) => {
        reqCaptured = req;
        cb(null, idealResp);
      });

    const ret = await client.getSearchLinks(tgt);
    expect(reqCaptured).toMatchSnapshot();
    expect(ret).not.toBeNull();
  });

  it('getProcesss: project-id-7288898082930342315.sandbox.sample_clone_table', async () => {
    const tgt = 'project-id-7288898082930342315.sandbox.sample_clone_table';
    const client = new DataLineage();
    // let reqCaptured: Object;
    // vi.spyOn(client, 'request');
    // .mockImplementation((req, cb) => {
    //   reqCaptured = req;
    //   cb(null, idealResp);
    // });

    const ret = await client.getProcesses();
    // expect(reqCaptured).toMatchSnapshot();
    expect(ret).not.toBeNull();
  });
});
