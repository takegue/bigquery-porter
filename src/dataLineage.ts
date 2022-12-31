import { Service } from '@google-cloud/common';

type Link = {
  name: string;
  source: {
    fullyQualifiedName: string;
  };
  target: {
    fullyQualifiedName: string;
  };
  endTime: string;
};

class DataLineage extends Service {
  location?: string;
  constructor(options = {}) {
    let apiEndpoint = 'https://datalineage.googleapis.com';
    options = Object.assign({}, options, {
      apiEndpoint,
    });
    const baseUrl = `${apiEndpoint}/v1`;
    const config = {
      apiEndpoint: apiEndpoint,
      baseUrl,
      scopes: [
        'https://www.googleapis.com/auth/cloud-platform',
      ],
      packageJson: { name: 'bigquery-porter', version: 'xxxx' },
    };
    super(config, options);
    this.location = 'us';
  }

  getOperations(): unknown {
    return new Promise((resolve, reject) => {
      this.request({
        method: 'GET',
        uri: `/locations/${this.location}/operations`,
        useQuerystring: true,
      }, (err, resp) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(resp);
      });
    });
  }

  getProcesses(): unknown {
    return new Promise((resolve, reject) => {
      this.request({
        method: 'GET',
        uri: `/locations/${this.location}/processes`,
        useQuerystring: true,
      }, (err, resp) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(resp);
      });
    });
  }

  getSearchLinks(
    tableId: string,
    target: 'source' | 'target',
  ): Promise<Link[]> {
    return new Promise((resolve, reject) => {
      this.request({
        method: 'POST',
        uri: `/locations/${this.location}:searchLinks`,
        useQuerystring: false,
        body: {
          [target]: {
            fullyQualifiedName: `bigquery:${tableId}`,
          },
        },
      }, (err, resp) => {
        if (err) {
          reject(err);
          return;
        }
        const links = resp['links'];
        return resolve(links);
      });
    });
  }

  /*
  getProcessRuns(): unknown {
    return new Promise((resolve, reject) => {
      this.request({
        method: 'GET',
        uri: `/locations/us/processes/d226f53d3a741b92ad54c996abf6cf82/runs`,
        useQuerystring: false,
      }, (err, resp) => {
        if (err) {
          reject(err);
        }
        resolve(resp);
      });
    });
  }

  getLineageEvents(): unknown {
    return new Promise((resolve, reject) => {
      this.request({
        method: 'GET',
        uri:
          `/locations/us/processes/d226f53d3a741b92ad54c996abf6cf82/runs/8d5b78c715a4db84c631881da5301035/lineageEvents`,
        useQuerystring: false,
      }, (err, resp) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(resp);
      });
    });
  }
  */
}

const getDyanmicLineage = async (
  bqIds: IterableIterator<string>,
  client?: DataLineage,
): Promise<Map<string, string[]>> => {
  client = client ?? new DataLineage();

  const requests: [string, Promise<Link[]>][] = [];
  const ret = new Map<string, string[]>();

  const fmt = (s: { fullyQualifiedName: string }): string => {
    const [_, id] = s.fullyQualifiedName.split(':');
    if (!id) {
      throw new Error(`Invalid fullyQualifiedName: ${s.fullyQualifiedName}`);
    }
    return id;
  };

  for (const bqId of bqIds) {
    requests.push([
      bqId,
      client.getSearchLinks(bqId, 'target'),
    ]);
  }
  try {
    await Promise.all(requests.map(([, p]) => p));
  } catch (e: unknown) {
    console.warn('WARNING: Failed to get lineage', e);
    return ret;
  }

  for (const [bqId, r] of requests) {
    const links = await r;
    if (!bqId || !links) {
      continue;
    }

    ret.set(bqId, links.map((l: Link) => fmt(l.source)));
  }
  return ret;
};

export { DataLineage, getDyanmicLineage, Link };
