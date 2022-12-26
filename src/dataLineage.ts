import { Service } from '@google-cloud/common';

export class DataLineage extends Service {
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
      packageJson: require('../package.json'),
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

  getSearchLinks(): unknown {
    return new Promise((resolve, reject) => {
      this.request({
        method: 'POST',
        uri: `/locations/${this.location}:searchLinks`,
        useQuerystring: false,
        body: {
          target: {
            fullyQualifiedName:
              'bigquery:project-id-7288898082930342315.sandbox.sample_clone_table',
          },
        },
      }, (err, resp) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(resp);
      });
    });
  }

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
}
