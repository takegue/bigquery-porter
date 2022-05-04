// Imports the Google Cloud client library
import { BigQuery } from '@google-cloud/bigquery';
import * as fs from 'fs';
import * as path from 'path';
import {cac} from 'cac';

import pLimit from 'p-limit';
import { topologicalSort, walk } from './util.js';

const cli = cac();

const baseDirectory = './bigquery';

const predefinedLabels = {
  'bigquery-loader': 'bigquery_templating',
};
const jsonSerializer = (obj: any) => JSON.stringify(obj, null, 4);


type BigQueryJobResource = {
  file: string,
  bigquery: string,
  dependencies: string[]
}
function createCLI() {
  cli
    .command('push', '説明') // コマンド
    .option('--opt', '説明') // 引数オプション
    .action(async (options: any) => {
      // 実行したい処理
      console.log('push command', options); // 引数の値をオブジェクトで受け取れる
      await pushBigQueryResources();
    });

  cli
    .command('pull', '説明') // コマンド
    .option('--opt', '説明') // 引数オプション
    .action(async (options: any) => {
      // 実行したい処理
      pullBigQueryResources();
    });

  cli.help();
  cli.parse();
}

async function pullBigQueryResources() {
  const bqClient = new BigQuery();
  // Lists all datasets in the specified project
  bqClient.getDatasetsStream()
    .on('error', console.error)
    .on('data', async (dataset: any) => {
      const projectID = dataset.metadata.datasetReference.projectId;
      const datasetPath = `${baseDirectory}/${projectID}/${dataset.id}`;
      if (!fs.existsSync(datasetPath)) {
        // console.log(`Creating ${datasetPath}`);
        await fs.promises.mkdir(datasetPath, { recursive: true });
      }

      // TODO: INFORMATION_SCHEMA.TABLES quota error will occur
      await bqClient.createQueryJob(`
          select 
            routine_type as type
            , routine_catalog as catalog
            , routine_schema as schema
            , routine_name as name
            , ddl 
          from \`${dataset.id}.INFORMATION_SCHEMA.ROUTINES\`
          union all
          select 
            table_type as type
            , table_catalog as catalog
            , table_schema as schema
            , table_name as name
            , ddl 
          from \`${dataset.id}.INFORMATION_SCHEMA.TABLES\`
        `).then(async ([job, apiResponse]) => {
        await job.getQueryResults()
          .then(async (records) => {
            await Promise.all(
              records[0].map(async ({
                type,
                catalog,
                schema,
                name,
                ddl,
              }) => {
                const pathDir = `${baseDirectory}/${catalog}/${schema}/${name}`;
                const pathDDL = `${pathDir}/ddl.sql`;
                const cleanedDDL = ddl
                  .replace(/\r\n/g, '\n')
                  .replace('CREATE PROCEDURE', 'CREATE OR REPLACE PROCEDURE')
                  .replace(
                    'CREATE TABLE FUNCTION',
                    'CREATE OR REPLACE TABLE FUNCTION',
                  )
                  .replace('CREATE FUNCTION', 'CREATE OR REPLACE FUNCTION')
                  .replace(/CREATE TABLE/, 'CREATE TABLE IF NOT EXISTS')
                  .replace(/CREATE VIEW/, 'CREATE OR REPLACE VIEW')
                  .replace(
                    /CREATE MATERIALIZED VIEW/,
                    'CREATE OR REPLACE MATERIALIZED VIEW',
                  );

                if (!fs.existsSync(pathDir)) {
                  await fs.promises.mkdir(pathDir, { recursive: true });
                }
                if (
                  type in
                    { 'VIEW': true, 'TABLE': true, 'MATERIALIZED VIEW': true }
                ) {
                  const [table, _] = await dataset.table(name).get();
                  const pathSchema = `${pathDir}/schema.json`;
                  await fs.promises.writeFile(
                    pathSchema,
                    jsonSerializer(table.metadata.schema.fields),
                  );

                  if ('view' in table.metadata) {
                    const pathView = `${pathDir}/view.sql`;
                    await fs.promises.writeFile(
                      pathView,
                      table.metadata.view.query
                        .replace(/\r\n/g, '\n'),
                    );
                  }
                }
                await fs.promises.writeFile(pathDDL, cleanedDDL)
                  .then(
                    () =>
                      console.log(
                        `${type}: ${catalog}:${schema}.${name} => ${pathDDL}`,
                      ),
                  );
              }),
            );
          });
      })
        .catch((err) => {
          console.error(err);
        });
    })
    .on('end', () => {
    });
}

const deployBigQueryResouce = async (bqClient: any, rootDir: string, p: string) => {
  const msgWithPath = (msg: string) => `${path.dirname(p)}: ${msg}`;
  // const jsonSerializer = (obj) => JSON.stringify(obj, null, 4);
  const fsHandler = (err: Error) => {
    throw new Error(
      msgWithPath(err.message),
    );
  };
  const bigqueryHandler = (err: any) => {
    throw new Error(
      msgWithPath(err.errors.map((e: any) => e.message).join('\n')),
    );
  };

  if (p && !p.endsWith('sql')) return null;

  const [catalogId, schemaId, name] = path.dirname(path.relative(rootDir, p))
    .split('/');
  const query = await fs.promises.readFile(p)
    .then((s: any) => s.toString())
    .catch((err: any) => {
      throw new Error(msgWithPath(err));
    });

  const syncMetadata = async (table: any, dirPath: string) => {
    const metadataPath = path.join(dirPath, 'metadata.json');
    const fieldsPath = path.join(dirPath, 'schema.json');
    const [metadata] = await table.getMetadata();

    // schema.json: local file <---> BigQuery Table
    if (fs.existsSync(fieldsPath)) {
      const oldFields = await fs.promises.readFile(fieldsPath)
        .then(s => JSON.parse(s.toString()))
        .catch((err: Error) => console.error(err));
      // Update
      let updateCnt = 0;
      Object.entries(metadata.schema.fields).map(
        ([k, v]: [string, any]) => {
          if (k in oldFields) {
            if (
              metadata.schema.fields[k].description &&
              metadata.schema.fields[k].description != v.description
            ) {
              metadata.schema.fields[k].description = v.description;
              updateCnt++;
            }
          }
        },
      );
    }

    // metadata.json: local file <--- BigQuery Table
    const localMetadata = Object.fromEntries(
      Object.entries({
        type: metadata.type,
        routineType: metadata.routineType,
        modelType: metadata.modelType,
        description: metadata.description,
        // Filter predefined labels
      }).filter(([_, v]) => !!v && Object.keys(v).length > 0),
    );

    // Sync
    await Promise.all([
      fs.promises.writeFile(
        fieldsPath,
        jsonSerializer(metadata.schema.fields),
      ).catch(fsHandler),
      ,
      fs.promises.writeFile(
        metadataPath,
        jsonSerializer(localMetadata),
      ).catch(fsHandler),
      table.setMetadata(metadata)
        .catch(bigqueryHandler),
    ]);
  };

  switch (path.basename(p)) {
    case 'ddl.sql':
      // https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration
      const [job] = await bqClient.createQueryJob({
        query,
        priority: 'BATCH',
        labels: {
          key1: 'value1',
        },
      })
        .catch(bigqueryHandler);
      await job.getQueryResults();

      switch (job.metadata.statistics.query.statementType) {
        case 'CREATE_SCHEMA':
        case 'DROP_SCHEMA':
        case 'ALTER_SCHEMA':
          break;
        case 'CREATE_ROW_ACCESS_POLICY':
        case 'DROP_ROW_ACCESS_POLICY':
          //TODO: row access policy
          console.log(job.metadata.statistics);
          break;
        case 'CREATE_MODEL':
        case 'EXPORT_MODEL':
          //TODO: models
          console.log(job.metadata.statistics);
          break;
        case 'CREATE_FUNCTION':
        case 'CREATE_TABLE_FUNCTION':
        case 'DROP_FUNCTION':
        case 'CREATE_PROCEDURE':
        case 'DROP_PROCEDURE':
          //TODO: rountines
          const schema = bqClient.dataset(schemaId);
          const routineId = name;
          const [routine] = await schema.routine(routineId).get();
          const metadata = routine.metadata;
          const metadataPath = path.join(path.dirname(p), 'metadata.json');
          const localMetadata = Object.fromEntries(
            Object.entries({
              routineType: metadata.routineType,
              description: metadata.description,
              // Filter predefined labels
            }).filter(([_, v]) => !!v && Object.keys(v).length > 0),
          );
          await fs.promises.writeFile(
            metadataPath,
            jsonSerializer(localMetadata),
          );
          break;
        case 'CREATE_VIEW':
        case 'CREATE_AS_TABLE_SELECT':
        case 'DROP_TABLE':
        case 'DROP_VIEW':
        case 'ALTER_TABLE':
        case 'ALTER_VIEW':
        case 'INSERT':
        case 'UPDATE':
        case 'DELETE':
        case 'MERGE':
        case 'CREATE_MATERIALIZED_VIEW':
        case 'DROP_MATERIALIZED_VIEW':
          {
            const schema = bqClient.dataset(schemaId);
            const [table] = await schema.table(name).get();
            await syncMetadata(table, path.dirname(p));
          }
          break;
        case 'SCRIPT':
          //TODO: script
          const [childJobs, _] = await bqClient.getJobs({
            parentJobId: job.id,
          });
          const tgt = childJobs[0].metadata.statistics.query.ddlTargetRoutine;
          console.dir(tgt);
          break;
        default:
          console.dir(job.metadata);
      }
      break;
    case 'view.sql':
      const schema = bqClient.dataset(schemaId);
      const tableId = name;
      const api = schema.table(tableId);
      const [isExist] = await api.exists();

      const [view] = await (
        isExist ? api.get() : schema.createTable(tableId, {
          view: query,
        })
      );
      const [metadata] = await view.getMetadata();
      await syncMetadata(view, path.dirname(p));
      break;
  }
  return null;
};

async function pushBigQueryResources() {
  const bqClient = new BigQuery();
  const rootDir = path.normalize('./bigquery');

  // console.log(catalogId, schemaId, tableId, p, path.basename(p))
  const results = await Promise
    .allSettled(
      (await walk(rootDir)).map((p: string) =>
        deployBigQueryResouce(bqClient, rootDir, p)
      ),
    );

  console.dir(results, { depth: 3 });
}

const pathToBigQueryIdentifier = (fpath: string) => {
  const rootDir = path.normalize('./bigquery');
  const [catalogId, schemaId, name] = path.dirname(
    path.relative(rootDir, fpath),
  ).split('/');
  return [catalogId, schemaId, name].filter((n) => n).join('.');
};

const extractSQLIdentifier = async (fpath: string) => {
  const rootDir = path.normalize('./bigquery');
  const bqID = path.dirname(path.relative(rootDir, fpath)).replaceAll('/', '.')
  const [catalogId, schemaId, name] = path.dirname(
    path.relative(rootDir, fpath),
  ).split('/');
  const sql: string = await fs.promises.readFile(fpath)
    .then((s: any) => s.toString());

  // Exclude self-reference
  const denyList = [
    `${catalogId}.${schemaId}.${name}`,
    `${schemaId}.${name}`,
    `${schemaId}`,
  ];

  const normalizeBqPath = (bqPath: string): string => {
    const parts = bqPath.split('.');
    if(parts.length == 2){
      const [dst_schema, dst_name] = parts;
      const dst_project = catalogId;
      return `${dst_project}.${dst_schema}.${dst_name}`;
    } else if(parts.length == 1) {
      const [dst_schema] = parts;
      return `${catalogId}.${dst_schema}`;
    }
      else {
      const [dst_project, dst_schema, dst_name] = parts;
      return `${dst_project}.${dst_schema}.${dst_name}`;
    }
  }

  return Array.from(sql.matchAll(/`(?:[a-zA-Z-0-9_-]+\.?){1,3}`/g))
    .map(([matchedStr]) => matchedStr.replace(/^`|`$/g, ''))
    .map(normalizeBqPath)
    // Append Schema dependencies
    .concat(name !== null ? [`${catalogId}.${schemaId}`] : [])
    .filter((n: string) => !denyList.includes(n))
    .filter((n: string) => bqID !== n)
};

const buildDAG = async () => {
  const rootDir = path.normalize('./bigquery');
  const results = await Promise.all(
    (await walk(rootDir)).filter((p: string) => p.endsWith('sql')).map(async (n: string) => ({
      file: n,
      bigquery: pathToBigQueryIdentifier(n),
      dependencies: await extractSQLIdentifier(n),
    } as BigQueryJobResource)),
  );
  const relations = [...results
    .reduce((ret, { bigquery: tgt, dependencies: deps }) => {
      const [src_project, src_schema] = tgt.split('.');
      deps.forEach(
        (d: string) => {
          ret.add(
            JSON.stringify([tgt, d]),
          );
        },
      );
      return ret;
    }, new Set())
  ].map((n: any) => JSON.parse(n));

  const bigquery2Obj = Object.fromEntries(results.map((n) => [n.bigquery, n]));
  const DAG = topologicalSort([...relations])
    .filter(n => n in bigquery2Obj)
    .map((bq) => bigquery2Obj[bq]);

  const bqClient = new BigQuery();
  const limit = pLimit(5);

  const deployDAG: Map<string, {
    promise: any,
    bigquery: BigQueryJobResource
  }> = new Map(
    DAG.map(
      (target: BigQueryJobResource) => (
      [
        target.bigquery, 
        {
          promise: limit(async () => {
              await Promise.all(target.dependencies
                .map((d: string) => deployDAG.get(d)?.promise))
              await deployBigQueryResouce(bqClient, rootDir, target.file)
          }),
          bigquery: target
        }
      ]
    )
  ))

  return await Promise.all(
    Array.from(deployDAG.values())
    .map(({promise}) => promise)
  )
};

const main = async () => {
  await buildDAG()
};

main();
