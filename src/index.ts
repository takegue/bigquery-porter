// Imports the Google Cloud client library
import logUpdate from 'log-update';
import { BigQuery } from '@google-cloud/bigquery';
import type { Metadata } from '@google-cloud/common';
import * as fs from 'fs';
import * as path from 'path';
import {cac} from 'cac';
import pLimit from 'p-limit';
import {
  topologicalSort,
  walk,
  extractRefenrences,
} from '../src/util.js';

import {
  Task,
} from '../src/reporter.js';


const cli = cac();

const baseDirectory = './bigquery';
const jsonSerializer = (obj: any) => JSON.stringify(obj, null, 4);

type BigQueryJobResource = {
  file: string,
  bigquery: string,
  dependencies: string[]
}

// type ErrorHanlder = (err: Error) => void

const syncMetadata = async (bqObject: any, dirPath: string) => {
  const metadataPath = path.join(dirPath, 'metadata.json');
  const fieldsPath = path.join(dirPath, 'schema.json');
  const accessPath = path.join(dirPath, 'access.json');
  const jobs: Promise<any>[] = []

  const [metadata] = await bqObject.getMetadata();

  // schema.json: local file <---> BigQuery Table
  if(metadata?.schema?.fields) {
    if (fs.existsSync(fieldsPath)) {
      const oldFields = await fs.promises.readFile(fieldsPath)
        .then(s => JSON.parse(s.toString()))
        .catch((err: Error) => console.error(err));
      // Update
      Object.entries(metadata.schema.fields).map(
        ([k, v]: [string, any]) => {
          if (k in oldFields) {
            if (
              metadata.schema.fields[k].description &&
              metadata.schema.fields[k].description != v.description
            ) {
              metadata.schema.fields[k].description = v.description;
            }
          }
        },
      );
    }
    jobs.push(fs.promises.writeFile(
      fieldsPath,
      jsonSerializer(metadata.schema.fields),
    ))
  }

  // access.json: local file <--- BigQuery Dataset
  if(metadata?.access) {
    jobs.push(fs.promises.writeFile(
      accessPath,
      jsonSerializer(metadata.access),
    ))
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

  jobs.push(
    fs.promises.writeFile(
      metadataPath,
      jsonSerializer(localMetadata),
    ),
    bqObject.setMetadata(metadata)
  )

  // Sync
  await Promise.all(jobs);
};


export async function pullBigQueryResources() {
  type ResultBQResource = {type: string, path: string, name: string, ddl: string};
  interface BQResourceObject {
    getMetadata(): Promise<[Metadata]>
  }

  const bqClient = new BigQuery();

  const fsWriter = async (
    {type, path, name, ddl}: ResultBQResource,
    bqObj: BQResourceObject
  ) => {
    const pathDir = `${baseDirectory}/${path}/${name}`;
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
        'CREATE IF NOT EXISTS MATERIALIZED VIEW',
      );

    if (!fs.existsSync(pathDir)) {
      await fs.promises.mkdir(pathDir, { recursive: true });
    }
    await syncMetadata(bqObj, pathDir)

    if(type == 'TABLE') {
      const [metadata] = await bqObj.getMetadata();
      if (metadata?.view) {
        const pathView = `${pathDir}/view.sql`;
        await fs.promises.writeFile(
          pathView,
          metadata.view.query
          .replace(/\r\n/g, '\n'),
        );
      }
    }

    await fs.promises.writeFile(pathDDL, cleanedDDL)
      .then(() => console.log(`${type}: ${path}.${name} => ${pathDDL}`));
  }

  bqClient.createQueryJob(`
    select 
      'SCHEMA' as type
      , catalog_name as path
      , schema_name as name
      , ddl 
    from \`INFORMATION_SCHEMA.SCHEMATA\`
  `).then(async ([job]) => {
    await job.getQueryResults()
      .then(async ([records]) => {
        await Promise.all(records
          .map(async (r) => {
            const [bqObj] = await bqClient.dataset(r.name).get()
            await fsWriter(r, bqObj)
          }));
      });
  })
  .catch((err) => {
    console.error(err);
  });  

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
          'ROUTINE' as type
          , format('%s/%s', routine_catalog, routine_schema) as path
          , routine_name as name
          , ddl 
        from \`${dataset.id}.INFORMATION_SCHEMA.ROUTINES\`
        union all
        select distinct
          'TABLE' as type
          , format('%s/%s', table_catalog, table_schema) as path
          , name
          , any_value(replace(ddl, table_name, name)) as ddl
        from \`${dataset.id}.INFORMATION_SCHEMA.TABLES\`
        left join unnest([struct(
          safe.parse_date('%Y%m%d', regexp_extract(table_name, r'\d+$')) as _table_suffix,
          regexp_replace(table_name, r'\d+$', '@_TABLE_SUFFIX') as newname
        )])
        left join unnest([struct(
          if(_table_suffix is not null, newname, table_name) as name
        )])
        group by path, name
      `).then(async ([job]) => {
      await job.getQueryResults()
        .then(async ([records]) => {
          await Promise.all(records.map(
            async r => {
              const bqObj = r.type === 'TABLE'? dataset.table(r.name) : dataset.routine(r.name);
              await fsWriter(r, bqObj)
            }
          ));
        });
      })
      .catch((err) => {
        console.error(err);
      });
    })
    .on('end', () => {});
}

const deployBigQueryResouce = async (bqClient: any, rootDir: string, p: string) => {
  const msgWithPath = (msg: string) => `${path.dirname(p)}: ${msg}`;
  // const jsonSerializer = (obj) => JSON.stringify(obj, null, 4);

  if (p && !p.endsWith('sql')) return null;

  const [_, schemaId, name] = path.dirname(path.relative(rootDir, p))
    .split('/');
  const query = await fs.promises.readFile(p)
    .then((s: any) => s.toString())
    .catch((err: any) => {
      throw new Error(msgWithPath(err));
    });

  const fetchBQJobResource = async (job: any): Promise<any> => {
    const schema = bqClient.dataset(schemaId);
    switch (job.metadata.statistics.query.statementType) {
      case 'CREATE_SCHEMA':
      case 'DROP_SCHEMA':
      case 'ALTER_SCHEMA':
        return await schema.get()
          .then(([d]: [any]) => d);
      case 'CREATE_ROW_ACCESS_POLICY':
      case 'DROP_ROW_ACCESS_POLICY':
        //TODO: row access policy
        // console.log(job.metadata.statistics);
        break;
      case 'CREATE_MODEL':
      case 'EXPORT_MODEL':
        //TODO: models
        break;
      case 'CREATE_FUNCTION':
      case 'CREATE_TABLE_FUNCTION':
      case 'DROP_FUNCTION':
      case 'CREATE_PROCEDURE':
      case 'DROP_PROCEDURE':
        const routineId = name;
        const [routine] = await schema.routine(routineId).get();
        return routine;
      case 'CREATE_TABLE':
      case 'CREATE_VIEW':
      case 'CREATE_TABLE_AS_SELECT':
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
        const [table] = await schema.table(name).get();
        return table;
      case 'SCRIPT':
        //TODO: script
        const [childJobs, _] = await bqClient.getJobs({
          parentJobId: job.id,
        });

        for(const ix in childJobs) {
          const stat = childJobs[ix].metadata.statistics;
          if (stat.query?.ddlTargetRoutine){
            const [routine] = await schema.routine(stat.query.ddlTargetRoutine.routineId).get();
            return routine
          }
          if (stat.query?.ddlTargetTable){
            const [table] = await schema.table(stat.query.ddlTargetTable.tableId).get();
            return table;
          }
        }
        console.error(`No Handled: ${childJobs}`);
        break
      default:
        console.error(`Not Found: ${job.metadata.statistics.query.statementType}`);
    } 

  }

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
      await job.getQueryResults();
      await fetchBQJobResource(job)
          .then((bqObj: any)=> syncMetadata(bqObj, path.dirname(p)))
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
      await syncMetadata(view, path.dirname(p));
      break;
  }
  return null;
};

const pathToBigQueryIdentifier = async (bqClient: BigQuery) => {
  const defautlProjectID = await bqClient.getProjectId();

  return (fpath: string) => {
    const rootDir = path.normalize(baseDirectory);
    const [catalogId, schemaId, name] = path.dirname(
      path.relative(rootDir, fpath.replace("@default", defautlProjectID)),
    ).split('/');
    return [catalogId, schemaId, name].filter((n) => n).join('.');
  };
}

const normalizedBQPath = (bqPath: string, defaultProject?: string): string => {
  const parts = bqPath.replace(/`/g, '').split('.');

  if(parts.length == 2){
    const [dst_schema, dst_name] = parts;
    const dst_project = defaultProject;
    return `${dst_project}.${dst_schema}.${dst_name}`;
  } else if(parts.length == 1) {
    const [dst_schema] = parts;
    return `${defaultProject}.${dst_schema}`;
  }
  else {
    const [dst_project, dst_schema, dst_name] = parts;
    return `${dst_project}.${dst_schema}.${dst_name}`;
  }
}

const extractBigQueryDependencies = async (fpath: string, bqClient: BigQuery) => {
  const path2bq = await pathToBigQueryIdentifier(bqClient);
  const [projectID, schema, resource] = path2bq(fpath).split('.')
  const sql: string = await fs.promises.readFile(fpath)
    .then((s: any) => s.toString());

  const refs = [...new Set(
    extractRefenrences(sql)
      .map(ref => normalizedBQPath(ref, projectID))
  )];
  const refs_schemas = [...new Set(refs)].map(n => n.replace(/\.[^.]+$/, ''));

  // Add schema as explict dependencies without self 
  const additionals = ((schema !== undefined && resource !== undefined) ? [normalizedBQPath(schema, projectID)] : []);
  return [...new Set(refs_schemas.concat(refs).concat(additionals))];
}

const buildDAG = async () => {
  const bqClient = new BigQuery();
  const limit = pLimit(2);
  const path2bq = await pathToBigQueryIdentifier(bqClient);

  const rootDir = path.normalize(baseDirectory);
  const results = await Promise.all(
    (await walk(rootDir))
      .filter((p: string) => p.endsWith('sql'))
      .map(async (n: string) => ({
        file: n,
        bigquery: path2bq(n),
        dependencies: await extractBigQueryDependencies(n, bqClient),
      } as BigQueryJobResource)),
  );
  const relations = [...results
    .reduce((ret, { bigquery: tgt, dependencies: deps }) => {
      ret.add(JSON.stringify(['#sentinal', tgt]));
      deps.forEach(
        (d: string) => {
          ret.add(JSON.stringify([tgt, d]));
        },
      );
      return ret;
    }, new Set())
  ].map((n: any) => JSON.parse(n));

  const bigquery2Obj = Object.fromEntries(results.map((n) => [n.bigquery, n]));
  const DAG: Map<string, {
    task: Task,
    bigquery: BigQueryJobResource
  }> = new Map(
    topologicalSort([...relations])
    .map((bq) => bigquery2Obj[bq])
    .filter((n):n is BigQueryJobResource => !!n)
    .map(
      (target: BigQueryJobResource) => (
      [
        target.bigquery, 
        {
          task: new Task(target.bigquery, 
            async () => {
              await Promise.all(
                target.dependencies
                .map(
                  (d: string) => DAG.get(d)?.task.runningPromise)
              )
              await deployBigQueryResouce(bqClient, rootDir, target.file)
            }),
          bigquery: target
          }
      ]
    )
  ))

  const tasks = [...DAG.values()]
    .map(({task}) => {
      limit(async () => await task.run()); return task}
    );
  while(tasks.some(t => !t.done())) {
    await new Promise(resolve => setTimeout(resolve, 100))
    logUpdate(
      `Tasks: remaing ${limit.pendingCount + limit.activeCount}\n`
      + '  ' + tasks.map(t => t.report()).join('\n  ')
    )
  }
};

export async function pushBigQueryResources() {
    await buildDAG()
}

function createCLI() {
  cli
    .command('push', '説明') // コマンド
    .option('--opt', '説明') // 引数オプション
    .action(async (options: any) => {
      // 実行したい処理
      console.log('push command', options); // 引数の値をオブジェクトで受け取れる
      await pushBigQueryResources()
    });

  cli
    .command('pull', '説明') // コマンド
    .option('--opt', '説明') // 引数オプション
    .action(async (options: any) => {
      // 実行したい処理
      console.log('push command', options); // 引数の値をオブジェクトで受け取れる
      pullBigQueryResources();
    });

  cli.help();
  cli.parse();
}


const main = async () => {
  // bqClient()
  createCLI()
};

main();
