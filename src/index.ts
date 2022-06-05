// Imports the Google Cloud client library
import logUpdate from 'log-update';
import readlinePromises from 'readline';
import { isatty } from 'tty';
import { BigQuery, Dataset, Table, Routine, GetDatasetsOptions } from '@google-cloud/bigquery';
// import type { Metadata } from '@google-cloud/common';
import * as fs from 'fs';
import * as path from 'path';
import {cac} from 'cac';
import pLimit from 'p-limit';
import pThrottle from 'p-throttle';
import {
  topologicalSort,
  walk,
  extractRefenrences,
} from '../src/util.js';

import {
  Task,
} from '../src/reporter.js';
import 'process';


const cli = cac();

const baseDirectory = './bigquery';
const jsonSerializer = (obj: any) => JSON.stringify(obj, null, 4);

const sqlDDLForSchemata = (projectId: string) => `
select 
  'SCHEMA' as type
  , schema_name as name
  , ddl 
from \`${projectId}.INFORMATION_SCHEMA.SCHEMATA\`
`;

//FIXME: BigQuery Runtime Error on many datasets
//FIXME: BigQuery Runtime Error on many tables in one dataset
//This sql can get only resources in default location.
const sqlDDLForProject = `
declare catalog string default @projectId;
declare schemas array<string> default @datasets;
if schemas is null then
  execute immediate format(
    "select as struct array_agg(distinct schema_name) as schemas from \`%s.INFORMATION_SCHEMA.SCHEMATA\`"
    , catalog
  ) into schemas;
end if;

execute immediate (
  select as value
    string_agg(template, "union distinct")
  from unnest(schemas) schema
  left join unnest([format("%s.%s", catalog, schema)]) identifier 
  left join unnest([format(
"""-- SQL TEMPLATE
select
  'ROUTINE' as type
  , routine_name as name
  , ddl
from \`%s.INFORMATION_SCHEMA.ROUTINES\`
union distinct
select distinct
  'TABLE' as type
  , table_name as name
  , ddl
from \`%s.INFORMATION_SCHEMA.TABLES\`
"""
  , identifier, identifier
)]) template
)`


type BigQueryJobResource = {
  file: string,
  bigquery: string,
  dependencies: string[]
}

// type Labels = Map<string, string>;
const syncMetadata = async (
  bqObject: any,
  dirPath: string,
  options?: {versionhash?: string, push?: boolean}
) => {
  type systemDefinedLabels = {
    'bqm-versionhash': string
  }

  const metadataPath = path.join(dirPath, 'metadata.json');
  const fieldsPath = path.join(dirPath, 'schema.json');
  const syncLabels: systemDefinedLabels = {
    'bqm-versionhash': `${Math.floor(Date.now() / 1000)}-${options?.versionhash ?? 'HEAD'}`
  }
  const jobs: Promise<any>[] = []

  const projectId =
      bqObject.metadata?.datasetReference?.projectId
      ?? bqObject.parent.metadata?.datasetReference?.projectId;
  const [metadata] = await bqObject.getMetadata({projectId, location: bqObject.location});

  // schema.json: local file <---> BigQuery Table
  if(metadata?.schema?.fields) {
    // Merge upstream and downstream schema description
    // due to some bigquery operations like view or materialized view purge description
    if (fs.existsSync(fieldsPath)) {
      const oldFields = await fs.promises.readFile(fieldsPath)
        .then(s => JSON.parse(s.toString()))
        .catch((err: Error) => console.error(err));
      // Update
      Object.entries(metadata.schema.fields).map(
        ([k, v]: [string, any]) => {
          if (
            k in oldFields
            && metadata.schema.fields[k].description 
            && metadata.schema.fields[k].description != v.description
          ) {
            metadata.schema.fields[k].description = v.description;
          }
        },
      );
    }

    jobs.push(fs.promises.writeFile(
      fieldsPath,
      jsonSerializer(metadata.schema.fields),
    ))
  }

  const newMetadata = Object.fromEntries(Object.entries({
      type: metadata.type,
      routineType: metadata.routineType,
      modelType: metadata.modelType,
      description: metadata.description,
      // Filter predefined labels
      labels: Object.fromEntries(
        Object.entries(metadata?.labels ?? [])
        .filter(([k]) => !(k in syncLabels))
      ),
      // Dataset attribute
      access: metadata?.access,
      location: bqObject instanceof Dataset ? metadata?.location : undefined,

      // Routine atributes
      argument: metadata?.arguments,
      language: metadata?.language,

      // SCALAR FUNCTION / PROCEDURE attribute
      returnType: metadata?.returnType,

      // TABLE FUNCTION attribute
      returnTableType: metadata?.returnTableType

    }).filter(([_, v]) => !!v && Object.keys(v).length > 0),
  );

  if (fs.existsSync(metadataPath)) {
    const local = await fs.promises.readFile(metadataPath)
      .then(s => JSON.parse(s.toString())) 

    if(options?.push) {
      Object.entries(newMetadata).forEach(([attr]) => {
        metadata[attr] = local[attr];
      })
      jobs.push(bqObject.setMetadata(metadata));
    } else {
      Object.entries(newMetadata).forEach(([attr]) => {
        newMetadata[attr] = local[attr] ?? metadata[attr];
      })
    }
  }
  jobs.push(fs.promises.writeFile(metadataPath, jsonSerializer(newMetadata)));

  await Promise.all(jobs)
};


export async function pullBigQueryResources({
  projectId,
  withDDL
}: {projectId?: string, withDDL?: boolean}
) {

  type ResultBQResource = {type: string, path: string, name: string, ddl: string | undefined, resource_type: string};
  // interface BQResourceObject {
  //   parent: any,
  //   metadata: Metadata,
  //   getMetadata(options?: any): Promise<[Metadata]>
  // }
  const throttle = pThrottle({
    limit: 20,
    interval: 500
  });
  const bqClient= new Proxy<BigQuery>(
    new BigQuery(),
    {
      get: (obj: any, sKey: any) => {
        const member = obj[sKey]
        // Request Throttling 
        if(member instanceof Function && sKey == 'request') {
          return async (...args: any[]) => {
            let result: any;
            await throttle(async () => {
              result = member.apply(obj, args);
            })()
            return result
          }
        }
        return member;
      }
    },
  );

  const defaultProjectId = await bqClient.getProjectId()

  const bqTreePath = (bqObj: any, defaultProject: string) => {
    let tree: string[] = [];
    let it = bqObj ;
    while(it.id) {
      tree.push(it.id)
      it = it?.parent
    }
    tree.push(
      defaultProject
      ?? bqObj.metadata?.datasetReference?.projectId
      ?? bqObj.parent.metadata?.datasetReference?.projectId
    )
    return tree.reverse()
  }


  let bqObj2DDL:any = {};
  const fsWriter = async (
    bqObj: any,
  ) => {
    const path = bqTreePath(bqObj, projectId ?? "@default").join('/')
    const bqId = bqTreePath(bqObj, projectId ?? defaultProjectId).join('.')
    const pathDir = `${baseDirectory}/${path}`;
    const catalogId = (
      bqObj.metadata?.datasetReference?.projectId
      ?? bqObj.parent.metadata?.datasetReference?.projectId
      ?? defaultProjectId
    ) as string;
    bqObj['projectId'] = catalogId;

    if (!fs.existsSync(pathDir)) {
      await fs.promises.mkdir(pathDir, { recursive: true });
    }
    console.log(`${bqId} => ${pathDir}`)

    await syncMetadata(bqObj, pathDir, {push: false})
      .catch(e => {console.log('syncerror', e, bqObj); throw e;})

    if(bqObj.metadata.type  == 'VIEW') {
      let [metadata] =  await bqObj.getMetadata();
      if (metadata?.view) {
        const pathView = `${pathDir}/view.sql`;
        await fs.promises.writeFile(
          pathView,
          metadata.view.query
          .replace(/\r\n/g, '\n'),
        );
      }
    }

    if(!withDDL) {
      return
    }

    // const ddlBody = bqObj.definitionBody ?? bqObj?.view?.query ?? bqObj?.materializedView?.query;
    // const ddlHeader = `CREATE OR REPLACE ${typeof bqObj}`
    // const bqObj2DDL = `${ddlHeader} ${ddlBody}` ;
    const ddlStatement = bqObj2DDL[bqObj.id]?.ddl;
    console.log(catalogId, bqObj.id, ddlStatement)
    if(!ddlStatement) {
      return
    }

    const pathDDL = `${pathDir}/ddl.sql`;
    const cleanedDDL = ddlStatement
      .replace(/\r\n/g, '\n')
      .replace('CREATE PROCEDURE', 'CREATE OR REPLACE PROCEDURE')
      .replace(
        'CREATE TABLE FUNCTION',
        'CREATE OR REPLACE TABLE FUNCTION',
      )
      .replace('CREATE FUNCTION', 'CREATE OR REPLACE FUNCTION')
      .replace(/CREATE TABLE/, 'CREATE TABLE IF NOT EXISTS')
      .replace(/CREATE TABLE/, 'CREATE SCHEMA IF NOT EXISTS')
      .replace(/CREATE VIEW/, 'CREATE OR REPLACE VIEW')
      .replace(
        /CREATE MATERIALIZED VIEW/,
        'CREATE IF NOT EXISTS MATERIALIZED VIEW',
      );

    await fs.promises.writeFile(pathDDL, cleanedDDL)
  }

  const [datasets] = await bqClient.getDatasets({projectId} as GetDatasetsOptions);

  if(withDDL) {
    const ddlFetcher = async (sql: string, params: { [param: string]: any }
    ) => {
      // Import BigQuery dataset Metadata
      const [job] = await bqClient
        .createQueryJob({
          query: sql,
          params 
        })
        .catch(e => {
          console.log(e.message)
          return []
        })
      if(!job) {
        return undefined
      }
      const [records] = await job.getQueryResults()
      return Object.fromEntries(records.map((r: ResultBQResource)  => [r.name, r]))
    }

    const schemaDDL = await ddlFetcher(sqlDDLForSchemata(projectId ?? defaultProjectId), {});
    const resourceDDL = await ddlFetcher(sqlDDLForProject, {
      projectId: projectId ?? defaultProjectId,
      datasets: datasets
        .filter(d => d.location == 'US')
        .map(d => d?.id ?? d.metadata?.id)
    });
    bqObj2DDL = {...schemaDDL, ...resourceDDL}
  }

  await Promise.allSettled(
    datasets.map(async (dataset: any) => [
        // Schema
        // fsWriter(dataset, schemaDDL[dataset.metadata?.datasetReference.datasetId]?.ddl)
        fsWriter(dataset)
          .catch(e => console.log('dataset ', e, dataset)), 
        // Tables
        ...(await dataset.getTables().then(
          ([rets]: [Table[]]) => rets.map((bqObj) => fsWriter(bqObj)))),
        // Routines
        ...(await dataset.getRoutines().then(
          ([rets]: [Routine[]]) => rets.map((bqObj) => fsWriter(bqObj)))),
      ]
    ).flat()
  )

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
        console.error(`Not Supported: ${childJobs}`);
        break
      default:
        console.error(`Not Supported: ${job.metadata.statistics.query.statementType}`);
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
          .then((bqObj: any)=> syncMetadata(bqObj, path.dirname(p), {push: true}))
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
      await syncMetadata(view, path.dirname(p), {push: true});
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

const buildDAG = async (rootPath: string, files: string[]) => {
  const limit = pLimit(2);
  const bqClient = new BigQuery();
  const path2bq = await pathToBigQueryIdentifier(bqClient);

  const rootDir = path.normalize(rootPath);
  const results = await Promise.all(
    files
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
              ).catch(() => {
                const msg = target.dependencies
                  .map(t => DAG.get(t)?.task)
                  .filter(t => t && t.status == 'failed')
                  .map(t => t?.name).join(', ')
                ;
                throw Error('Suspended: Parent job is faild: ' + msg)
              })

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

export async function pushBigQueryResources(options: {rootDir: string, projectId?: string}) {
  const rootDir = options.rootDir ?? baseDirectory;
  const inputFiles: string[] = await (async () => {
    if(isatty(0)) {
      return await walk(rootDir)
    }
    const rl = readlinePromises.createInterface({
      input: process.stdin,
    });
    const buffer: string[] = [];
    for await (const line of rl) {
      buffer.push(line)
    }
    return buffer
  })();

  const files = inputFiles
    .filter((p: string) => p.endsWith('sql'))
    .filter((p: string) => p.includes(options.projectId ?? '@default'))
  ;
  console.log(inputFiles)

  await buildDAG(rootDir, files)
}

function createCLI() {
  cli
    // Global Options
    .option('--concurrency', "API Call Concurrency", {
      default: 8,
      type: [Number],
    })
    .option('-C, --root-path', "API Call Concurrency", {
      default: "bigquery",
      type: [Number],
    })
    .command('push [...projects]', '説明')
    .action(async (projects: string[], cmdOptions: any) => {
      const options = {
        rootDir: cmdOptions.rootPath as string,
        projectId: projects[0] as string,
      }
      await pushBigQueryResources(options)
    });

  cli
    .command('pull [...projects]', 'pull dataset and its tabald and routine information')
    .option('--with-ddl', "Pulling BigQuery Resources with DDL SQL", {
      default: false,
      type: [Boolean],
    })
    .action(async (projects: string[], cmdOptions: any) => {
      const options = {
        withDDL: cmdOptions.withDdl
      }
      console.log(options);

      if(projects.length > 0) {
        await Promise.allSettled(
          projects.map(async (p) => await pullBigQueryResources({projectId: p, ...options}))
        )
      } else {
        await pullBigQueryResources({...options})
      }
    });

  cli.help();
  cli.parse();
}


const main = async () => {
  // bqClient()
  createCLI()
};


main();
