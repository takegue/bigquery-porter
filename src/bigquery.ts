
import type { Metadata } from '@google-cloud/common';

interface BigQueryResource {
  id?: string,
  baseUrl?: string,
  metadata?: Metadata,
  projectId?: string;
  parent?: BigQueryResource,
}

const bq2path = (bqObj: BigQueryResource, asDefaultProject: boolean) => {
  let tree: string[] = [];
  let it: BigQueryResource = bqObj ;
  let depth = 0;

  while(true) {
    depth += 1
    if(it.id) {
      tree.push(it.id);
    }

    if(!it.parent) {
      break;
    }
    it = it.parent;
  }

  if(asDefaultProject) {
    tree.push("@default");
  } else if (bqObj.projectId) {
    tree.push(bqObj.projectId);
  } else if (bqObj.parent?.projectId) {
    tree.push(bqObj.parent.projectId);
  }  

  const ns =  bqObj.baseUrl?.replace('/', '@');
  if(ns && depth == 3 && !["/tables"].includes(bqObj.baseUrl as string)) {
    tree.splice(1, 0, ns)
  }
  return tree.reverse().join('/')
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


export {
  BigQueryResource,
  normalizedBQPath,
  bq2path,
}
