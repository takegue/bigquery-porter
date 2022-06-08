
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
  } else {
    if(bqObj.projectId) {
      tree.push(bqObj.projectId);
    }
  }

  const ns =  bqObj.baseUrl?.replace('/', '@').replace(/$/, 's');
  if(ns && depth == 3 && !["/table"].includes(bqObj.baseUrl as string)) {
    tree.splice(1, 0, ns)
  }
  return tree.reverse().join('/')
}


export {
  BigQueryResource,
  bq2path
}
