import { StatementType, topologicalSort } from '../../src/util.js';
import { getDyanmicLineage } from '../../src/dataLineage.js';

type JobConfig = {
  file: string;
  namespace: string;
  dependencies: string[];
  destinations: [string, StatementType][];
  shouldDeploy: boolean;
};

const buildRelationsInDataOperation = (
  jobs: JobConfig[],
): Set<string> => {
  const repoStmtType = new Map<string, StatementType>();
  const key = ([f, id]: [string, string]) => JSON.stringify([f, id]);
  const priority = (s: StatementType): number => {
    switch (s) {
      case 'DDL_DROP':
        return 3;
      case 'DDL_CREATE':
        return 2;
      case 'DML':
        return 1;
      case 'UNKNOWN':
        return 0;
    }
  };
  const ret = new Set<string>();

  const bq2files = new Map<string, Set<string>>();
  for (const { destinations, file } of jobs) {
    for (const [dst, stype] of destinations) {
      if (!bq2files.has(dst)) {
        bq2files.set(dst, new Set());
      }
      bq2files.get(dst)?.add(file);
      repoStmtType.set(key([file, dst]), stype);
    }
  }

  for (const [bq, dsts] of bq2files) {
    // DDL_CREATE <- DML(Data Update: ) <- DDL_DROP
    const orderdDsts = Array.from(dsts).sort((lhs, rhs) => {
      const aType = repoStmtType.get(key([lhs, bq])) ?? 'UNKNOWN';
      const bType = repoStmtType.get(key([rhs, bq])) ?? 'UNKNOWN';
      return priority(aType) - priority(bType);
    });
    for (let ix = 1; ix < orderdDsts.length; ix++) {
      ret.add(JSON.stringify([orderdDsts[ix - 1], orderdDsts[ix]]));
    }
  }
  return ret;
};

const buildDAG = async (
  jobs: JobConfig[],
  options?: {
    enableDataLineage: boolean;
  },
): Promise<[JobConfig[], WeakMap<JobConfig, JobConfig[]>]> => {
  const job2deps = new WeakMap<JobConfig, JobConfig[]>();
  const file2job = Object.fromEntries(jobs.map((j) => [j.file, j]));

  const bq2files = new Map<string, { dsts: Set<string>; refs: Set<string> }>();
  for (const { destinations, dependencies, file } of jobs) {
    for (const [dst] of destinations) {
      if (!bq2files.has(dst)) {
        bq2files.set(dst, { dsts: new Set(), refs: new Set() });
      }
      bq2files.get(dst)?.dsts.add(file);
    }
    for (const dep of dependencies) {
      if (!bq2files.has(dep)) {
        bq2files.set(dep, { dsts: new Set(), refs: new Set() });
      }
      bq2files.get(dep)?.refs.add(file);
    }
  }

  // job => deps
  for (const { dsts, refs } of bq2files.values()) {
    for (const dst of dsts) {
      const dstJob = file2job[dst];
      if (dstJob === undefined) {
        throw new Error(`Job not found: ${dst}`);
      }

      if (!job2deps.has(dstJob)) {
        job2deps.set(dstJob, []);
      }
    }

    for (const ref of refs) {
      const srcJob = file2job[ref];
      if (srcJob === undefined) {
        throw new Error(`Job not found: ${ref}`);
      }
      if (!job2deps.has(srcJob)) {
        job2deps.set(srcJob, []);
      }

      for (const dst of dsts) {
        const dstJob = file2job[dst];
        if (dstJob === undefined) {
          throw new Error(`Job not found: ${dst}`);
        }

        job2deps.get(srcJob)?.push(dstJob);
      }
    }
  }

  // ordered jobs: static relation
  const relations = new Set<string>([
    ...buildRelationsInDataOperation(jobs),
  ]);

  for (const { dsts, refs } of bq2files.values()) {
    for (const ref of [...refs, '#leaf']) {
      for (const dst of [...dsts, '#root']) {
        relations.add(JSON.stringify([ref, dst]));
      }
    }
  }

  // ordered jobs: dynamic relation
  if (options?.enableDataLineage ?? false) {
    const bqResources = bq2files.keys();
    const lineage = await getDyanmicLineage(bqResources);
    const toFiles = (
      r: { dsts: Set<string>; refs: Set<string> },
    ) => [...r?.refs ?? [], ...r.dsts ?? []];
    for (const [dst, srcs] of lineage) {
      const dstFiles = bq2files.get(dst);
      if (!dstFiles) continue;

      for (const src of srcs) {
        const srcFiles = bq2files.get(src);
        if (!srcFiles || !dstFiles) continue;

        for (const src of toFiles(srcFiles)) {
          for (const dst of toFiles(dstFiles)) {
            relations.add(JSON.stringify([dst, src]));
          }
        }
      }
    }
  }

  const orderdJobs = topologicalSort(
    Array.from(relations.values())
      .map((n: string) => JSON.parse(n))
      .filter(([src, dst]) => src !== dst),
  )
    .filter((s) => !['#root', '#leaf'].includes(s))
    .map((s) => file2job[s])
    .filter((s): s is JobConfig => s !== undefined);

  for (const job of orderdJobs) {
    for (const dep of job2deps.get(job) ?? []) {
      if (dep.shouldDeploy) {
        job.shouldDeploy = true;
        break;
      }
    }
  }

  // DAG Validation: All files should included
  for (const job of orderdJobs) {
    if (!job2deps.has(job)) {
      console.warn(`Warning: No deployment files for ${job.file}`);
    }

    const m = job.namespace.match(/@[0-9_A-Za-z]+/);
    if (m) {
      // Special namespace starts with @ is ignored except predefined resource @MODELS, @ROUTINES
      if (!(['@MODELS', '@ROUTINES'].includes(m[0]))) {
        continue;
      }
    }

    const ns = job.namespace.replace(/@[A-Za-z]+\./, '');
    if (
      !job.destinations.map(([n]) => n).includes(ns) &&
      !job.dependencies.includes(ns)
    ) {
      console.error(
        `Warning: Irrelevant SQL file located in ${job.file} for ${job.namespace}`,
      );
    }
  }

  return [orderdJobs, job2deps];
};

export { buildDAG, JobConfig };
