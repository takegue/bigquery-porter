import * as fs from 'node:fs';
import * as path from 'node:path';

import Parser from 'tree-sitter';
import Language from 'tree-sitter-sql-bigquery';

import { normalizeShardingTableId } from './bigquery.js';

async function walk(dir: string): Promise<string[]> {
  const fs_files = await fs.promises.readdir(dir);
  const files: string[][] = await Promise.all(fs_files.map(async (file) => {
    const filePath = path.join(dir, file);
    const stats = await fs.promises.stat(filePath);
    if (stats.isDirectory()) {
      return walk(filePath);
    } else if (stats.isFile()) {
      return [filePath];
    }
    return [];
  }));

  return files.reduce(
    (all: string[], folder_contents) => {
      return all.concat(folder_contents);
    },
    [] as string[],
  ) as string[];
}

type Relation = [string, string];
function topologicalSort(relations: Relation[]) {
  const [E, G, N] = relations.reduce((
    [E, G, N]: [Map<string, number>, Map<string, Set<string>>, Set<string>],
    [src, dst]: Relation,
  ) => {
    E.set(src, 1 + (E.get(src) ?? 0));
    G.set(dst, new Set([src, ...G.get(dst) ?? []]));
    N.add(dst);
    N.add(src);
    return [E, G, N];
  }, [
    new Map<string, number>(),
    new Map<string, Set<string>>(),
    new Set<string>(),
  ]);

  let S = [...N].filter((n) => (E.get(n) ?? 0) === 0);
  let L = new Set<string>();

  S.forEach((n) => N.delete(n));
  while (S.length > 0) {
    const tgt: string | undefined = S.pop();
    if (!tgt) {
      break;
    }
    L.add(tgt);

    (G.get(tgt) ?? []).forEach((n: string) => E.set(n, -1 + (E.get(n) ?? 0)));
    S = [...N]
      .filter((n) => (E.get(n) ?? 0) <= 0)
      .reduce((ret, n) => {
        N.delete(n);
        return [n, ...ret];
      }, S);
  }

  if (N.size > 0) {
    throw new Error(`Cycle detected: ${[...N].join(', ')}`);
  }

  return [...L];
}

const parser = new Parser();
parser.setLanguage(Language);

const findBigQueryResourceIdentifier = function* (node: any): any {
  const resource_name = _extractBigQueryResourceIdentifier(node);
  if (resource_name != null) {
    yield resource_name;
  }

  for (let ix in node.children) {
    for (let n of findBigQueryResourceIdentifier(node.children[ix])) {
      if (n != null) {
        yield n;
      }
    }
  }
};

const _extractBigQueryResourceIdentifier = (node: any) => {
  const fields: string[] = node.fields ?? [];
  if (fields.includes('tableNameNode')) {
    return node.tableNameNode;
  }

  if (fields.includes('routineNameNode')) {
    return node.routineNameNode;
  }

  if (fields.includes('schemaNameNode')) {
    return node.schemaNameNode;
  }

  if (fields.includes('modelNameNode')) {
    return node.modelNameNode;
  }

  if (fields.includes('aliasNameNode')) {
    return node.aliasNameNode;
  }

  if (fields.includes('functionNode')) {
    return node.functionNode;
  }

  // FOREIGN KEY
  if (fields.includes('referencedTableNameNode')) {
    return node.referencedTableNameNode;
  }

  return null;
};

function _replaceWildcard(table: string): string {
  if (table.startsWith('`') && table.endsWith('`')) {
    const trimmed = table.replace(/^`/, '').replace(/`$/, '');
    return `\`${normalizeShardingTableId(trimmed)}\``.replace('@', '*');
  }

  return normalizeShardingTableId(table).replace('@', '*');
}

type StatementType = 'DML' | 'DDL_DROP' | 'DDL_CREATE' | 'UNKNOWN';
type ResourceType =
  | 'SCHEMA'
  | 'MODEL'
  | 'TABLE'
  | 'ROUTINE'
  | 'TEMPORARY_TABLE'
  | 'TEMPORARY_ROUTINE';

function _categorizeDDL(
  n: string,
): StatementType {
  if (n.startsWith('create')) {
    return 'DDL_CREATE';
  } else if (n.startsWith('insert')) {
    return 'DML';
  } else if (n.startsWith('merge')) {
    return 'DML';
  } else if (n.startsWith('truncate')) {
    return 'DML';
  } else if (n.startsWith('drop')) {
    return 'DDL_DROP';
  }
  return 'UNKNOWN';
}

function extractDestinations(
  sql: string,
): [string, ResourceType, StatementType][] {
  const tree = parser.parse(sql);
  let ret: [string, ResourceType, StatementType][] = [];

  for (let n of findBigQueryResourceIdentifier(tree.rootNode)) {
    const statementType = _categorizeDDL(n.parent.type);

    if (n.parent.type.match(/schema_statement/)) {
      ret.push([n.text, 'SCHEMA', statementType]);
    } else if (n.parent.type.match(/procedure_statement|function_statement/)) {
      const is_temp = n.parent.children.some((c: any) =>
        c.type == 'keyword_temporary'
      );
      ret.push([
        n.text,
        is_temp ? 'TEMPORARY_ROUTINE' : 'ROUTINE',
        statementType,
      ]);
    } else if (n.parent.type.match(/create_model_statement/)) {
      ret.push([n.text, 'MODEL', statementType]);
    } else if (n.parent.type.match(/table_statement/)) {
      const is_temp = n.parent.children.some((c: any) =>
        c.type == 'keyword_temporary'
      );
      ret.push([
        _replaceWildcard(n.text),
        is_temp ? 'TEMPORARY_TABLE' : 'TABLE',
        statementType,
      ]);
    } else if (
      n.parent.type.match(/statement/) &&
      !n.parent.type.match(/call_statement/)
    ) {
      ret.push([_replaceWildcard(n.text), 'TABLE', statementType]);
      continue;
    }
  }
  return ret;
}

function extractRefenrences(sql: string): string[] {
  const tree = parser.parse(sql);
  let ret = [];
  let CTEs = new Set<string>();

  for (let n of findBigQueryResourceIdentifier(tree.rootNode)) {
    // Exclude CTE names from  candidates
    if (n.parent.type.match(/cte/)) {
      CTEs.add(n.text);
    }

    if (n.parent.type.match(/from_item/)) {
      ret.push(_replaceWildcard(n.text));
    }

    if (n.parent.type.match(/function_call/)) {
      ret.push(n.text);
    }

    if (n.parent.type.match(/call_statement/)) {
      ret.push(n.text);
    }

    if (n.parent.type === 'foreign_key_references') {
      ret.push(n.text);
    }
  }
  return ret.filter((n) => !CTEs.has(n));
}

/**
 * Format milliseconds as human-readable text.
 *
 * @param {number} ms Number of milliseconds to be turned into a human-readable string.
 */
function msToTime(ms: number): string {
  // Pad to 2 or 3 digits, default is 2
  function pad(n: number, z?: number): string {
    z = z || 2;
    return ('  ' + n).slice(-z);
  }

  const seconds = ms / 1000;
  const minutes = seconds / 60;
  const hours = minutes / 60;

  if (hours >= 1) {
    return [
      `${pad(Math.floor(hours))}h`,
      `${pad(Math.floor(minutes % 60))}m`,
      `${pad(Math.floor(seconds % 60))}s`,
    ].join();
  }

  if (minutes >= 1) {
    return `${pad(Math.floor(minutes % 60))}m${pad(Math.floor(seconds % 60))}s`;
  }

  if (seconds >= 1) {
    return `${pad(Math.floor(seconds % 60))}s`;
  }

  return `${pad(Math.floor(ms), 2)}ms`;
}

/**
 * Format bytes as human-readable text.
 *
 * @param bytes Number of bytes.
 * @param si True to use metric (SI) units, aka powers of 1000. False to use
 *           binary (IEC), aka powers of 1024.
 * @param dp Number of decimal places to display.
 *
 * @return Formatted string.
 */
function humanFileSize(bytes: number, si = false, dp = 1) {
  const thresh = si ? 1000 : 1024;

  if (Math.abs(bytes) < thresh) {
    return bytes + 'B';
  }

  const units = si
    ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
  let u = -1;
  const r = 10 ** dp;

  do {
    bytes /= thresh;
    ++u;
  } while (
    Math.round(Math.abs(bytes) * r) / r >= thresh && u < units.length - 1
  );

  return bytes.toFixed(dp) + ' ' + units[u];
}

export {
  extractDestinations,
  extractRefenrences,
  humanFileSize,
  msToTime,
  Relation,
  StatementType,
  topologicalSort,
  walk,
};
