import * as fs from 'node:fs';
import * as path from 'node:path';

import Parser from 'tree-sitter';
import Language from 'tree-sitter-sql-bigquery';

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

const findBigQueryResourceIdentifier = function*(node: any): any {
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

  return null;
};


function extractDestinations(sql: string): [string, string][] {
  const tree = parser.parse(sql);
  let ret: [string, string][] = [];

  for (let n of findBigQueryResourceIdentifier(tree.rootNode)) {
    if (n.parent.type.match(/schema_statement/)) {
      ret.push([n.text, 'SCHEMA']);
    }
    else if (n.parent.type.match(/procedure_statement|function_statement/)) {
      ret.push([n.text, 'ROUTINE']);
    }
    else if (n.parent.type.match(/table_statement/)) {
      ret.push([n.text, 'TABLE']);
    }
    else if (n.parent.type.match(/create_model_statement/)) {
      ret.push([n.text, 'MODEL']);
    }
    else if (
      n.parent.type.match(/statement/)
      && !n.parent.type.match(/call_statement/)
    ) {
      ret.push([n.text, 'TABLE']);
      continue
    }
  }
  return ret;
}

function extractRefenrences(sql: string): string[] {
  const tree = parser.parse(sql);
  let ret = [];
  let CTEs = new Set<string>();

  for (let n of findBigQueryResourceIdentifier(tree.rootNode)) {
    if (n.parent.type.match(/non_recursive_cte/)) {
      CTEs.add(n.text);
    }

    if (n.parent.type.match(/from_item/)) {
      ret.push(n.text);
    }

    if (n.parent.type.match(/function_call/)) {
      ret.push(n.text);
    }

    if (n.parent.type.match(/call_statement/)) {
      ret.push(n.text);
    }
  }
  return ret.filter((n) => !CTEs.has(n));
}

function fixDestinationSQL(
  namespace: string,
  sql: string
): string {
  let newSQL = sql;
  let tree = parser.parse(sql);

  const _visit = function*(node: any): any {
    yield node
    for (let n of node.children) {
      for (let c of _visit(n)) {
        yield c
      }
    }
  };
  const _isRootDDL = function(node: any): any {
    let _n = node;
    let _cnt = 0;
    while (_n !== null) {
      if (_n.type.match(/create/)) {
        _cnt++;
      }
      _n = _n.parent
    }
    return _cnt <= 1;
  }
  const _detectDDLKind = function(node: any): any {
    if (node.parent == null) {
      return [false, undefined]
    }

    if (node.parent.type.match('create_table_statement')) {
      return [true, 'TABLE']
    }

    if (node.parent.type.match('create_schema_statement')) {
      return [true, 'SCHEMA']
    }

    if (node.parent.type.match('create_function_statement')) {
      return [true, 'ROTUINE']
    }

    if (node.parent.type.match('create_table_function_statement')) {
      return [true, 'ROTUINE']
    }

    if (node.parent.type.match('create_procedure_statement')) {
      return [true, 'ROTUINE']
    }

    return [false, undefined]
  }

  const _cleanIdentifier = (n: string) => n.trim().replace(/`/g, '');

  let _iteration = 0
  let _stop = false
  let replacedIdentifier: Set<string> = new Set();

  while (!_stop && _iteration < 100) {
    _stop = true
    const row2count = newSQL.split('\n').map((r) => r.length)
      .reduce((ret, r) => {
        // Sum of ret;
        ret.push((ret[ret.length - 1] ?? 0) + r + 1);
        return ret
      }, [0] as number[])

    for (const n of _visit(tree.rootNode)) {
      const desired = `\`${namespace}\``
      const [isDDL] = _detectDDLKind(n);

      /*
      * Rule #1: If the node is a destination in DDL, then replace it with a qualified name from namespace.
      */
      if (
        n.type === 'identifier'
        && replacedIdentifier.size == 0
        && isDDL
        && _isRootDDL(n)
        // Matching BigQuery Level
        && (desired.split('.').length - n.text.split('.').length) ** 2 <= 1
      ) {
        // Memorize propagate modification
        replacedIdentifier.add(_cleanIdentifier(n.text))

        if (n.text !== desired) {
          const start = row2count[n.startPosition.row] + n.startPosition.column
          const end = row2count[n.endPosition.row] + n.endPosition.column

          newSQL = newSQL.substring(0, start) + desired + newSQL.substring(end);
          tree.edit({
            startIndex: start,
            oldEndIndex: end,
            newEndIndex: start + desired.length,
            startPosition: n.startPosition,
            oldEndPosition: n.endPosition,
            newEndPosition: { row: n.endPosition.row, column: n.endPosition.column + desired.length },
          })
        }

        _stop = false
        break;
      }

      /*
      * Rule #2: Replaced Identifer
      */
      if (
        n.type === 'identifier'
        && replacedIdentifier.has(_cleanIdentifier(n.text))
      ) {
        const start = row2count[n.startPosition.row] + n.startPosition.column
        const end = row2count[n.endPosition.row] + n.endPosition.column
        newSQL = newSQL.substring(0, start) + desired + newSQL.substring(end);
        tree.edit({
          startIndex: start,
          oldEndIndex: end,
          newEndIndex: start + desired.length,
          startPosition: n.startPosition,
          oldEndPosition: n.endPosition,
          newEndPosition: { row: n.endPosition.row, column: n.endPosition.column + desired.length },
        });

        _stop = false
        break
      }
    }

    _iteration += 1
    tree = parser.parse(newSQL, tree)
  }
  return newSQL
}


/**
 * Format milliseconds as human-readable text.
 *
 * @param {number} milliseconds Number of milliseconds to be turned into a
 *    human-readable string.
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
    return `${pad(Math.floor(hours))}h${pad(Math.floor(minutes % 60))}m${pad(Math.floor(seconds % 60))}s`;
  }

  if (minutes >= 1) {
    return `${pad(Math.floor(minutes % 60))}m${pad(Math.floor(seconds % 60))}s`;
  }

  if (seconds >= 1) {
    return `${pad(Math.floor(seconds % 60))}s`;
  }

  return `${pad(Math.floor(ms), 3)}ms`;
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
    return bytes + ' B';
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
  topologicalSort,
  walk,
  fixDestinationSQL
};
