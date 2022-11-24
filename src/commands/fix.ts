import * as fs from 'node:fs';
import { BigQuery } from '@google-cloud/bigquery';
import Parser from 'tree-sitter';
import Language from 'tree-sitter-sql-bigquery';

import { path2bq } from '../../src/bigquery.js';
import { extractDestinations, walk } from '../../src/util.js';

const formatLocalfiles = async (
  rootPath: string,
  options?: { dryRun?: string },
) => {
  const files = (await walk(rootPath))
    .filter((p: string) => p.endsWith('ddl.sql'))
    .filter(async (p: string) => {
      const sql: string = await fs.promises.readFile(p)
        .then((s: any) => s.toString());
      const destinations = extractDestinations(sql);
      return destinations.length > 0;
    });

  const bqClient = new BigQuery();
  const parser = new Parser();
  parser.setLanguage(Language);

  const defaultProjectId = await bqClient.getProjectId();
  console.log(files);
  for (const file of files) {
    const sql: string = await fs.promises.readFile(file)
      .then((s: any) => s.toString());
    const ns = path2bq(file, rootPath, defaultProjectId).split('.').slice(1)
      .join('.');
    const newSQL = fixDestinationSQL(parser, ns, sql);

    if (newSQL !== sql) {
      console.log(file);
      if (options?.dryRun ?? false) {
      } else {
        await fs.promises.writeFile(file, newSQL);
      }
    }
  }
};

function fixDestinationSQL(
  parser: Parser,
  namespace: string,
  sql: string,
): string {
  let newSQL = sql;
  let tree = parser.parse(sql);

  const _visit = function* (node: any): any {
    yield node;
    for (let n of node.children) {
      for (let c of _visit(n)) {
        yield c;
      }
    }
  };

  const _isRootDDL = function (node: any): any {
    let _n = node;
    let _cnt = 0;
    while (_n !== null) {
      if (_n.type.match(/create/)) {
        _cnt++;
      }
      _n = _n.parent;
    }
    return _cnt <= 1;
  };

  const _detectDDLKind = function (node: any): any {
    if (node.parent == null) {
      return [false, undefined];
    }

    if (node.parent.type.match('create_table_statement')) {
      return [true, 'TABLE'];
    }

    if (node.parent.type.match('create_schema_statement')) {
      return [true, 'SCHEMA'];
    }

    if (node.parent.type.match('create_function_statement')) {
      return [true, 'ROTUINE'];
    }

    if (node.parent.type.match('create_table_function_statement')) {
      return [true, 'ROTUINE'];
    }

    if (node.parent.type.match('create_procedure_statement')) {
      return [true, 'ROTUINE'];
    }

    return [false, undefined];
  };

  const _cleanIdentifier = (n: string) => n.trim().replace(/`/g, '');

  let _iteration = 0;
  let _stop = false;
  let replacedIdentifier: Set<string> = new Set();

  while (!_stop && _iteration < 100) {
    _stop = true;
    const row2count = newSQL.split('\n').map((r) => r.length)
      .reduce((ret, r) => {
        // Sum of ret;
        ret.push((ret[ret.length - 1] ?? 0) + r + 1);
        return ret;
      }, [0] as number[]);

    for (const n of _visit(tree.rootNode)) {
      const desired = `\`${namespace}\``;
      const [isDDL] = _detectDDLKind(n);

      /*
      * Rule #1: If the node is a destination in DDL, then replace it with a qualified name from namespace.
      */
      if (
        n.type === 'identifier' &&
        replacedIdentifier.size == 0 &&
        isDDL &&
        _isRootDDL(n) &&
        // Matching BigQuery Level
        (desired.split('.').length - n.text.split('.').length) ** 2 <= 1
      ) {
        // Memorize propagate modification
        replacedIdentifier.add(_cleanIdentifier(n.text));

        if (n.text !== desired) {
          const start = row2count[n.startPosition.row] + n.startPosition.column;
          const end = row2count[n.endPosition.row] + n.endPosition.column;

          newSQL = newSQL.substring(0, start) + desired + newSQL.substring(end);
          tree.edit({
            startIndex: start,
            oldEndIndex: end,
            newEndIndex: start + desired.length,
            startPosition: n.startPosition,
            oldEndPosition: n.endPosition,
            newEndPosition: {
              row: n.endPosition.row,
              column: n.endPosition.column + desired.length,
            },
          });
        }

        _stop = false;
        break;
      }

      /*
      * Rule #2: Replaced Identifer
      */
      if (
        n.type === 'identifier' &&
        replacedIdentifier.has(_cleanIdentifier(n.text))
      ) {
        const start = row2count[n.startPosition.row] + n.startPosition.column;
        const end = row2count[n.endPosition.row] + n.endPosition.column;
        newSQL = newSQL.substring(0, start) + desired + newSQL.substring(end);
        tree.edit({
          startIndex: start,
          oldEndIndex: end,
          newEndIndex: start + desired.length,
          startPosition: n.startPosition,
          oldEndPosition: n.endPosition,
          newEndPosition: {
            row: n.endPosition.row,
            column: n.endPosition.column + desired.length,
          },
        });

        _stop = false;
        break;
      }
    }

    _iteration += 1;
    tree = parser.parse(newSQL, tree);
  }
  return newSQL;
}

export { fixDestinationSQL, formatLocalfiles };
