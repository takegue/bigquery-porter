import * as fs from 'node:fs';
import { BigQuery } from '@google-cloud/bigquery';
import Parser from 'tree-sitter';
import Language from 'tree-sitter-sql-bigquery';

import { path2bq } from '../../src/bigquery.js';
import {
  extractDestinations,
  extractRefenrences,
  walk,
} from '../../src/util.js';

const formatLocalfiles = async (
  rootPath: string,
  options?: { dryRun?: string },
) => {
  const files = (await walk(rootPath))
    .filter((f) => f.endsWith('.sql'));

  const bqClient = new BigQuery();
  const parser = new Parser();
  parser.setLanguage(Language);

  const defaultProjectId = await bqClient.getProjectId();
  for (const file of files) {
    const sql: string = await fs.promises.readFile(file)
      .then((s: any) => s.toString());
    const bqId = path2bq(file, rootPath, defaultProjectId);
    const ns = bqId.split('.').slice(1).join('.');
    const nsType = (() => {
      const levels = bqId.split('.').length;
      if (levels == 3) {
        return 'table_or_routine';
      }
      if (levels == 2) {
        return 'dataset';
      }
      if (levels == 1) {
        return 'project';
      }

      return 'table_or_routine';
    })();
    const newSQL = fixDestinationSQL(parser, nsType, ns, sql);
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
  namespaceType: 'table_or_routine' | 'dataset' | 'project',
  namespace: string,
  sql: string,
): string {
  let newSQL = sql;
  let tree = parser.parse(sql);

  const _visit = function*(node: any): any {
    yield node;
    for (let n of node.children) {
      for (let c of _visit(n)) {
        yield c;
      }
    }
  };

  const _cleanup = (n: string) => n.trim().replace(/`/g, '');

  let _iteration = 0;
  let _stop = false;

  const dests = extractDestinations(sql);
  type Flag = {
    has_create: boolean;
    has_drop: boolean;
    has_dml: boolean;
    has_create_after_drop: boolean;
    has_drop_after_create: boolean;
  };

  const checks = dests.reduce(
    (acc, [n, resourceType, stmtType]) => {
      const d = acc.get(_cleanup(n)) ?? {
        has_create: false,
        has_drop: false,
        has_dml: false,
        has_create_after_drop: false,
        has_drop_after_create: false,
      };

      if (resourceType.startsWith('TEMPORARY')) {
        return acc;
      }

      if (stmtType == 'DDL_CREATE') {
        if (d.has_drop) {
          d.has_create_after_drop = true;
        }
        d.has_create = true;
      } else if (stmtType == 'DDL_DROP') {
        if (d.has_create) {
          d.has_drop_after_create = true;
        }
        d.has_drop = true;
      } else if (stmtType == 'DML') {
        d.has_dml = true;
      }
      acc.set(_cleanup(n), d);
      return acc;
    },
    new Map<string, Flag>(),
  );
  const qualify = (n: string) => {
    const flags = checks.get(_cleanup(n));
    if (!flags) return false;
    if (flags.has_drop_after_create) {
      return false;
    }
    if (flags.has_create) {
      return true;
    }
    return false;
  };

  const willReplaceIdentifier: Map<string, string> = new Map<string, string>(
    (() => {
      const qualifiedDDL = dests
        .filter(([n]) => {
          if (!qualify(_cleanup(n))) {
            return false;
          }

          const ancestors = _cleanup(n).split('.');
          while (ancestors.length > 1) {
            ancestors.pop();
            const pFlag = checks.get(ancestors.join('.'));
            if (pFlag?.has_drop_after_create ?? false) {
              return false;
            }
          }
          return true;
        });

      if (qualifiedDDL.length > 0) {
        const [ddl] = qualifiedDDL.at(0) ?? [];
        if (!ddl) {
          return [];
        }
        return [[_cleanup(ddl), `\`${namespace}\``]];
      }

      const refs = extractRefenrences(sql);
      const candidates = refs
        .map((n) => {
          if (!n) {
            return undefined;
          }

          if (namespaceType == 'dataset') {
            return undefined;
            // return _cleanup(n).split('.').at(-2);
          }

          if (
            namespaceType == 'table_or_routine' &&
            namespace.split('.').at(-1) === _cleanup(n).split('.').at(-1)
          ) {
            return _cleanup(n);
          }

          return undefined;
        });

      const candidate = candidates.filter((n) => n !== undefined)[0];

      if (!candidate) {
        return [];
      }

      return refs
        .map((n) => ({
          before: _cleanup(n),
          after: _cleanup(n).replaceAll(_cleanup(candidate), namespace),
        }))
        .filter(({ before, after }) => before !== after)
        .map(({ before, after }) => [before, after]);
    })(),
  );

  while (!_stop && _iteration < 100) {
    _stop = true;
    const row2count = newSQL.split('\n').map((r) => r.length)
      .reduce((ret, r) => {
        // Sum of ret;
        ret.push((ret[ret.length - 1] ?? 0) + r + 1);
        return ret;
      }, [0] as number[]);

    for (const n of _visit(tree.rootNode)) {
      if (n.type !== 'identifier') {
        continue;
      }

      /*
      * Rule #99: Replaced remaining identifer that replaced by other rules.
      */
      if (
        willReplaceIdentifier.has(_cleanup(n.text))
      ) {
        const replacement = willReplaceIdentifier.get(
          _cleanup(n.text),
        );
        if (!replacement) continue;
        const desired = `\`${_cleanup(replacement)}\``;

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
