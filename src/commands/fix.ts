import * as fs from "node:fs";
import { BigQuery } from "@google-cloud/bigquery";
import Parser from "tree-sitter";
import Language from "tree-sitter-sql-bigquery";

import { path2bq } from "../../src/bigquery.js";
import {
  extractDestinations,
  extractRefenrences,
  walk,
} from "../../src/util.js";

const formatLocalfiles = async (
  rootPath: string,
  options?: { dryRun?: string },
) => {
  const files = (await walk(rootPath));

  const bqClient = new BigQuery();
  const parser = new Parser();
  parser.setLanguage(Language);

  const defaultProjectId = await bqClient.getProjectId();
  for (const file of files) {
    const sql: string = await fs.promises.readFile(file)
      .then((s: any) => s.toString());
    const bqId = path2bq(file, rootPath, defaultProjectId);
    const ns = bqId.split(".").slice(1).join(".");
    const nsType = (() => {
      const levels = bqId.split(".").length;
      if (levels == 3) {
        return "table_or_routine";
      }
      if (levels == 2) {
        return "dataset";
      }
      if (levels == 1) {
        return "project";
      }

      return "table_or_routine";
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
  namespaceType: "table_or_routine" | "dataset" | "project",
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

  const _detectDDLKind = function(node: any): any {
    if (node.parent == null) {
      return [false, undefined];
    }

    if (node.parent.type.match("create_table_statement")) {
      return [true, "TABLE"];
    }

    if (node.parent.type.match("create_schema_statement")) {
      return [true, "SCHEMA"];
    }

    if (node.parent.type.match("create_function_statement")) {
      return [true, "ROTUINE"];
    }

    if (node.parent.type.match("create_table_function_statement")) {
      return [true, "ROTUINE"];
    }

    if (node.parent.type.match("create_procedure_statement")) {
      return [true, "ROTUINE"];
    }

    return [false, undefined];
  };

  const _cleanupIdentifier = (n: string) => n.trim().replace(/`/g, "");

  let _iteration = 0;
  let _stop = false;
  let replacedIdentifier: Set<string> = new Set();

  // Pre-scan
  const isQualifiedIdentifier = (type: string) => {
    if (namespaceType == "dataset" && type == "SCHEMA") {
      return true;
    }

    if (
      namespaceType == "table_or_routine" && ["TABLE", "ROUTINE"].includes(type)
    ) {
      return true;
    }

    return false;
  };

  const hasDDL = extractDestinations(sql)
    .filter(([_, type]) => isQualifiedIdentifier(type))
    .length > 0;
  const Refs = extractRefenrences(sql);
  const namespaceMatchedIdentifier = (() => {
    // Sort `Refs` elements by similarity calculated by text mateced to namespace
    const tail = namespace.split(".").at(-1);
    const qualified = Refs
      // Filter out non-qualified identifiers
      .filter((n) => {
        if (!n) {
          return false;
        }

        if (tail === _cleanupIdentifier(n).split(".").at(-1)) {
          return true;
        }
        return false;
      });

    return qualified.at(0);
  })();

  while (!_stop && _iteration < 100) {
    _stop = true;
    const row2count = newSQL.split("\n").map((r) => r.length)
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
        n.type === "identifier" &&
        replacedIdentifier.size == 0 &&
        hasDDL && isDDL &&
        // Matching BigQuery Level
        (desired.split(".").length - n.text.split(".").length) ** 2 <= 1
      ) {
        // Memorize propagate modification
        replacedIdentifier.add(_cleanupIdentifier(n.text));

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
      * Rule #2: Replace identifier most close to namespace in fuzzy-match rules if and only if SQL has no DDL
      */
      if (
        n.type === "identifier" &&
        replacedIdentifier.size == 0 &&
        !hasDDL && n.text == namespaceMatchedIdentifier &&
        (desired.split(".").length - n.text.split(".").length) ** 2 <= 1
      ) {
        // Memorize propagate modification
        replacedIdentifier.add(_cleanupIdentifier(n.text));

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
      * Rule #99: Replaced remaining identifer that replaced by other rules.
      */
      if (
        n.type === "identifier" &&
        replacedIdentifier.has(_cleanupIdentifier(n.text))
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
