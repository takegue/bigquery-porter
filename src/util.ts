import * as fs from 'fs';
import * as path from 'path';

export async function walk(dir: string): Promise<string[]> {  const fs_files = await fs.promises.readdir(dir);
  const files: string[][] = await Promise.all(fs_files.map(async (file) => {
    const filePath = path.join(dir, file);
    const stats = await fs.promises.stat(filePath);
    if (stats.isDirectory()) {
        return walk(filePath);
    }
    else if (stats.isFile()) {
        return [filePath];
    }
    return []
  }));

  return files.reduce(
    (all: string[], folder_contents) => {
        return all.concat(folder_contents)
    },
    [] as string[],
  ) as string[];
}

export type Relation = [string, string];
export function topologicalSort(relations: [string, string][]) {
  const [E, G, N] = relations.reduce((
      [E, G, N]: [Map<string, number>, Map<string, Set<string>>, Set<string>],
      [src, dst]: [string, string]
  ) => {
    E.set(src, 1 + (E.get(src) ?? 0));
    G.set(dst, new Set([src, ...G.get(dst) ?? []]));
    N.add(dst);
    N.add(src);
    return [E, G, N];
  }, [new Map<string, number>(), new Map<string, Set<string>>(), new Set<string>()]);

  let S = [...N].filter((n) => (E.get(n) ?? 0) === 0);
  let L = new Set<string>();

  S.forEach((n) => N.delete(n));
  while (S.length > 0) {
    const tgt: string | undefined = S.pop();
    if(!tgt) {
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

  return [...L];
}
