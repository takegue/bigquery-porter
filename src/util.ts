import * as fs from 'fs';
import * as path from 'path';

export async function walk(dir: string) {
  const fs_files = await fs.promises.readdir(dir);
  const files = await Promise.all(fs_files.map(async (file) => {
    const filePath = path.join(dir, file);
    const stats = await fs.promises.stat(filePath);
    if (stats.isDirectory()) return walk(filePath);
    else if (stats.isFile()) return filePath;
  }));

  return files.reduce(
    (all: Array<string>, folderContents: Array<string>) =>
      all.concat(folderContents),
    [],
  );
}

export type Relation = [string, string];
export function topologicalSort(relations: Array<[string, string]>) {
  const [E, G, N] = relations.reduce(([E, G, N], [src, dst]) => {
    E[src] = 1 + (E[src] ?? 0);
    G[dst] = new Set([src, ...G[dst] ?? []]);
    N.add(dst);
    N.add(src);
    return [E, G, N];
  }, [{}, {}, new Set<string>()]);

  let S = [...N].filter((n) => (E[n] ?? 0) === 0);
  let L = new Set<string>();

  S.forEach((n) => N.delete(n));
  while (S.length > 0) {
    const tgt: string = S.pop();
    L.add(tgt);

    (G[tgt] ?? []).forEach((n: string) => E[n] -= 1);
    S = [...N]
      .filter((n) => (E[n] ?? 0) <= 0)
      .reduce((ret, n) => {
        N.delete(n);
        return [n, ...ret];
      }, S);
  }

  return [...L];
}
