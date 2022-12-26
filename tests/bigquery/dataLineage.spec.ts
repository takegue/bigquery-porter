import { describe, it } from 'vitest';

import { DataLineage } from '../../src/dataLineage.js';

describe('dataLineageAPI', () => {
  it('client test', async () => {
    const client = new DataLineage();
    console.dir(await client.getSearchLinks(), { depth: null });
  });
});
