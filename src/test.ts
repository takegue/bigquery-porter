import { BigQuery } from '@google-cloud/bigquery';

const main = async () => {
  const bq = new BigQuery();
  const d = bq.dataset('test_listing');
  console.log(d);
  console.log('get', await d.get());
  console.log('getMetadata', await d.getMetadata());
};
await main();
