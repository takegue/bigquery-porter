import type { BigQuery } from '@google-cloud/bigquery';

type Config = {
  root: string;
  dryRun?: boolean;
  force?: false;
};

export class BigQueryPorter {
  public constructor(
    public readonly config: Config,
    public readonly client: BigQuery,
  ) {}
}
