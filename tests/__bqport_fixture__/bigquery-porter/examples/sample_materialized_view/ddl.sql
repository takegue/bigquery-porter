CREATE MATERIALIZED VIEW IF NOT EXISTS  `examples.sample_materialized_view`
AS select 1 as a from `bigquery-porter.examples.sample_table`;