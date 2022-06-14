CREATE MATERIALIZED VIEW IF NOT EXISTS  `project-id-7288898082930342315.sandbox.sample_materialized_view`
OPTIONS(
  labels=[("bqlunchpad-versionhash", "1654041193-undefined")]
)
AS select 1 as a from `sandbox.sample_table`;