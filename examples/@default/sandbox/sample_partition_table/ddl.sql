CREATE TABLE IF NOT EXISTS `sandbox.sample_partition_table`
PARTITION BY date_jst
AS
select date '2022-01-01' as date_jst
