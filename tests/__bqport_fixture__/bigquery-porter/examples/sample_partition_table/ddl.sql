CREATE SCHEMA IF NOT EXISTS `examples_eu`
OPTIONS(
  location="eu"
);

CREATE TABLE IF NOT EXISTS `examples_eu.sample_table`
AS
SELECT 1 as a;
