CREATE OR REPLACE TABLE FUNCTION sandbox.sample_table(argument INT64) RETURNS TABLE<a INT64>
AS
(
select argument as a
);