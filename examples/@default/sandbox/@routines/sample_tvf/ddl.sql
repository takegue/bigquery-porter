CREATE OR REPLACE TABLE FUNCTION `sandbox.sample_tvf`(argument INT64) RETURNS TABLE<a INT64>
AS
(
select argument as a
);