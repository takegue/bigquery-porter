create or replace procedure `sandbox.correct_name`(
)
begin
  create or replace temp table `table_labels`
  as select 1 as a
  ;
end;

-- Unit test
begin
  -- dataset for test
  create schema if not exists `tmp_dataset`;

  -- reference
  call `sandbox.correct_name`((null, 'tmp_dataset'))
  ;

  -- dataset for test
  drop schema if exists `tmp_dataset` CASCADE;
end
