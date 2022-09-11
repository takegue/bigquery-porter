create or replace procedure `some_schema.awesome_procedure`(
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
  call `some_schema.awesome_procedure`((null, 'tmp_dataset'))
  ;

  -- dataset for test
  drop schema if exists `tmp_dataset` CASCADE;
end
