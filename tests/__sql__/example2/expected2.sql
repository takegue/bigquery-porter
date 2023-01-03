-- Unit test
begin
  -- dataset for test
  create schema if not exists `tmp_dataset`;
  call `other_dataset.init`();

  -- reference
  call `other_dataset.awesome_procedure`((null, 'tmp_dataset'));

  -- dataset for test
  drop schema if exists `tmp_dataset` CASCADE;
end
