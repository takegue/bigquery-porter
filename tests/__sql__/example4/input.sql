begin
  create temp table `temp_table1`
  as select 1 as a from `project.sandbox.sample_table`
  ;

  select * from `temp_table1`
end
