create table if not exists sandbox.sample_sharding_20220101
as
select DATE '2022-01-01' as date_jst;

FOR record in (
  select * from unnest(generate_date_array('2022-01-01', '2022-01-07')) as d
) do
  execute immediate format("""
    create table if not exists sandbox.sample_sharding_%s
    as
    select @date as date_jst
  """
  , format_date('%Y%m%d', record.d)
  ) using record.d as date
  ;
end FOR;

select * from `sandbox.demo_shardings_*`;
