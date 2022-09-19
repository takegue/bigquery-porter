create or replace function `v0.zgensql__partition_alignment2`(
  destination STRUCT<project_id STRING, dataset_id STRING, table_id STRING>
  , sources ARRAY<STRUCT<project_id STRING, dataset_id STRING, table_id STRING>>
  , partition_alignments ARRAY<STRUCT<destination STRING, sources ARRAY<STRING>>>
)
as
((
  with
    sql__information_schema as (
      select as value
          string_agg(
            format("""
              select
                '%s' as label
                , '%s' as argument
                , *
              from `%s.INFORMATION_SCHEMA.PARTITIONS`
              where %s
              """
              , label
              , target.table_id
              , coalesce(
                format('%s.%s', target.project_id, target.dataset_id)
                , target.dataset_id
                , error(format('Invalid target: %t', target))
              )
              , format(if(
                contains_substr(target.table_id, '*')
                , 'starts_with(table_name, replace("%s", "*", ""))'
                , 'table_name = "%s"'
                )
                , target.table_id)
            )
            , '\nunion all'
          )
      from unnest([
        struct('destination' as label, destination as target)
      ] || array(select as struct 'source', s from unnest(sources) s))
    )
    , ret as (
      select
        *
      from sql__information_schema
    )

    select * from ret
));

begin
  declare query string;

  create schema if not exists `zpreview_test__alignment`;

  create or replace table `zpreview_test__alignment.dest1`
  partition by date_jst
  as select date '2006-01-02' as date_jst
  ;

  create or replace table `zpreview_test__alignment.ref1`
  partition by date_jst
  as select date '2006-01-02' as date_jst
  ;

  set query = `v0.zgensql__partition_alignment`(
    (string(null), "zpreview_test__alignment", 'dest1')
    , [
      (string(null), "zpreview_test__alignment", 'ref1')
    ]
    , `v0.alignment__day2day`('2006-01-02', '2006-01-02')
  );

  execute immediate query;
  drop schema if exists `zpreview_test__alignment` cascade;
end
