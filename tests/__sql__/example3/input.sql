declare ret array<string>;
create schema if not exists `zpreview_test`;

create or replace table `zpreview_test.ref1`
partition by date_jst
as select date '2006-01-02' as date_jst

drop schema `zpreview_test` cascade;
