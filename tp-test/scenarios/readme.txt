1、When the base column of the generated column has a binary character set, mysql will fail to create the table, but tidb will succeed---It may be a bug in mysql

2、set new_collations_enabled_on_first_bootstrap on or off, There is a difference in the handling of trailing spaces ---on:ignore trailing spaces as the same as mysql;off:Don't ignore trailing spaces 

3、There is a difference in the handling of swaping two column value---mysql uses the updated value,tidb uses the pre-update value,eg:
drop table if exists t;
create table t (a int, b int);
insert into t values (1, 10);
update t set a = a+1, b = a;
select * from t;---mysql：a=2,b=2; tidb: a=2,b=1
