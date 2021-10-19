1、自动生成列的基础列的字符集为binary，mysql 创建表失败，tidb 创建表成功

2、new_collations_enabled_on_first_bootstrap=on及off时，对于后置空格处理不同会导致用例失败 ---on:忽略后置空格(处理同mysql);off:未忽略后置空格，具体可参考官网
   旧的字符集配置下如果使用了新的字符集则不生效，在使用字符串进行匹配时，结果会存在差异导致用例失败，暂时没有找到办法规避，目前只能人工确认

3、交换列值与mysql的处理存在差异，mysql使用更新后的值,tidb使用未更新的值，https://github.com/pingcap/tidb/issues/19137,例如:
drop table if exists t;
create table t (a int, b int);
insert into t values (1, 10);
update t set a = a+1, b = a;
select * from t;---mysql：a=2,b=2; tidb: a=2,b=1

4、执行multi-table.yy文件时，需手工开启配置项：set @@session.tidb_enable_list_partition = ON

5、在 update 语句中更新同一个字段2次，行为跟 mysql 未保持一致，https://github.com/pingcap/tidb/issues/28370,例如：
create table t(a int not null,b int);
insert into t values(1,1);
update t set a=2,a=a+1;---mysql 值为3，tidb 值为2

5、执行 multi-table-prepare.yy 文件时，mysql 在 all 子查询的情况下存在 bug 会导致比对不通过，例子:
create table t1  (c_int int, c_str varchar(40) character set latin1 collate latin1_bin, c_datetime datetime, c_timestamp timestamp, c_double double, c_decimal decimal(12, 6), c_enum enum('blue','green','red','yellow','white','orange','purple'), primary key (c_int, c_str)   , key(c_decimal)  , key(c_timestamp));
create table t2  like t1 ;
insert into t1 values(74,"happy hawking","2020-02-09 09:05:59",NULL,52.087467,NULL,'red');
insert into t2 values(9,"relaxed pike","2020-02-21 04:01:17","2020-06-16 07:29:04",67.851665,2.140000,"purple");
insert into t2 values(13,"magical feynman"  ,"2020-02-22 20:54:25","2020-01-17 12:44:09",91.760801,6.078000,"blue"  );
insert into t2 values(37,"sleepy poincare"  ,"2020-02-08 12:27:38","2020-05-24 17:20:53",92.522031,3.527000,"purple");
insert into t2 values(16,"bold lamport"     ,"2020-06-23 06:30:24","2020-03-30 14:18:39",23.914033,6.928000,"white" );
insert into t2 values(20,"dazzling joliot"  ,"2020-01-04 14:45:20","2020-04-23 15:40:17",29.495902,3.745000,"red"  );
insert into t2 values(24,"reverent mclaren" ,"2020-02-20 22:46:35","2020-02-05 10:34:07",73.501801,3.898000,"white" );
insert into t2 values(32,"practical leavitt","2020-06-30 16:44:03","2020-03-04 19:48:55",69.007064,6.101000,"red"   );
insert into t2 values( 6,"inspiring gould"  ,"2020-01-31 09:51:10","2020-05-08 12:51:29",49.691598,2.323000,"orange");
insert into t2 values(18,"18:musing cohen"  ,"2020-02-06 06:26:37","2020-03-07 22:16:40",61.623608,6.576000,"purple");
insert into t2 values(22,"nervous hodgkin"  ,"2020-06-23 07:23:00","2020-04-09 16:04:43", 46.96624,5.528000,"white" );
insert into t2 values(26,"hungry lamport"   ,"2020-05-24 13:13:09","2020-01-20 09:32:34",77.832095,6.648000,"orange");
insert into t2 values( 7,"kind mclean"      ,"2020-05-28 14:39:30","2020-04-04 13:56:23",    6.339,2.109000,"yellow");
insert into t2 values(15,"awesome feynman"  ,"2020-02-10 21:55:36","2020-03-13 12:42:44",92.327104,9.830000,"green" );
insert into t2 values(23,"reverent solomon" ,"2020-04-24 03:54:51","2020-04-09 11:34:48",41.917707,7.140000,"white" );
insert into t2 values(39,"pensive chaplygin","2020-01-26 12:56:59","2020-03-03 05:12:12",49.550909,9.598000,"white" );

prepare stmt1348 from "select * from t1 where c_timestamp = all (select c_timestamp from t2 where c_int = ? )  ";
/* 257:1709 */ begin;
/* 257:1710 */ set @v0 = 19; 
/* 257:1710 */ execute stmt1348 using @v0;
/* 257:1711 */ set @v0 = 14; 
/* 257:1711 */ execute stmt1348 using @v0;
/* 257:1712 */ update t1 set c_datetime = c_timestamp, c_timestamp = '2020-03-01 18:18:13' where c_str in ('ecstatic raman', null, 'sleepy bassi');
/* 257:1713 */ set @v0 = 20; 
/* 257:1713 */ execute stmt1348 using @v0; ---预期无记录返回，mysql 返回了c_int=74 的记录
/* 257:1714 */ commit;

