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

6、执行 multi-table-prepare.yy 文件时，mysql 在 all 子查询的情况下存在 bug 会导致比对不通过, mysql issue:https://bugs.mysql.com/bug.php?id=106164

7、执行 multi-table-expression-index.yy 文件时，mysql 在 any 子查询的情况下存在 bug 会导致比对不通过，mysql issue: https://bugs.mysql.com/bug.php?id=106155

8、由于 mysql 不支持表达式：vitess_hash()，md5(), expression index 的 yy 文件没有增加这2个函数的表达式索引的测试
