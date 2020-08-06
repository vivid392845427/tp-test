{
function selected_cols()
    print("c_int, c_double, c_decimal, c_string, c_datetime, c_timestamp, c_enum, c_set")
end
}

query:
    random_ops

random_ops: random_op | random_op; random_ops

random_op:
    all_read
|   common_write
|   update_select
|   insert_delete

maybe_for_update: | for update
maybe_order_by_limit: | order by c_int, c_string, c_timestamp asc_or_desc | order by c_int, c_string, c_timestamp asc_or_desc limit { print(1+math.random(3)) }

asc_or_desc: asc | desc
union_or_union_all: union | union all

null_or_rand_int: null | __c_int__ | __c_int__ | __c_int__
null_or_rand_str: null | __c_string__ | __c_string__ | __c_string__

rand_row:
    (__c_int__, __c_double__, __c_decimal__, __c_string__, __c_datetime__, __c_timestamp__, __c_enum__, __c_set__, __c_json__)
|   (__c_int__, __c_double__, __c_decimal__, null_or_rand_str, __c_datetime__, __c_timestamp__, __c_enum__, __c_set__, __c_json__)

rand_row_x:
    ({ print(20+math.random(5)) }, __c_double__, __c_decimal__, __c_string__, __c_datetime__, __c_timestamp__, __c_enum__, __c_set__, __c_json__)
|   ({ print(20+math.random(5)) }, __c_double__, __c_decimal__, null_or_rand_str, __c_datetime__, __c_timestamp__, __c_enum__, __c_set__, __c_json__)


rand_rows:
    rand_row, rand_row_x
|   rand_row, rand_row, rand_row
|   rand_row_x, rand_row_x, rand_row_x


all_read:
    common_read maybe_order_by_limit maybe_for_update
|   union_common_read maybe_order_by_limit
|   agg_read
|   union_agg_read

common_read:
    select { selected_cols() } from t where c_int = __c_int__
|   select { selected_cols() } from t where c_int is null
|   select { selected_cols() } from t where c_int in (__c_int__, __c_int__, __c_int__)
|   select { selected_cols() } from t where c_int in { k = __c_int__() } ({ print(k) }, __c_int__, { print(k) })
|   select { selected_cols() } from t where c_string = __c_string__
|   select { selected_cols() } from t where c_string is null
|   select { selected_cols() } from t where c_string in (__c_string__, __c_string__, __c_string__)
|   select { selected_cols() } from t where c_int between { print(math.random(5)) } and { print(5+math.random(5)) }
|   select { selected_cols() } from t where c_decimal < { print(math.random(32)) }
|   select { selected_cols() } from t where c_datetime > __c_datetime__

union_common_read: (common_read maybe_for_update) union_or_union_all (common_read maybe_for_update)

agg_read:
    { t = __c_timestamp__() } select count(*) from t where c_timestamp between { print(t) } and date_add({ print(t) }, interval 7 day)
|   { t = __c_datetime__() } select sum(c_int) from t where c_datetime between { print(t) } and date_add({ print(t) }, interval 3 day)

union_agg_read: (agg_read maybe_for_update) union_or_union_all (agg_read maybe_for_update)

common_write:
    common_update
|   common_update
|   common_insert
|   common_delete
|   common_insert; common_update; common_delete

common_update:
    update t set c_int = null_or_rand_int where c_int = __c_int__
|   update t set c_string = null_or_rand_str where c_int = __c_int__
|   update t set c_int = null_or_rand_int, c_string = null_or_rand_str where c_int = __c_int__ and c_string > __c_string__
|   update t set c_decimal = c_decimal - 5 where c_int in (__c_int__, __c_int__, null_or_rand_int)
|   update t set c_decimal = c_decimal - 5 where c_string in (__c_string__, null_or_rand_str, null_or_rand_str)
|   update t set c_decimal = c_decimal + 5 where c_decimal <= 20

common_insert:
    insert ignore into t values rand_row
|   insert into t values rand_row
|   insert into t values rand_row on duplicate key update c_int = __c_int__, c_string = __c_string__
|   insert into t values rand_rows on duplicate key update c_int = c_int + 1, c_string = concat(c_int, ':', c_string)
|   insert into t set c_int = __c_int__, c_double = __c_double__, c_decimal = __c_decimal__, c_string = __c_string__, c_datetime = __c_datetime__, c_timestamp = __c_timestamp__, c_enum = __c_enum__, c_set = __c_set__, c_json = __c_json__
|   replace into t values rand_row
|   replace into t values rand_rows

common_delete:
    delete from t where c_int = __c_int__
|   delete from t where c_int is null
|   delete from t where c_int in (__c_int__, __c_int__, null_or_rand_int)
|   delete from t where c_string is null
|   delete from t where c_string in (__c_string__, null_or_rand_str, null_or_rand_str)
|   delete from t where c_decimal < { print(math.random(20)) }

update_select:
    { key = __c_int__(); }
    update t set c_double = __c_double__ where c_int = { print(key); };
    select { selected_cols() } from t where c_int = { print(key); }
|   { key = __c_int__(); }
    update t set c_double = __c_double__ where c_int = { print(key); };
    random_ops;
    select { selected_cols() } from t where c_int = { print(key); }

insert_delete:
    { key = __c_int__(); }
    insert into t values ({ print(key); }, __c_double__, __c_decimal__, __c_string__, __c_datetime__, __c_timestamp__, __c_enum__, __c_set__, __c_json__);
    delete from t where c_int = { print(key); }
|   { key = __c_int__(); }
    insert into t values ({ print(key); }, __c_double__, __c_decimal__, __c_string__, __c_datetime__, __c_timestamp__, __c_enum__, __c_set__, __c_json__);
    random_ops;
    delete from t where c_int = { print(key); }
