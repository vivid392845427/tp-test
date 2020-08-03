{
function selected_cols()
    print("c_int, c_double, c_decimal, c_string, c_datetime, c_enum, c_set")
end
}

query:
    random_ops
|   update_select
|   insert_delete

random_op: common_read | common_write
random_ops: random_op | random_op; random_ops

common_read:
    select { selected_cols() } from t where c_int = __c_int__
|   select { selected_cols() } from t where c_int in (__c_int__, __c_int__, __c_int__)
|   select { selected_cols() } from t where c_int between { print(math.random(5)); } and { print(5+math.random(5)); }
|   select { selected_cols() } from t where c_string = __c_int__
|   select { selected_cols() } from t where c_decimal < 20
|   select sum(c_int) from t where c_datetime < __c_datetime__

common_write:
    common_update
|   common_insert
|   common_delete

common_update:
    update t set c_string = __c_string__ where c_int = __c_int__
|   update t set c_decimal = c_decimal - 5 where c_int in (__c_int__, __c_int__, __c_int__)
|   update t set c_decimal = c_decimal + 5 where c_decimal <= 20

common_insert:
    insert ignore into t values (__c_int__, __c_double__, __c_decimal__, __c_string__, __c_datetime__, __c_timestamp__, __c_enum__, __c_set__, __c_json__)
|   insert into t values (__c_int__, __c_double__, __c_decimal__, __c_string__, __c_datetime__, __c_timestamp__, __c_enum__, __c_set__, __c_json__)
|   insert into t values (__c_int__, __c_double__, __c_decimal__, __c_string__, __c_datetime__, __c_timestamp__, __c_enum__, __c_set__, __c_json__)
    on duplicate key update c_int=values(c_int), c_double=values(c_double), c_decimal=values(c_decimal), c_string=values(c_string), c_datetime=values(c_datetime), c_timestamp=values(c_timestamp), c_enum=values(c_enum), c_set=values(c_set), c_json=values(c_json)

common_delete:
    delete from t where c_int = __c_int__
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
