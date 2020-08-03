{
function selected_cols()
    print("c_int, c_double, c_decimal, c_string, c_datetime, c_enum, c_set")
end
}

query:
	basic_read
|	update_select
|	insert_delete

basic_read:
	select { selected_cols() } from t where c_int = __c_int__
|	select { selected_cols() } from t where c_int in (__c_int__, __c_int__, __c_int__) order by c_int, c_string

update_select: { key = __c_int__(); }
	update t set c_double = __c_double__ where c_int = { print(key); };
	select { selected_cols() } from t where c_int = { print(key); }

insert_delete: { key = __c_int__(); }
	insert into t values ({ print(key); }, __c_double__, __c_decimal__, __c_string__, __c_datetime__, __c_timestamp__, __c_enum__, __c_set__, __c_json__);
	delete from t where c_int = { print(key); }
