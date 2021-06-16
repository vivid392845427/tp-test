{

    local util = require("util")

    T = {
        c_int = { seq = util.seq() },
        c_str = {},
        c_datetime = { range = util.range(1577836800, 1593561599) },
        c_timestamp = { range = util.range(1577836800, 1593561599) },
        c_double = { range = util.range(100) },
        c_decimal = { range = util.range(10) },
        
        collations = {'character set utf8mb4 collate utf8mb4_general_ci',
                      'character set utf8mb4 collate utf8mb4_unicode_ci',
                      'character set utf8mb4 collate utf8mb4_bin',
                      'character set utf8 collate utf8_bin',
                      'character set utf8 collate utf8_general_ci',
                      'character set utf8 collate utf8_unicode_ci',
                      'character set binary collate binary',
                      'character set ascii collate ascii_bin',
                      'character set latin1 collate latin1_bin'
                      },
        c_str_len = {range = util.range(1, 40)},
        enums_set = {'blue', 'green', 'red', 'yellow', 'white', 'orange', 'purple'},
    }

    T.c_int.rand = function() return T.c_int.seq:rand() end
    T.c_str.rand = function() return random_name() end
    T.c_datetime.rand = function() return T.c_datetime.range:randt() end
    T.c_timestamp.rand = function() return T.c_timestamp.range:randt() end
    T.c_double.rand = function() return T.c_double.range:randf() end
    T.c_decimal.rand = function() return T.c_decimal.range:randf() end
    
    T.rand_collation = function() return util.choice(T.collations) end
    T.c_str_len.rand = function() return T.c_str_len.range:randi() end

    T.get_enum_values = function() local enum_values = "" for k,v in ipairs(T.enums_set) do
                                         if (k==7) then
                                             enum_values=enum_values.."'"..v.."'"
                                         else
                                             enum_values=enum_values.."'"..v.."',"
                                         end
                                    end
                                    return enum_values
                        end
    T.rand_c_enum = function() return util.choice(T.enums_set) end
}

init: create_table; create_view; insert_data

txn: rand_queries

create_table:
    create table t (
        c_int int,
        c_str varchar(40) rand_collation,
        v_str varchar(40) as (sub_str) virtual,
        s_str varchar(40) as (sub_str) stored,            
        c_datetime datetime,
        c_timestamp timestamp,
        c_double double,
        c_decimal decimal(12, 6),
        c_enum enum(enums_values),
        c_set set (enums_values)
        key_primary
        key_c_int
        key_c_str
        key_c_decimal
        key_c_datetime
        key_c_timestamp
        key_c_enum
        key_c_set
    )
    
create_view:
    create view v as select * from t
    
sub_str:
    SUBSTR(c_str, 1, generated_len)

key_primary:
 |  , primary key(c_int)
 |  , primary key(c_str)
 |  , primary key(c_int, c_str)
 |  , primary key(c_str(prefix_idx_len))
 |  , primary key(c_int, c_str(prefix_idx_len))
 |  , primary key(c_int, c_enum)
 |  , primary key(c_int, c_set)
 
prefix_idx_len: { print(T.c_str_len.rand()) }

generated_len: { print(T.c_str_len.rand()) }

rand_collation: { print(T.rand_collation()) }

enums_values: {print(T.get_enum_values())}

key_c_int:                       
 |  , key(c_int)
 |  , unique key(c_int)

key_c_str:
 |  , key(c_str)
 |  , unique key(c_str)
 |  , key(c_str(prefix_idx_len))
 |  , unique key(c_str(prefix_idx_len))

key_c_decimal:
 |  , key(c_decimal)
 |  , unique key(c_decimal)

key_c_datetime:
 |  , key(c_datetime)
 |  , unique key(c_datetime)

key_c_timestamp:
 |  , key(c_timestamp)
 |  , unique key(c_timestamp) 
 
key_generated_column:
 |  , key(v_str)
 |  , unique key(v_str)
 |  , key(s_str)
 |  , unique key(s_str)
 
key_c_enum:
 |  , key(c_enum)
 
key_c_set:
 |  , key(c_set)

col_list: c_int, c_str, c_datetime, c_timestamp, c_double, c_decimal, c_enum, c_set

insert_data:
    insert into t(col_list) values next_row, next_row, next_row, next_row, next_row;
    insert into t(col_list) values next_row, next_row, next_row, next_row, next_row;
    insert into t(col_list) values next_row, next_row, next_row, next_row, next_row;

next_row: (next_c_int, rand_c_str, rand_c_datetime, rand_c_timestamp, rand_c_double, rand_c_decimal, rand_c_enum, rand_c_enum)
rand_row: (rand_c_int, rand_c_str, rand_c_datetime, rand_c_timestamp, rand_c_double, rand_c_decimal, rand_c_enum, rand_c_enum)

next_c_int: { print(T.c_int.seq:next()) }
rand_c_int: { print(T.c_int.rand()) }
rand_c_str: { printf("'%s'", T.c_str.rand()) }
rand_c_str_or_null: rand_c_str | [weight=0.2] null
rand_c_datetime: { printf("'%s'", T.c_datetime.rand()) }
rand_c_timestamp: { printf("'%s'", T.c_timestamp.rand()) }
rand_c_double: { printf("%.6f", T.c_double.rand()) }
rand_c_decimal: { printf("%.3f", T.c_decimal.rand()) }
rand_c_enum: { printf("'%s'", T.rand_c_enum()) }

union_or_union_all: union | union all
insert_or_replace: insert | replace

rand_queries:
    rand_query; rand_query; rand_query; rand_query
 |  [weight=9] rand_query; rand_queries

rand_query:
    [weight=0.3] common_select maybe_for_update
 |  [weight=0.2] (common_select maybe_for_update) union_or_union_all (common_select maybe_for_update)
 |  [weight=0.3] agg_select maybe_for_update
 |  [weight=0.2] (agg_select maybe_for_update) union_or_union_all (agg_select maybe_for_update)
 |  [weight=0.5] common_insert
 |  common_update
 |  common_delete
 |  common_update; common_delete; common_select
 |  common_insert; common_delete; common_select
 |  common_delete; common_insert; common_update

maybe_for_update: | for update
maybe_write_limit: | [weight=2] order by c_int, c_str, c_double, c_decimal limit { print(math.random(3)) }
  
enum_order:
  |  order by c_int, c_str, c_double, c_decimal, c_enum limit { print(math.random(3)) }
  
set_order:
  |  order by c_int, c_str, c_double, c_decimal, c_set limit { print(math.random(3)) }


common_select:
    select col_list from t where c_int = rand_c_int
 |  select col_list from t where c_int in (rand_c_int, rand_c_int, rand_c_int)
 |  select col_list from t where c_int between { k = T.c_int.rand(); print(k) } and { print(k+3) }
 |  select col_list from t where c_str = rand_c_str
 |  select col_list from t where v_str = sub_str
 |  select col_list from t where s_str = sub_str
 |  select col_list from t where c_decimal < { local r = T.c_decimal.range; print((r.max-r.min)/2+r.min) }
 |  select col_list from t where c_datetime > rand_c_datetime
 |  select col_list from t where c_enum = rand_c_enum
 |  select col_list from t where c_enum in (rand_c_enum, rand_c_enum, rand_c_enum, rand_c_enum) enum_order
 |  select col_list from t where c_set = rand_c_enum
 |  select col_list from t where c_set in (rand_c_enum, rand_c_enum, rand_c_enum, rand_c_enum) set_order
 |  select col_list from v where c_int = rand_c_int
 |  select col_list from v where c_int in (rand_c_int, rand_c_int, rand_c_int)
 |  select col_list from v where c_int between { k = T.c_int.rand(); print(k) } and { print(k+3) }
 |  select col_list from v where c_str = rand_c_str
 |  select col_list from v where v_str = sub_str
 |  select col_list from v where s_str = sub_str
 |  select col_list from v where c_decimal < { local r = T.c_decimal.range; print((r.max-r.min)/2+r.min) }
 |  select col_list from v where c_datetime > rand_c_datetime

agg_select:
    select count(*) from t where c_timestamp between { t = T.c_timestamp.rand(); printf("'%s'", t) } and date_add({ printf("'%s'", t) }, interval 15 day)
 |  select sum(c_int) from t where c_datetime between { t = T.c_datetime.rand(); printf("'%s'", t) } and date_add({ printf("'%s'", t) }, interval 15 day)
 |  select count(*) from v where c_timestamp between { t = T.c_timestamp.rand(); printf("'%s'", t) } and date_add({ printf("'%s'", t) }, interval 15 day)
 |  select sum(c_int) from v where c_datetime between { t = T.c_datetime.rand(); printf("'%s'", t) } and date_add({ printf("'%s'", t) }, interval 15 day)
 |  select count(*) from t where c_enum between rand_c_enum and rand_c_enum
 |  select count(*) from t where c_set between rand_c_enum and rand_c_enum

common_update:
    update t set c_str = rand_c_str where c_int = rand_c_int
 |  update t set c_enum = rand_c_enum where c_int = rand_c_int
 |  update t set c_set = rand_c_enum where c_int = rand_c_int
 |  update t set c_double = c_decimal, c_decimal = rand_c_decimal where c_int in (rand_c_int, rand_c_int, rand_c_int)
 |  update t set c_datetime = c_timestamp, c_timestamp = rand_c_timestamp where c_str in (rand_c_str_or_null, rand_c_str_or_null, rand_c_str_or_null)
 |  update t set c_int = c_int + 10, c_str = rand_c_str where c_int in (rand_c_int, { local k = T.c_int.seq:head(); print(k-2) })
 |  update t set c_int = c_int + 5, c_str = rand_c_str_or_null where (c_int, c_str) in ((rand_c_int, rand_c_str), (rand_c_int, rand_c_str), (rand_c_int, rand_c_str))
 |  [weight=0.4] update t set c_datetime = rand_c_datetime, c_timestamp = rand_c_timestamp, c_double = rand_c_double, c_decimal = rand_c_decimal where c_datetime is null maybe_write_limit
 |  [weight=0.4] update t set c_datetime = rand_c_datetime, c_timestamp = rand_c_timestamp, c_double = rand_c_double, c_decimal = rand_c_decimal where c_decimal is null maybe_write_limit

common_insert:
    insert into t(col_list) values next_row
 |  [weight=0.5] insert_or_replace into t(col_list) values next_row, next_row, ({ print(T.c_int.seq:head()-1) }, rand_c_str, rand_c_datetime, rand_c_timestamp, rand_c_double, rand_c_decimal, rand_c_enum, rand_c_enum)
 |  insert_or_replace into t (c_int, c_str, c_datetime, c_double, c_enum, c_set) values (rand_c_int, rand_c_str, rand_c_datetime, rand_c_double, rand_c_enum, rand_c_enum)
 |  insert_or_replace into t (c_int, c_str, c_timestamp, c_decimal, c_enum, c_set) values (next_c_int, rand_c_str, rand_c_timestamp, rand_c_decimal, rand_c_enum, rand_c_enum), (rand_c_int, rand_c_str, rand_c_timestamp, rand_c_decimal, rand_c_enum, rand_c_enum)
 |  insert into t(col_list) values rand_row, rand_row, next_row on duplicate key update c_int=values(c_int), c_str=values(c_str), c_enum=values(c_enum), c_set=values(c_set), c_double=values(c_double), c_timestamp=values(c_timestamp)
 |  insert into t(col_list) values rand_row, rand_row, next_row on duplicate key update c_int = c_int + 1, c_str = concat(c_int, ':', c_str)

common_delete:
    delete from t where c_int = rand_c_int
 |  delete from t where c_int in ({ local k = T.c_int.seq:head(); print(k-2) }, rand_c_int) or c_str in (rand_c_str, rand_c_str, rand_c_str, rand_c_str) maybe_write_limit
 |  delete from t where c_int in ({ local k = T.c_int.seq:head(); print(k-2) }, rand_c_int) or c_enum in (rand_c_enum, rand_c_enum, rand_c_enum, rand_c_enum) maybe_write_limit
 |  delete from t where c_int in ({ local k = T.c_int.seq:head(); print(k-2) }, rand_c_int) or c_set in (rand_c_enum, rand_c_enum, rand_c_enum, rand_c_enum) maybe_write_limit
 |  delete from t where c_str is null
 |  delete from t where v_str is null
 |  delete from t where s_str is null
 |  delete from t where c_decimal > c_double/2 maybe_write_limit
 |  [weight=0.8] delete from t where c_timestamp is null or c_double is null maybe_write_limit
