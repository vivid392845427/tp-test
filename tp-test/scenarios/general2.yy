{

    util = require("util")

    G = {
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

    G.c_int.rand = function() return G.c_int.seq:rand() end
    G.c_str.rand = function() return util.quota(random_name()) end
    G.c_datetime.rand = function() return util.quota(G.c_datetime.range:randt()) end
    G.c_timestamp.rand = function() return util.quota(G.c_timestamp.range:randt()) end
    G.c_double.rand = function() return sprintf('%.6f', G.c_double.range:randf()) end
    G.c_decimal.rand = function() return sprintf('%.3f', G.c_decimal.range:randf()) end
    G.rand_collation = function() return util.choice(G.collations) end
    G.c_str_len.rand = function() return G.c_str_len.range:randi() end

    G.get_enum_values = function() local enum_values = "" for k,v in ipairs(G.enums_set) do
                                         if (k==7) then
                                             enum_values=enum_values.."'"..v.."'"
                                         else
                                             enum_values=enum_values.."'"..v.."',"
                                         end
                                    end
                                    return enum_values
                        end
    G.rand_c_enum = function() return util.quota(util.choice(G.enums_set)) end

    T = {
        cols = {},
        cur_col = nil,
    }

    T.cols[#T.cols+1] = util.col('c_int', G.c_int.rand)
    T.cols[#T.cols+1] = util.col('c_str', G.c_str.rand)
    T.cols[#T.cols+1] = util.col('c_datetime', G.c_datetime.rand)
    T.cols[#T.cols+1] = util.col('c_timestamp', G.c_timestamp.rand)
    T.cols[#T.cols+1] = util.col('c_double', G.c_double.rand)
    T.cols[#T.cols+1] = util.col('c_decimal', G.c_decimal.rand)
    T.cols[#T.cols+1] = util.col('c_enum', G.rand_c_enum)
    T.cols[#T.cols+1] = util.col('c_set', G.rand_c_enum)

    T.col_int_str = util.col('(c_int, c_str)', function() return sprintf("(%d, %s)", G.c_int.rand(), G.c_str.rand()) end)

    T.rand_col = function()
        return util.choice(T.cols)
    end
    T.get_col = function(name)
        for _, c in ipairs(T.cols) do
            if c.name == name then
                return c
            end
        end
        return T.rand_col()
    end

}

init: drop table if exists t; create_table; insert_data

txn: begin; rand_queries; commit

create_table:
    create table t (
        c_int int,
        c_str varchar(40) rand_collation,
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

key_primary:
 |  , primary key(c_int)
 |  , primary key(c_str)
 |  , primary key(c_str(prefix_idx_len))
 |  , primary key(c_int, c_str)
 |  , primary key(c_str, c_int)
 |  , primary key(c_int, c_str(prefix_idx_len))
 |  , primary key(c_str(prefix_idx_len), c_int)
 |  , primary key(c_int, c_enum)
 |  , primary key(c_int, c_set)
 
prefix_idx_len: { print(G.c_str_len.rand()) }

rand_collation: { print(G.rand_collation()) }

enums_values: {print(G.get_enum_values())}

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
 |  [weight=0.5] , unique key(c_decimal)

key_c_datetime:
 |  , key(c_datetime)
 |  [weight=0.2] , unique key(c_datetime)

key_c_timestamp:
 |  , key(c_timestamp)
 |  [weight=0.2] , unique key(c_timestamp)
 
key_c_enum:
 |  , key(c_enum)
 
key_c_set:
 |  , key(c_set)


insert_data:
    insert into t values next_row, next_row, next_row, next_row, next_row;
    insert into t values next_row, next_row, next_row, next_row, next_row

next_row: (next_c_int, rand_c_str, rand_c_datetime, rand_c_timestamp, rand_c_double, rand_c_decimal, rand_c_enum, rand_c_set)
rand_row: (rand_c_int, rand_c_str, rand_c_datetime, rand_c_timestamp, rand_c_double, rand_c_decimal, rand_c_enum, rand_c_set)

next_c_int: { print(G.c_int.seq:next()) }
rand_c_int: { T.get_col('c_int'):pval() }
rand_c_str: { T.get_col('c_str'):pval() }
rand_c_enum: {T.get_col('c_enum'):pval() }
rand_c_set: {T.get_col('c_set'):pval() }
rand_c_datetime: { T.get_col('c_datetime'):pval() }
rand_c_timestamp: { T.get_col('c_timestamp'):pval() }
rand_c_double: { T.get_col('c_double'):pval() }
rand_c_decimal: { T.get_col('c_decimal'):pval() }
rand_col_val: { T.cur_col:pval() }
rand_col_vals: rand_col_val | rand_col_val, rand_col_vals

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

rand_cmp: < | > | >= | <= | <> | = | !=
rand_logic: or | and
rand_arithmetic: + | - | * | /
rand_strfunc: upper | lower | reverse | to_base64

is_null_or_not: is null | is not null
union_or_union_all: union | union all
insert_or_replace: insert | replace

maybe_for_update: | for update
maybe_write_limit: order by c_int, c_str, c_decimal, c_double | [weight=2] order by c_int, c_str, c_decimal, c_double limit { print(math.random(3)) }

selected_cols: c_int, c_str, c_double, c_decimal, c_datetime, c_timestamp, c_enum, c_set

predicates: [weight=2] predicate | predicate rand_logic predicates

predicate:
    { T.cur_col = T.rand_col(); print(T.cur_col.name) } = rand_col_val
 |  { T.cur_col = T.rand_col(); print(T.cur_col.name) } in (rand_col_vals)
 |  { T.cur_col = T.col_int_str; print(T.cur_col.name) } = rand_col_val
 |  { T.cur_col = T.col_int_str; print(T.cur_col.name) } in (rand_col_vals)
 |  { T.cur_col = T.rand_col(); print(T.cur_col.name) } rand_cmp rand_col_val
 |  { T.cur_col = T.rand_col(); print(T.cur_col.name) } between { local v1, v2 = T.cur_col:val(), T.cur_col:val(); if v1 > v2 then v1, v2 = v2, v1 end; printf("%v and %v", v1, v2) }
 |  { print(util.choice({'c_decimal', 'c_double', 'c_datetime', 'c_timestamp'})) } is_null_or_not

common_select:
    select selected_cols from t where predicate maybe_write_limit
 |  select selected_cols from t where predicates maybe_write_limit

agg_select:
    select count(*) from t where predicates
 |  select sum(c_int) from t where predicates

assignments: [weight=3] assignment | assignment, assignments

assignment:
    { T.cur_col = T.rand_col(); print(T.cur_col.name) } = rand_col_val
 |  [weight=0.3] { T.cur_col = T.get_col(util.choice({'c_int', 'c_decimal', 'c_double'})); print(T.cur_col.name) } = { print(T.cur_col.name) } rand_arithmetic { T.cur_col:pval() }
 |  [weight=0.1] c_str = rand_strfunc(c_str)

common_update:
    update t set assignment where predicates maybe_write_limit

rows_to_ins: [weight=4] row_to_ins | row_to_ins, rows_to_ins

row_to_ins:
    next_row
 |  [weight=0.4] rand_row
 |  [weight=0.4] ({ print(G.c_int.seq:head()-math.random(3)) }, rand_c_str, rand_c_datetime, rand_c_timestamp, rand_c_double, rand_c_decimal, rand_c_enum, rand_c_set)

on_dup_assignments: [weight=3] on_dup_assignment | on_dup_assignment, on_dup_assignments

on_dup_assignment:
    assignment
 |  { T.cur_col = T.rand_col(); print(T.cur_col.name) } = values({ print(T.cur_col.name) })

common_insert:
    insert_or_replace into t values rows_to_ins
 |  insert_or_replace into t (c_int, c_str, c_datetime, c_double, c_enum, c_set) values (rand_c_int, rand_c_str, rand_c_datetime, rand_c_double, rand_c_enum, rand_c_set)
 |  insert_or_replace into t (c_int, c_str, c_timestamp, c_decimal, c_enum, c_set) values (next_c_int, rand_c_str, rand_c_timestamp, rand_c_decimal, rand_c_enum, rand_c_set), (rand_c_int, rand_c_str, rand_c_timestamp, rand_c_decimal, rand_c_enum, rand_c_set)
 |  insert into t values rows_to_ins on duplicate key update on_dup_assignments

common_delete:
    [weight=3] delete from t where predicates maybe_write_limit
 |  delete from t where c_int in ({ local k = G.c_int.seq:head(); print(k-math.random(3)) }, rand_c_int) maybe_write_limit
 |  delete from t where { print(util.choice({'c_int', 'c_str', 'c_decimal', 'c_double', 'c_datetime', 'c_timestamp', 'c_enum', 'c_set'})) } is null maybe_write_limit
