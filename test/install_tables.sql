drop table if exists test_table_all_types;
/
create table if not exists test_table_all_types(
   pk                               number not null,
   varchar2_t                       varchar2(100),
   char_t                           char,
   nchar_t                          nchar,
   nvarchar2_t                      nvarchar2(100),
   number_t                         number,
   float_t                          float,
   binary_float_t                   binary_float,
   binary_double_t                  binary_double,
   date_t                           date,
   timestamp_t                      timestamp,
   timestamp_with_local_time_zone_t timestamp with local time zone,
   timestamp_with_time_zone_t       timestamp with time zone,
   interval_year_to_month_t         interval year to month,
   interval_day_to_second_t         interval day to second,
   blob_t                           blob,
   clob_t                           clob,
   nclob_t                          nclob,
   --long_t                           long,
   raw_t                            raw(64),
   --long_raw_t                       long raw,
   bool_t                           boolean,
   --rowid_t                          rowid,
   created_by                       varchar2(100),
   created_at                       date,
   modified_by                      varchar2(100),
   modified_at                      timestamp,
   constraint test_table_all_types_pk primary key(pk) using index
)
/
create unique index if not exists test_table_all_types_varchar2_t_unique on test_table_all_types(varchar2_t)
/
create index if not exists test_table_all_types_number_t_idx on test_table_all_types(number_t)
/
drop table if exists test_table_non_lob_types;
/
create table if not exists test_table_non_lob_types(
   no_pk                            number not null,
   varchar2_t                       varchar2(100),
   char_t                           char,
   nchar_t                          nchar,
   nvarchar2_t                      nvarchar2(100),
   number_t                         number,
   float_t                          float,
   binary_float_t                   binary_float,
   binary_double_t                  binary_double,
   date_t                           date,
   timestamp_t                      timestamp,
   timestamp_with_local_time_zone_t timestamp with local time zone,
   timestamp_with_time_zone_t       timestamp with time zone,
   interval_year_to_month_t         interval year to month,
   interval_day_to_second_t         interval day to second,
   unique (no_pk) using index
)
/
drop table if exists test_table_no_pk;
/
create table if not exists test_table_no_pk(
   no_pk                            number not null
)
/

drop table if exists test_table_edge_case;
/
create table if not exists test_table_edge_case(
   pk                            number not null,
   test_table_edge_case          varchar2(100), -- special handling in record definition  
   varchar2_t                    varchar2(100) default 'null',
   number_t                      number,
   --rowid_t                       rowid,
   check (number_t in (1, 0)),
   constraint test_table_edge_case_pk primary key(pk) using index
)
/
