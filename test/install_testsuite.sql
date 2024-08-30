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

@test_tapir_generate.pks
/
@test_tapir_generate.pkb
/

drop table if exists test_table;
/
create table if not exists test_table(
   pk                               number not null,
   identity_column                  int generated always as identity,
   varchar2_t                       varchar2(100),
   char_t                           char,
   nchar_t                          nchar,
   nvarchar2_t                      nvarchar2(100),
   number_t                         number not null,
   float_t                          float,
   binary_float_t                   binary_float,
   binary_double_t                  binary_double,
   date_t                           date default sysdate,
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
   virtual_col                      integer generated always as (number_t * 2) virtual,
   created_by                       varchar2(100),
   created_at                       date,
   modified_by                      varchar2(100),
   modified_at                      timestamp,
   constraint test_table_pk primary key(pk) using index
)
/
create unique index if not exists test_table_unique_col on test_table(varchar2_t)
/
create index if not exists test_table_col_idx on test_table(number_t)
/

prompt GENERATE TEST TAPI PACKAGE
declare
    init_values tapir.mapping := tapir.mapping('VARCHAR2_T'                       => 'sys.dbms_random.string(''L'', round(sys.dbms_random.value(1, 100)))',
                                               'CHAR_T'                           => 'sys.dbms_random.string(''L'', 1)',
                                               'NCHAR_T'                          => 'sys.dbms_random.string(''L'', 1)',
                                               'NVARCHAR2_T'                      => 'sys.dbms_random.string(''L'', round(sys.dbms_random.value(1, 100)))',
                                               'NUMBER_T'                         => 'round(sys.dbms_random.value(1, 1024 * 1024))',
                                               'FLOAT_T'                          => 'to_number(2)',
                                               'BINARY_FLOAT_T'                   => 'round(sys.dbms_random.value(1, 1024 * 1024))',
                                               'BINARY_DOUBLE_T'                  => 'round(sys.dbms_random.value(1, 1024 * 1024))',
                                               'DATE_T'                           => 'sysdate',
                                               'TIMESTAMP_T'                      => 'systimestamp',
                                               'TIMESTAMP_WITH_LOCAL_TIME_ZONE_T' => 'systimestamp',
                                               'TIMESTAMP_WITH_TIME_ZONE_T'       => 'systimestamp',
                                               'INTERVAL_YEAR_TO_MONTH_T'         => '(systimestamp - to_date(''2024'', ''YYYY'')) year(9) to month',
                                               'INTERVAL_DAY_TO_SECOND_T'         => '(systimestamp - to_date(''2024'', ''YYYY'')) day(9) to second',
                                               'BLOB_T'                           => 'utl_raw.cast_to_raw(''blob'')',
                                               'CLOB_T'                           => 'substr(sys_guid(), 1, 20)',
                                               'NCLOB_T'                          => 'substr(sys_guid(), 1, 20)',
                                               'RAW_T'                            => 'utl_raw.cast_to_raw(''raw'')',
                                               'BOOL_T'                           => 'true',
                                               'ROWID_T'                          => '''1''');
begin
    tapir.init(tapir.params_t(tapi_name                           => tapir.mapping('^(.*)$' => '\1$tapi'),
                              proc_pipe                           => null,
                              create_occ_procedures               => true,
                              raise_error_on_failed_update_delete => true,
                              audit                               => tapir.audit_t(user_exp              => '''me''',
                                                                                   col_created_by        => 'created_by',
                                                                                   col_created_date      => 'created_at',
                                                                                   col_modified_by       => 'modified_by',
                                                                                   col_modified_date     => 'modified_at',
                                                                                   ignore_when_comparing => true),
                              defaults                            => tapir.defaults_t(init_record_expressions => init_values)));
    tapir.compile_tapi(p_table_name => 'test_table');
end;
/
@test_tapi.pks
/
@test_tapi.pkb
/