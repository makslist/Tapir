drop table if exists tapir_all_types;
/
create table if not exists tapir_all_types(
   t_varchar2                       varchar2(100) not null,
   t_char                           char not null,
   t_nchar                          nchar not null,
   t_nvarchar2                      nvarchar2(100) not null,
   t_number                         number not null,
   t_float                          float not null,
   t_binary_float                   binary_float not null,
   t_binary_double                  binary_double not null,
   t_date                           date not null,
   t_timestamp                      timestamp not null,
   t_timestamp_with_local_time_zone timestamp with local time zone not null,
   t_timestamp_with_time_zone       timestamp with time zone not null,
   t_interval_year_to_month         interval year to month not null,
   t_interval_day_to_second         interval day to second not null,
   t_blob                           blob not null,
   t_clob                           clob not null,
   t_nclob                          nclob not null,
   --t_long                           long not null,
   t_raw                            raw(64) not null,
   --t_long_raw                       long raw not null,
   t_bool                           boolean not null,
   --t_rowid                          rowid not null,
   constraint tapir_all_types_pk primary key(t_varchar2) using index
)
/
begin
   tapir.init(tapir.params_t(proc_pipe                  => null,
                             custom_default_expressions => tapir.mapping('T_VARCHAR2'                       => 'sys.dbms_random.string(''L'', round(sys.dbms_random.value(1, 100)))',
                                                                         'T_CHAR'                           => 'sys.dbms_random.string(''L'', 1)',
                                                                         'T_NCHAR'                          => 'sys.dbms_random.string(''L'', 1)',
                                                                         'T_NVARCHAR2'                      => 'sys.dbms_random.string(''L'', round(sys.dbms_random.value(1, 100)))',
                                                                         'T_NUMBER'                         => 'round(sys.dbms_random.value(1, 1024 * 1024))',
                                                                         'T_FLOAT'                          => 'to_number(2)',
                                                                         'T_BINARY_FLOAT'                   => 'round(sys.dbms_random.value(1, 1024 * 1024))',
                                                                         'T_BINARY_DOUBLE'                  => 'round(sys.dbms_random.value(1, 1024 * 1024))',
                                                                         'T_DATE'                           => 'sysdate',
                                                                         'T_TIMESTAMP'                      => 'systimestamp',
                                                                         'T_TIMESTAMP_WITH_LOCAL_TIME_ZONE' => 'systimestamp',
                                                                         'T_TIMESTAMP_WITH_TIME_ZONE'       => 'systimestamp',
                                                                         'T_INTERVAL_YEAR_TO_MONTH'         => '(systimestamp - to_date(''2024'', ''YYYY'')) year(9) to month',
                                                                         'T_INTERVAL_DAY_TO_SECOND'         => '(systimestamp - to_date(''2024'', ''YYYY'')) day(9) to second',
                                                                         'T_BLOB'                           => 'utl_raw.cast_to_raw(''blob'')',
                                                                         'T_CLOB'                           => 'substr(sys_guid(), 1, 20)',
                                                                         'T_NCLOB'                          => 'substr(sys_guid(), 1, 20)',
                                                                         'T_RAW'                            => 'utl_raw.cast_to_raw(''raw'')',
                                                                         'T_BOOL'                           => 'true',
                                                                         'T_ROWID'                          => '''1''')));
   tapir.compile_tapi(p_table_name => 'tapir_all_types');
end;
/
