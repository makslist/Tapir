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