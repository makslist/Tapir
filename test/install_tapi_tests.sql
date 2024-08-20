set serveroutput on

prompt GENERATE CRUD TAPI
begin
   tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$crud'),
                             proc_pipe                  => null,
                             create_bulk_procedures     => true,
                             create_occ_procedures      => false,
                             proc_json_obj              => null,
                             proc_of_json               => null,
                             record_default_expressions => tapir.mapping('T_VARCHAR2'                       => 'sys.dbms_random.string(''L'', round(sys.dbms_random.value(1, 100)))',
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

prompt GENERATE AUDIT TAPI
begin
   tapir.init(tapir.params_t(audit_user_exp          => '''me''',
                             audit_col_created_by    => 'created_by',
                             audit_col_created_date  => 'created_at',
                             audit_col_modified_by   => 'modified_by',
                             audit_col_modified_date => 'modified_at'));
   tapir.compile_tapi(p_table_name => 'TAPIR_AUDIT_COLS');
end;
/


prompt GENERATE JSON TAPI
begin
   tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$json'),
                             proc_pipe                  => null,
                             create_bulk_procedures     => true,
                             create_occ_procedures      => true,
                             record_default_expressions => tapir.mapping('T_VARCHAR2'                       => 'sys.dbms_random.string(''L'', round(sys.dbms_random.value(1, 100)))',
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
