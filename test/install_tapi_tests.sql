set serveroutput on

prompt GENERATE CRUD TAPI
begin
   tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => 'tapir_all_types$crud'),
                             proc_pipe                  => null,
                             create_bulk_procedures     => true,
                             create_occ_procedures      => true,
                                  audit                       => tapir.audit_t(user_exp          => '''me''',
                                                                         col_created_by    => 'created_by',
                                                                         col_created_date  => 'created_at',
                                                                         col_modified_by   => 'modified_by',
                                                                         col_modified_date => 'modified_at',
                                                                         ignore_when_comparing => true),
                             record_default_expressions => tapir.mapping('VARCHAR2_T'                       => 'sys.dbms_random.string(''L'', round(sys.dbms_random.value(1, 100)))',
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
                                                                         'ROWID_T'                          => '''1''')));
   tapir.compile_tapi(p_table_name => 'test_table_all_types');
end;
/
