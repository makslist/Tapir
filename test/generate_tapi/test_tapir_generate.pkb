create or replace package body test_tapir_generate is

    l_record_default_exp tapir.mapping := tapir.mapping('VARCHAR2_T'                       => 'sys.dbms_random.string(''L'', round(sys.dbms_random.value(1, 100)))',
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

    procedure test_tapi_create_ce_table is
    begin
        tapir.create_ce_table('test_table_ce', user ,true ,16);

        execute immediate 'drop table test_table_ce';
    end;

    procedure test_tapi_create_and_drop_ce_aq is
    begin
        tapir.create_ce_queue('ce_queue');

        tapir.drop_ce_queue('ce_queue');
    end;

    procedure test_table_does_not_exist is
    begin
        tapir.init(tapir.params_t());
        tapir.compile_tapi(p_table_name => 'table_does_not_exist');
    end;

    procedure test_tapi_source is
        l_source clob;
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$default'),
                                  audit_user_exp             => null,
                                  proc_pipe                  => null,
                                  create_bulk_procedures     => true,
                                  create_occ_procedures      => false,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  logging_exception_procedure => 'dbms_output.put_line(\1)',
                                  boolean_pseudo_type        => tapir.mapping('true' => 'Y', 'false' => 'N'),
                                  record_default_expressions => l_record_default_exp));
        l_source := tapir.tapi_source(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_tapis_failed is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping()));
        tapir.compile_tapis(p_owner => user, p_name_like => 'test_table_all_types');
    end;

    procedure test_tapi_compile_tapis is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$default'),
                                  audit_user_exp             => null,
                                  proc_pipe                  => null,
                                  create_bulk_procedures     => true,
                                  create_occ_procedures      => false,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  logging_exception_procedure => 'dbms_output.put_line(\1)',
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapis(p_owner => user, p_name_like => 'test_table_all_types');
    end;

    procedure test_tapi_compile_crud is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$default'),
                                  audit_user_exp             => null,
                                  proc_pipe                  => null,
                                  create_bulk_procedures     => true,
                                  create_occ_procedures      => false,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  logging_exception_procedure => 'dbms_output.put_line(\1)',
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_row_version is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$row_version'),
                                  audit_user_exp             => null,
                                  proc_pipe                  => null,
                                  create_bulk_procedures     => false,
                                  create_occ_procedures      => false,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  increase_row_version_column => 'char_t',
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_double_quoted_names is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$double_quote'),
                                  audit_user_exp             => null,
                                  proc_pipe                  => null,
                                  create_bulk_procedures     => true,
                                  create_occ_procedures      => true,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  double_quote_names         => true,
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_lock_timeout is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$lock_timeout'),
                                  audit_user_exp             => null,
                                  proc_pipe                  => null,
                                  create_bulk_procedures     => true,
                                  create_occ_procedures      => true,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  acquire_lock_timeout       => 1,
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_lock_timeout_no_wait is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$lock_timeout_no_wait'),
                                  audit_user_exp             => null,
                                  proc_pipe                  => null,
                                  create_bulk_procedures     => true,
                                  create_occ_procedures      => true,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  acquire_lock_timeout       => 0,
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_column_default_exp is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$column_default_exp'),
                                  audit_user_exp             => null,
                                  proc_pipe                  => null,
                                  create_bulk_procedures     => true,
                                  create_occ_procedures      => true,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  column_default_expressions => l_record_default_exp,
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_audit is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$audit'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  audit_user_exp              => '''me''',
                                  audit_col_created_by        => 'created_by',
                                  audit_col_created_date      => 'created_at',
                                  audit_col_modified_by       => 'modified_by',
                                  audit_col_modified_date     => 'modified_at',
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_audit_ignore_when_comparing is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$audit_ignore_compare'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  audit_user_exp              => '''me''',
                                  audit_col_created_by        => 'created_by',
                                  audit_col_created_date      => 'created_at',
                                  audit_col_modified_by       => 'modified_by',
                                  audit_col_modified_date     => 'modified_at',
                                  audit_ignore_when_comparing => true,
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_occ_procedures is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$occ_procedures'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  create_occ_procedures      => true,
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_non_lob_types');
    end;

    procedure test_tapi_compile_result_cache is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$result_cache'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  use_result_cache           => true,
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_non_lob_types');
    end;

    procedure test_tapi_compile_no_lock_proc is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$no_lock_proc'),
                                  proc_pipe                  => null,
                                  proc_of_json               => null,
                                  proc_lock_record           => null,
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_non_lob_types');
    end;

    procedure test_tapi_compile_json is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$json'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => 'json_obj',
                                  proc_of_json               => 'of_json',
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_no_logging_diff_json is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$no_logging'),
                                  proc_pipe                  => null,
                                  logging_exception_procedure => null,
                                  proc_diff                  => null,
                                  proc_json_obj              => null,
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    

    procedure test_tapi_compile_bulk is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$bulk'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => 'json_obj',
                                  proc_of_json               => 'of_json',
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_pipe_rows is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$pipe_rows'),
                                  proc_pipe                  => 'pipe',
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_return_null_when_no_data_found is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$null_when_no_data_found'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  return_null_when_no_data_found => true,
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_no_record_default_expression is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$no_record_default'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  record_default_expressions => tapir.mapping()));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_no_pk is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$dummy'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_no_pk');
    end;

    procedure test_tapi_compile_col_name_eq_tab_name is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$col_eq_tab'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  warn_about_null_string_default => false,
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_edge_case');
    end;

    procedure test_tapi_compile_col_default_null_string is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$col_default_null_string'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  warn_about_null_string_default => true,
                                  record_default_expressions => l_record_default_exp));
        tapir.compile_tapi(p_table_name => 'test_table_edge_case');
    end;

    procedure test_tapi_compile_cloud_event_table_not_exists is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$dummy'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  log_cloud_events           => tapir.mapping('table_name'    => 'non_existing',
                                                                              'aq_queue_name' => null)));
    end;

    procedure test_tapi_compile_cloud_event_queue_not_exists is
    begin
        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$dummy'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  log_cloud_events           => tapir.mapping('table_name'    => null,
                                                                              'aq_queue_name' => 'non_existing')));
    end;

    procedure test_tapi_compile_cloud_event_table is
        l_ce_table varchar2(100) := 'test_table_ce';
    begin
        tapir.create_ce_table(l_ce_table);

        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$cloud_events_table'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  log_cloud_events           => tapir.mapping('table_name'    => l_ce_table,
                                                                              'aq_queue_name' => null)));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');

        execute immediate 'drop table ' || l_ce_table;
    end;

    procedure test_tapi_compile_cloud_event_queue is
        l_ce_queue varchar2(100) := 'test_ce_queue';
    begin
        tapir.create_ce_queue(l_ce_queue);

        tapir.init(tapir.params_t(tapi_name                  => tapir.mapping('^(.*)$' => '\1$cloud_events_queue'),
                                  proc_pipe                  => null,
                                  proc_json_obj              => null,
                                  proc_of_json               => null,
                                  log_cloud_events           => tapir.mapping('table_name'    => null,
                                                                              'aq_queue_name' => l_ce_queue)));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');

        tapir.drop_ce_queue(l_ce_queue);
    end;

end;
/
