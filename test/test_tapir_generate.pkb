create or replace package body test_tapir_generate is

    l_col_defaults tapir.mapping := tapir.mapping('VARCHAR2_T'                       => 'sys.dbms_random.string(''L'', round(sys.dbms_random.value(1, 100)))',
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
    l_defaults     tapir.defaults_t := tapir.defaults_t(init_record_expressions => l_col_defaults);

    function count_objects(p_name_like in varchar2) return pls_integer is
        l_count pls_integer;
    begin
        select count(*)
          into l_count
          from all_objects o
         where o.owner = user
               and upper(o.object_name) = upper(p_name_like);
        return l_count;
    end;

    procedure test_tapi_create_ce_table is
        l_ce_tab_name varchar2(100) := 'test_table_ce';
    begin
        tapir.create_ce_table(l_ce_tab_name, user, true, 16);
        ut.expect(count_objects(l_ce_tab_name)).to_equal(1);
    
        execute immediate 'drop table test_table_ce';
    end;

    procedure test_tapi_create_and_drop_ce_aq is
        l_queue_name varchar2(100) := 'ce_queue';
    begin
        tapir.create_ce_queue(l_queue_name);
        ut.expect(count_objects('cloud_event')).to_equal(1);
        ut.expect(count_objects(l_queue_name || '_tab')).to_equal(1);
        ut.expect(count_objects(l_queue_name)).to_equal(1);
    
        tapir.drop_ce_queue(p_queue_name => 'ce_queue', p_drop_type => true);
        ut.expect(count_objects('cloud_event')).to_equal(0);
        ut.expect(count_objects(l_queue_name || '_tab')).to_equal(0);
        ut.expect(count_objects(l_queue_name)).to_equal(0);
    end;

    procedure test_table_does_not_exist is
    begin
        tapir.init(tapir.params_t());
        tapir.compile_tapi(p_table_name => 'table_does_not_exist');
    end;

    procedure test_table_name_is_null is
    begin
        tapir.init(tapir.params_t());
        tapir.compile_tapi(p_table_name => null);
    end;

    procedure test_tapi_source is
        l_source clob;
    begin
        tapir.init(tapir.params_t(tapi_name               => tapir.mapping('^(.*)$' => 'test_tapir$default'),
                                  audit                   => null,
                                  proc_pipe               => null,
                                  proc_json_obj           => null,
                                  proc_of_json            => null,
                                  log_exception_procedure => 'dbms_output.put_line(\1)',
                                  boolean_pseudo_type     => tapir.boolean_pseudo_type_t(true_value  => 'Y',
                                                                                         false_value => 'N'),
                                  defaults                => l_defaults));
        l_source := tapir.tapi_source(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_tapis_failed is
    begin
        tapir.init(tapir.params_t(tapi_name => tapir.mapping()));
        tapir.compile_tapis(p_owner => user, p_name_like => 'test_table_all_types');
    end;

    procedure test_tapi_compile_tapis is
    begin
        tapir.init(tapir.params_t(tapi_name               => tapir.mapping('^(.*)$' => 'test_tapir$default'),
                                  audit                   => null,
                                  proc_pipe               => null,
                                  proc_json_obj           => null,
                                  proc_of_json            => null,
                                  log_exception_procedure => 'dbms_output.put_line(\1)',
                                  defaults                => l_defaults));
        tapir.compile_tapis(p_owner => user, p_name_like => 'test_table_all_types');
    end;

    procedure test_tapi_compile_crud is
    begin
        tapir.init(tapir.params_t(tapi_name               => tapir.mapping('^(.*)$' => 'test_tapir$default'),
                                  audit                   => null,
                                  proc_pipe               => null,
                                  proc_json_obj           => null,
                                  proc_of_json            => null,
                                  log_exception_procedure => 'dbms_output.put_line(\1)',
                                  defaults                => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_failes_when_result_cache_with_lobs is
    begin
        tapir.init(tapir.params_t(tapi_name               => tapir.mapping('^(.*)$' => 'test_tapir$failed'),
                                  use_result_cache        => true));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_row_version is
    begin
        tapir.init(tapir.params_t(tapi_name     => tapir.mapping('^(.*)$' => 'test_tapir$row_version'),
                                  audit         => null,
                                  proc_pipe     => null,
                                  bulk_proc     => tapir.bulk_t(generate => false),
                                  proc_json_obj => null,
                                  proc_of_json  => null,
                                  defaults      => tapir.defaults_t(init_record_expressions => l_col_defaults,
                                                                    row_version_column      => 'number_t')));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_double_quoted_names is
    begin
        tapir.init(tapir.params_t(tapi_name             => tapir.mapping('^(.*)$' => 'test_tapir$double_quote'),
                                  audit                 => null,
                                  proc_pipe             => null,
                                  create_occ_procedures => true,
                                  proc_json_obj         => null,
                                  proc_of_json          => null,
                                  double_quote_names    => true,
                                  defaults              => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_lock_timeout is
    begin
        tapir.init(tapir.params_t(tapi_name             => tapir.mapping('^(.*)$' => 'test_tapir$lock_timeout'),
                                  audit                 => null,
                                  proc_pipe             => null,
                                  create_occ_procedures => true,
                                  proc_json_obj         => null,
                                  proc_of_json          => null,
                                  acquire_lock_timeout  => 1,
                                  defaults              => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_lock_timeout_no_wait is
    begin
        tapir.init(tapir.params_t(tapi_name             => tapir.mapping('^(.*)$' => 'test_tapir$lock_timeout_no_wait'),
                                  audit                 => null,
                                  proc_pipe             => null,
                                  create_occ_procedures => true,
                                  proc_json_obj         => null,
                                  proc_of_json          => null,
                                  acquire_lock_timeout  => 0,
                                  defaults              => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_column_default_exp is
    begin
        tapir.init(tapir.params_t(tapi_name             => tapir.mapping('^(.*)$' => 'test_tapir$column_default_exp'),
                                  audit                 => null,
                                  proc_pipe             => null,
                                  create_occ_procedures => true,
                                  proc_json_obj         => null,
                                  proc_of_json          => null,
                                  defaults              => tapir.defaults_t(init_record_expressions => l_col_defaults,
                                                                            column_expressions      => l_col_defaults,
                                                                            row_version_column      => 'number_t')));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_source_no_boolean_pseudo_type is
        l_source clob;
    begin
        tapir.init(tapir.params_t(tapi_name               => tapir.mapping('^(.*)$' => 'test_tapir$no_boolean_pseudo_type'),
                                  audit                   => null,
                                  proc_pipe               => null,
                                  proc_json_obj           => null,
                                  proc_of_json            => null,
                                  log_exception_procedure => 'dbms_output.put_line(\1)',
                                  boolean_pseudo_type     => null,
                                  defaults                => l_defaults));
        l_source := tapir.tapi_source(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_audit is
    begin
        tapir.init(tapir.params_t(tapi_name     => tapir.mapping('^(.*)$' => 'test_tapir$audit'),
                                  proc_pipe     => null,
                                  proc_json_obj => null,
                                  proc_of_json  => null,
                                  audit         => tapir.audit_t(user_exp          => '''me''',
                                                                 col_created_by    => 'created_by',
                                                                 col_created_date  => 'created_at',
                                                                 col_modified_by   => 'modified_by',
                                                                 col_modified_date => 'modified_at'),
                                  defaults      => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_audit_ignore_when_comparing is
    begin
        tapir.init(tapir.params_t(tapi_name     => tapir.mapping('^(.*)$' => 'test_tapir$audit_ignore_compare'),
                                  proc_pipe     => null,
                                  proc_json_obj => null,
                                  proc_of_json  => null,
                                  audit         => tapir.audit_t(user_exp              => '''me''',
                                                                 col_created_by        => 'created_by',
                                                                 col_created_date      => 'created_at',
                                                                 col_modified_by       => 'modified_by',
                                                                 col_modified_date     => 'modified_at',
                                                                 ignore_when_comparing => true),
                                  defaults      => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_occ_procedures is
    begin
        tapir.init(tapir.params_t(tapi_name             => tapir.mapping('^(.*)$' => 'test_tapir$occ_procedures'),
                                  proc_pipe             => null,
                                  proc_json_obj         => null,
                                  proc_of_json          => null,
                                  create_occ_procedures => true,
                                  defaults              => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_non_lob_types');
    end;

    procedure test_tapi_compile_result_cache is
    begin
        tapir.init(tapir.params_t(tapi_name        => tapir.mapping('^(.*)$' => 'test_tapir$result_cache'),
                                  proc_pipe        => null,
                                  proc_json_obj    => null,
                                  proc_of_json     => null,
                                  use_result_cache => true,
                                  defaults         => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_non_lob_types');
    end;

    procedure test_tapi_compile_no_lock_proc is
    begin
        tapir.init(tapir.params_t(tapi_name        => tapir.mapping('^(.*)$' => 'test_tapir$no_lock_proc'),
                                  proc_pipe        => null,
                                  proc_of_json     => null,
                                  proc_lock_record => null,
                                  defaults         => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_non_lob_types');
    end;

    procedure test_tapi_compile_json is
    begin
        tapir.init(tapir.params_t(tapi_name     => tapir.mapping('^(.*)$' => 'test_tapir$json'),
                                  proc_pipe     => null,
                                  proc_json_obj => 'json_obj',
                                  proc_of_json  => 'of_json',
                                  defaults      => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_no_logging_diff_json is
    begin
        tapir.init(tapir.params_t(tapi_name               => tapir.mapping('^(.*)$' => 'test_tapir$no_logging'),
                                  proc_pipe               => null,
                                  log_exception_procedure => null,
                                  proc_diff               => null,
                                  proc_json_obj           => null,
                                  defaults                => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_bulk is
    begin
        tapir.init(tapir.params_t(tapi_name     => tapir.mapping('^(.*)$' => 'test_tapir$bulk'),
                                  proc_pipe     => null,
                                  proc_json_obj => 'json_obj',
                                  proc_of_json  => 'of_json',
                                  defaults      => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_pipe_rows is
    begin
        tapir.init(tapir.params_t(tapi_name     => tapir.mapping('^(.*)$' => 'test_tapir$pipe_rows'),
                                  proc_pipe     => 'pipe',
                                  proc_json_obj => null,
                                  proc_of_json  => null,
                                  defaults      => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_return_null_when_no_data_found is
    begin
        tapir.init(tapir.params_t(tapi_name                      => tapir.mapping('^(.*)$' => 'test_tapir$null_when_no_data_found'),
                                  proc_pipe                      => null,
                                  proc_json_obj                  => null,
                                  proc_of_json                   => null,
                                  return_null_when_no_data_found => true,
                                  defaults                       => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_raise_error_on_failed_update_delete is
    begin
        tapir.init(tapir.params_t(tapi_name                           => tapir.mapping('^(.*)$' => 'test_tapir$raise_error_on_failed_update_delete'),
                                  proc_pipe                           => null,
                                  proc_json_obj                       => null,
                                  proc_of_json                        => null,
                                  raise_error_on_failed_update_delete => true,
                                  defaults                            => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_no_record_default_expression is
    begin
        tapir.init(tapir.params_t(tapi_name     => tapir.mapping('^(.*)$' => 'test_tapir$no_record_default'),
                                  proc_pipe     => null,
                                  proc_json_obj => null,
                                  proc_of_json  => null));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    end;

    procedure test_tapi_compile_no_pk is
    begin
        tapir.init(tapir.params_t(tapi_name     => tapir.mapping('^(.*)$' => 'test_tapir$dummy'),
                                  proc_pipe     => null,
                                  proc_json_obj => null,
                                  proc_of_json  => null,
                                  defaults      => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_no_pk');
    end;

    procedure test_tapi_compile_col_name_eq_tab_name is
    begin
        tapir.init(tapir.params_t(tapi_name                      => tapir.mapping('^(.*)$' => 'test_tapir$col_eq_tab'),
                                  proc_pipe                      => null,
                                  proc_json_obj                  => null,
                                  proc_of_json                   => null,
                                  warn_about_null_string_default => false,
                                  defaults                       => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_edge_case');
    end;

    procedure test_tapi_compile_col_default_null_string is
    begin
        tapir.init(tapir.params_t(tapi_name                      => tapir.mapping('^(.*)$' => 'test_tapir$col_default_null_string'),
                                  proc_pipe                      => null,
                                  proc_json_obj                  => null,
                                  proc_of_json                   => null,
                                  warn_about_null_string_default => true,
                                  defaults                       => l_defaults));
        tapir.compile_tapi(p_table_name => 'test_table_edge_case');
    end;

    procedure test_tapi_compile_cloud_event_table_not_exists is
    begin
        tapir.init(tapir.params_t(tapi_name     => tapir.mapping('^(.*)$' => 'test_tapir$dummy'),
                                  proc_pipe     => null,
                                  proc_json_obj => null,
                                  proc_of_json  => null,
                                  cloud_events  => tapir.cloud_events_t(table_name => 'non_existing')));
    end;

    procedure test_tapi_compile_cloud_event_queue_not_exists is
    begin
        tapir.init(tapir.params_t(tapi_name     => tapir.mapping('^(.*)$' => 'test_tapir$dummy'),
                                  proc_pipe     => null,
                                  proc_json_obj => null,
                                  proc_of_json  => null,
                                  cloud_events  => tapir.cloud_events_t(aq_queue_name => 'non_existing')));
    end;

    procedure test_tapi_compile_cloud_event_table is
        l_ce_table varchar2(100) := 'test_table_ce';
    begin
        tapir.create_ce_table(l_ce_table);
    
        tapir.init(tapir.params_t(tapi_name     => tapir.mapping('^(.*)$' => 'test_tapir$cloud_events_table'),
                                  proc_pipe     => null,
                                  proc_json_obj => null,
                                  proc_of_json  => null,
                                  cloud_events  => tapir.cloud_events_t(table_name => l_ce_table)));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    
        execute immediate 'drop table ' || l_ce_table;
    end;

    procedure test_tapi_compile_cloud_event_queue is
        l_ce_queue varchar2(100) := 'test_ce_queue';
    begin
        tapir.create_ce_queue(l_ce_queue);
    
        tapir.init(tapir.params_t(tapi_name     => tapir.mapping('^(.*)$' => 'test_tapir$cloud_events_queue'),
                                  proc_pipe     => null,
                                  proc_json_obj => null,
                                  proc_of_json  => null,
                                  cloud_events  => tapir.cloud_events_t(aq_queue_name => l_ce_queue)));
        tapir.compile_tapi(p_table_name => 'test_table_all_types');
    
        tapir.drop_ce_queue(l_ce_queue);
    end;

    procedure test_tapi_drop_cloud_event_queue is
    begin
        tapir.drop_ce_queue('non_existing', user, true);
    end;

end;
/
