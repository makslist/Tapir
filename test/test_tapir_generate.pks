create or replace package test_tapir_generate is

    --%suite('test_tapir_compile')
    --%rollback(manual)
    --%suitepath(all.globaltests)

    --%test
    --%rollback(manual)
    procedure test_tapi_create_ce_table;

    --%test
    --%rollback(manual)
    procedure test_tapi_create_and_drop_ce_aq;

    --%test
    --%throws(-20000)
    procedure test_table_does_not_exist;

    --%test
    --%throws(-20000)
    procedure test_table_name_is_null;

    --%test
    procedure test_tapi_source;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_tapis_failed;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_tapis;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_crud;

    --%test
    --%throws(-20000)
    --%rollback(manual)
    procedure test_tapi_compile_failes_when_result_cache_with_lobs;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_row_version;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_double_quoted_names;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_lock_timeout;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_lock_timeout_no_wait;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_column_default_exp;

    --%test
    --%rollback(manual)
    procedure test_tapi_source_no_boolean_pseudo_type;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_audit;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_audit_ignore_when_comparing;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_occ_procedures;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_result_cache;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_no_lock_proc;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_json;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_no_logging_diff_json;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_bulk;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_pipe_rows;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_return_null_when_no_data_found;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_raise_error_on_failed_update_delete;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_no_record_default_expression;

    --%test
    --%throws(-20000)
    --%rollback(manual)
    procedure test_tapi_compile_no_pk;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_col_name_eq_tab_name;

    --%test
    --%throws(-20000)
    --%rollback(manual)
    procedure test_tapi_compile_col_default_null_string;

    --%test
    --%throws(-20000)
    --%rollback(manual)
    procedure test_tapi_compile_cloud_event_table_not_exists;

    --%test
    --%throws(-20000)
    --%rollback(manual)
    procedure test_tapi_compile_cloud_event_queue_not_exists;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_cloud_event_table;

    --%test
    --%rollback(manual)
    procedure test_tapi_compile_cloud_event_queue;

    --%test
    --%throws(-20000)
    --%rollback(manual)
    procedure test_tapi_drop_cloud_event_queue;

end;
/
