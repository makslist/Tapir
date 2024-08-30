create or replace package test_tapi authid definer is

    --%suite('test_table$tapi')
    --%suitepath(tapi.globaltests)

    --%test
    procedure test_pk_str;

    --%test
    procedure test_insert_with_custom_defaults;

    --%test
    --%throws(-1)
    procedure test_insert_dup_val_on_index;

    --%test
    --%throws(-20000)
    procedure test_parameter_is_null;

    --%test
    procedure test_insert_rows;

    --%test
    procedure test_ins_cursor;

    --%test
    procedure test_insert_rows_return_errors;

    --%test
    --%throws(-24381)
    procedure test_insert_rows_exception;

    --%test
    procedure test_exists;

    --%test
    procedure test_exists_yn;

    --%test
    procedure test_counts;

    --%test
    procedure test_exists_select;

    --%test
    procedure test_select;

    --%test
    --%throws(-1403)
    procedure test_select_no_data_found;

    --%test
    --%throws(-1403)
    procedure test_select_unique_col_no_data_found;

    --%test
    procedure test_select_for_update;

    --%test
    --%throws(-1403)
    procedure test_select_for_update_no_data_found;

    --%test
    procedure test_select_unique_col_for_update;

    --%test
    --%throws(-1403)
    procedure test_select_unique_col_for_update_no_data_found;

    --%test
    procedure test_select_rows;

    --%test
    procedure test_non_unique_index_cursors;

    --%test
    procedure test_update;

    --%test
    procedure test_update_rows;

    --%test
    --%throws(-24381)
    procedure test_update_rows_exception;

    --%test
    procedure test_merge;

    --%test
    procedure test_delete;

    --%test
    procedure test_delete_unique_col;

    --%test
    --%throws(-1403)
    procedure test_delete_no_data_found;

    --%test
    --%throws(-1403)
    procedure test_delete_unique_col_no_data_found;

    --%test
    procedure test_audit_insert;

    --%test
    procedure test_audit_insert_rows;

    --%test
    procedure test_audit_update;

    --%test
    procedure test_audit_update_rows;

    --%test
    procedure test_audit_merge_insert;

    --%test
    procedure test_audit_merge_update;

    --%test
    procedure test_checksum;

    --%test
    procedure test_select_opt;

    --%test
    procedure test_update_opt;

    --%test
    --%throws(-20000)
    procedure test_update_opt_not_found;

    --%test
    --%throws(-20000)
    procedure test_update_opt_changed;

    --%test
    procedure test_to_json;

    --%test
    procedure test_of_json;

    --%test
    procedure test_to_json_array;

    --%test
    procedure test_diff;

    --%test
    procedure test_diff_equals;

    --%test
    procedure test_diff_delete;

    --%test
    procedure test_diff_insert;

    --%test
    procedure test_undo_insert;
    --%test

    procedure test_undo_delete;

    --%test
    procedure test_undo_update;

    --%test
    procedure test_redo_insert;

    --%test
    procedure test_redo_delete;

    --%test
    procedure test_redo_update;

end;
/
