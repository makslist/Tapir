create or replace package body test_tapi is

    procedure test_pk_str is
        l_pk1 test_table$tapi.pk_t := 1;
        l_str varchar2(32767);
    begin
        l_str := test_table$tapi.pk_str(test_table$tapi.rt_defaults(pk => l_pk1));
    
        ut.expect(l_str).to_equal(to_char(l_pk1));
    end;

    procedure test_insert_with_custom_defaults is
        l_pk1  test_table$tapi.pk_t := 1;
        l_tapi test_table$tapi.rt;
    begin
        l_tapi := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
    
        ut.expect(test_table$tapi.counts(p_pk => l_pk1)).to_equal(1);
    end;

    procedure test_print is
        l_pk1    test_table$tapi.pk_t := 1;
        l_tapi   test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        l_line   varchar2(32767);
        l_status integer;
    begin
        test_table$tapi.print(l_tapi);

        dbms_output.get_line(l_line, l_status);
        ut.expect(l_status).to_equal(0);
    end;

    procedure test_insert_dup_val_on_index is
        l_pk1  test_table$tapi.pk_t := 1;
        l_tapi test_table$tapi.rt;
    begin
        l_tapi := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        l_tapi := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
    end;

    procedure test_insert_dup_val_on_index_unique is
        l_pk1  test_table$tapi.pk_t := 1;
        l_tapi test_table$tapi.rt;
    begin
        l_tapi := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => 1, varchar2_t => '1'));
        l_tapi := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => 2, varchar2_t => '1'));
    end;

    procedure test_insert_not_null_constraint is
        l_pk1  test_table$tapi.pk_t := 1;
        l_tapi test_table$tapi.rt;
    begin
        l_tapi := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => 1, number_t => null));
    end;

    procedure test_parameter_is_null is
        l_tapi test_table$tapi.rt;
    begin
        l_tapi := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => null));
    end;

    procedure test_insert_rows is
        l_rows test_table$tapi.rows_tab;
    begin
        l_rows := test_table$tapi.rows_tab(test_table$tapi.rt_defaults(pk => '1'),
                                           test_table$tapi.rt_defaults(pk => '2'));
        test_table$tapi.ins_rows(l_rows);
    
        ut.expect(test_table$tapi.counts()).to_equal(2);
    end;

    procedure test_ins_cursor is
        l_pk1 test_table$tapi.pk_t := 1;
        l_cur test_table$tapi.strong_ref_cursor;
    begin
        open l_cur for
            select l_pk1 as pk,
                   null as identity_column,
                   sys.dbms_random.string('L', round(sys.dbms_random.value(1, 100))) as t_varchar2,
                   sys.dbms_random.string('L', 1) as t_char,
                   sys.dbms_random.string('L', 1) as t_nchar,
                   sys.dbms_random.string('L', round(sys.dbms_random.value(1, 100))) as t_nvarchar2,
                   round(sys.dbms_random.value(1, 1024 * 1024)) as t_number,
                   to_number(2) as t_float,
                   round(sys.dbms_random.value(1, 1024 * 1024)) as t_binary_float,
                   round(sys.dbms_random.value(1, 1024 * 1024)) as t_binary_double,
                   sysdate as t_date,
                   systimestamp as t_timestamp,
                   systimestamp as t_timestamp_with_local_time_zone,
                   systimestamp as t_timestamp_with_time_zone,
                   (systimestamp - to_date('2024', 'YYYY')) year(9) to month as t_interval_year_to_month,
                   (systimestamp - to_date('2024', 'YYYY')) day(9) to second as t_interval_day_to_second_t,
                   utl_raw.cast_to_raw('blob') as t_blob,
                   substr(sys_guid(), 1, 20) as t_clob,
                   substr(sys_guid(), 1, 20) as t_nclob,
                   utl_raw.cast_to_raw('raw') as t_raw,
                   true as t_bool,
                   null as virtual_col,
                   null as created_by_t,
                   null as created_at_t,
                   null as modified_by_t,
                   null as modified_at_t
              from dual;
        test_table$tapi.ins_cur(l_cur);
    
        ut.expect(test_table$tapi.counts(p_pk => l_pk1)).to_equal(1);
    end;

    procedure test_insert_rows_return_errors is
        l_pk1        test_table$tapi.pk_t := 1;
        l_unique_row test_table$tapi.rt := test_table$tapi.rt_defaults(pk => l_pk1);
        l_rows       test_table$tapi.rows_tab;
        l_errors     test_table$tapi.rows_tab;
    begin
        test_table$tapi.ins(l_unique_row);
        l_rows := test_table$tapi.rows_tab(l_unique_row, test_table$tapi.rt_defaults(pk => '2'));
        l_rows := test_table$tapi.ins_rows(l_rows, l_errors);
    
        ut.expect(l_rows.count).to_equal(1);
        ut.expect(l_errors.count).to_equal(1);
        ut.expect(test_table$tapi.counts()).to_equal(2);
    end;

    procedure test_insert_rows_exception is
        l_rows test_table$tapi.rows_tab;
    begin
        l_rows := test_table$tapi.rows_tab(test_table$tapi.rt_defaults(pk => '1'));
        test_table$tapi.ins_rows(l_rows);
    
        test_table$tapi.ins_rows(l_rows);
    end;

    procedure test_exists is
        l_pk1        test_table$tapi.pk_t := 1;
        l_unique_col test_table$tapi.pk_t := '1';
        l_tapi       test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk         => l_pk1,
                                                                                           varchar2_t => l_unique_col));
        l_exists     boolean;
    begin
        l_exists := test_table$tapi.exist(l_tapi);
        ut.expect(l_exists).to_be_true();
    
        l_exists := test_table$tapi.exist_unique_col(p_varchar2_t => l_unique_col);
        ut.expect(l_exists).to_be_true();
    end;

    procedure test_exists_yn is
        l_pk1        test_table$tapi.pk_t := 1;
        l_unique_col test_table$tapi.pk_t := '1';
        l_tapi       test_table$tapi.rt;
        l_exists_yn  varchar2(1);
    begin
        l_exists_yn := test_table$tapi.exist_yn(l_pk1);
        ut.expect(l_exists_yn).to_equal('N');
    
        l_exists_yn := test_table$tapi.exist_unique_col_yn(p_varchar2_t => l_unique_col);
        ut.expect(l_exists_yn).to_equal('N');
    
        l_tapi := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1, varchar2_t => l_unique_col));
    
        l_exists_yn := test_table$tapi.exist_yn(l_pk1);
        ut.expect(l_exists_yn).to_equal('Y');
    
        l_exists_yn := test_table$tapi.exist_unique_col_yn(p_varchar2_t => l_unique_col);
        ut.expect(l_exists_yn).to_equal('Y');
    end;

    procedure test_counts is
        l_pk1        test_table$tapi.pk_t := 1;
        l_unique_col test_table$tapi.pk_t := '1';
        l_tapi       test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk         => l_pk1,
                                                                                           varchar2_t => l_unique_col));
        l_count      number;
    begin
        l_count := test_table$tapi.counts(p_pk => l_pk1);
        ut.expect(l_count).to_equal(1);
    
        l_count := test_table$tapi.counts_unique_col(p_varchar2_t => l_unique_col);
        ut.expect(l_count).to_equal(1);
    end;

    procedure test_exists_select is
        l_pk1        test_table$tapi.pk_t := 1;
        l_unique_col test_table$tapi.pk_t := '1';
        l_tapi       test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk         => l_pk1,
                                                                                           varchar2_t => l_unique_col));
        l_exists     boolean;
    begin
        l_exists := test_table$tapi.exists_sel(l_tapi);
        ut.expect(l_exists).to_be_true();
    
        l_exists := test_table$tapi.exists_sel_unique_col(p_varchar2_t => l_unique_col, p_rec => l_tapi);
        ut.expect(l_exists).to_be_true();
    end;

    procedure test_select is
        l_pk1  test_table$tapi.pk_t := 1;
        l_tapi test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
    begin
        test_table$tapi.sel(l_tapi);
        l_tapi := test_table$tapi.sel(l_tapi);
    
        ut.expect(l_tapi.pk).to_equal(l_pk1);
    end;

    procedure test_select_no_data_found is
        l_pk1  test_table$tapi.pk_t := 1;
        l_tapi test_table$tapi.rt;
    begin
        l_tapi := test_table$tapi.sel(p_pk => l_pk1);
    end;

    procedure test_select_unique_col_no_data_found is
        l_pk1  test_table$tapi.pk_t := 1;
        l_tapi test_table$tapi.rt;
    begin
        l_tapi := test_table$tapi.sel_unique_col(p_varchar2_t => 'aaa');
    end;

    procedure test_select_for_update is
        l_pk1        test_table$tapi.pk_t := 1;
        l_unique_col test_table$tapi.varchar2_t_t := '1';
        l_tapi       test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk         => l_pk1,
                                                                                           varchar2_t => l_unique_col));
    begin
        test_table$tapi.sel_lock(l_tapi);
        l_tapi := test_table$tapi.sel_lock(l_tapi);
        ut.expect(l_tapi.pk).to_equal(l_pk1);
    
        l_tapi := test_table$tapi.sel_unique_col(p_varchar2_t => l_unique_col);
        ut.expect(l_tapi.varchar2_t).to_equal(l_unique_col);
    end;

    procedure test_select_for_update_no_data_found is
        l_pk1  test_table$tapi.pk_t := 1;
        l_tapi test_table$tapi.rt;
    begin
        l_tapi := test_table$tapi.sel_lock(p_pk => l_pk1);
    end;

    procedure test_select_unique_col_for_update is
        l_pk1        test_table$tapi.pk_t := 1;
        l_unique_col test_table$tapi.varchar2_t_t := '1';
        l_tapi       test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk         => l_pk1,
                                                                                           varchar2_t => l_unique_col));
    begin
        l_tapi := test_table$tapi.sel_lock_unique_col(p_varchar2_t => l_unique_col);
        ut.expect(l_tapi.varchar2_t).to_equal(l_unique_col);
    end;

    procedure test_select_unique_col_for_update_no_data_found is
        l_pk1        test_table$tapi.pk_t := 1;
        l_unique_col test_table$tapi.varchar2_t_t := '1';
        l_tapi       test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk         => l_pk1,
                                                                                           varchar2_t => l_unique_col));
    begin
        l_tapi := test_table$tapi.sel_lock_unique_col(p_varchar2_t => 'aaa');
    end;

    procedure test_select_rows is
        l_pk1    test_table$tapi.pk_t := 1;
        l_rows   test_table$tapi.rows_tab;
        l_errors test_table$tapi.rows_tab;
        l_cur    test_table$tapi.strong_ref_cursor;
    begin
        l_rows := test_table$tapi.rows_tab(test_table$tapi.rt_defaults(pk => l_pk1),
                                           test_table$tapi.rt_defaults(pk => '2'));
        l_rows := test_table$tapi.ins_rows(l_rows, l_errors);
    
        open l_cur for
            select *
              from test_table
             where pk = l_pk1;
        l_rows := test_table$tapi.sel_rows(l_cur);
    
        ut.expect(l_rows.count).to_equal(1);
    end;

    procedure test_pipe_rows is
        l_pk1    test_table$tapi.pk_t := 1;
        l_row    test_table$tapi.rt;
        l_rows   test_table$tapi.rows_tab;
        l_cur    test_table$tapi.strong_ref_cursor;
    begin
        l_rows := test_table$tapi.rows_tab(test_table$tapi.rt_defaults(pk => l_pk1),
                                           test_table$tapi.rt_defaults(pk => '2'));
        test_table$tapi.ins_rows(l_rows);
    
        open l_cur for
            select *
              from test_table
             where pk = l_pk1;
        select *
          into l_row
          from table(test_table$tapi.pipe_rows(l_cur));
    
        ut.expect(l_row.pk).to_equal(l_pk1);
    end;

    procedure test_pipe_rows_cursor_closed is
        l_pk1    test_table$tapi.pk_t := 1;
        l_count  number;
        l_rows   test_table$tapi.rows_tab;
        l_cur    test_table$tapi.strong_ref_cursor;
    begin
        l_rows := test_table$tapi.rows_tab(test_table$tapi.rt_defaults(pk => l_pk1),
                                           test_table$tapi.rt_defaults(pk => '2'));
        test_table$tapi.ins_rows(l_rows);
    
        open l_cur for
            select *
              from test_table
             where pk = l_pk1;
        close l_cur;
        select count(*)
          into l_count
          from table(test_table$tapi.pipe_rows(l_cur));
    
        ut.expect(l_count).to_equal(0);
    end;

    procedure test_non_unique_index_cursors is
        l_pk1       test_table$tapi.pk_t := 1;
        l_idx_value test_table$tapi.number_t_t := 123456;
        l_rows      test_table$tapi.rows_tab;
        l_errors    test_table$tapi.rows_tab;
        l_rec       test_table$tapi.rt;
        l_found     boolean;
    begin
        l_rows := test_table$tapi.rows_tab(test_table$tapi.rt_defaults(pk => l_pk1, number_t => l_idx_value),
                                           test_table$tapi.rt_defaults(pk => '2'));
        l_rows := test_table$tapi.ins_rows(l_rows, l_errors);
        open test_table$tapi.cur_idx_col_idx(l_idx_value);
        fetch test_table$tapi.cur_idx_col_idx
            into l_rec;
        l_found := test_table$tapi.cur_idx_col_idx%found;
        close test_table$tapi.cur_idx_col_idx;
        ut.expect(l_found).to_be_true();
    
        open test_table$tapi.cur_idx_col_idx_lock(l_idx_value);
        fetch test_table$tapi.cur_idx_col_idx_lock
            into l_rec;
        l_found := test_table$tapi.cur_idx_col_idx_lock%found;
        close test_table$tapi.cur_idx_col_idx_lock;
        ut.expect(l_found).to_be_true();
    end;

    procedure test_update is
        l_pk1    test_table$tapi.pk_t := 1;
        l_number number := 999999999;
        l_tapi   test_table$tapi.rt;
    begin
        l_tapi          := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        l_tapi.number_t := l_number;
        test_table$tapi.upd(l_tapi);
    
        l_tapi := test_table$tapi.sel(p_pk => l_pk1);
        ut.expect(l_tapi.number_t).to_equal(l_number);
    end;

    procedure test_update_dup_val_on_index is
        l_tapi   test_table$tapi.rt;
    begin
        l_tapi          := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => 1, varchar2_t => 'unique'));
        l_tapi          := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => 2, varchar2_t => 'non_unique'));
        l_tapi.varchar2_t := 'unique';
        test_table$tapi.upd(l_tapi);
    end;

    procedure test_update_not_null is
        l_tapi   test_table$tapi.rt;
    begin
        l_tapi          := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => 1));
        l_tapi.number_t := null;
        test_table$tapi.upd(l_tapi);
    end;

    procedure test_update_rows is
        l_row_1  test_table$tapi.rt := test_table$tapi.rt_defaults(pk => '1');
        l_row_2  test_table$tapi.rt := test_table$tapi.rt_defaults(pk => '2');
        l_number number := 999999999;
        l_rows   test_table$tapi.rows_tab;
        l_cur    test_table$tapi.strong_ref_cursor;
    begin
        test_table$tapi.ins(l_row_1);
        test_table$tapi.ins(l_row_2);
    
        l_row_1.number_t := l_number;
        l_row_2.number_t := l_number;
        l_rows           := test_table$tapi.rows_tab(l_row_1, l_row_2);
        test_table$tapi.upd_rows(l_rows);
    
        open l_cur for
            select *
              from test_table
             where number_t = l_number;
        l_rows := test_table$tapi.sel_rows(l_cur);
        ut.expect(l_rows.count).to_equal(2);
    end;

    procedure test_update_rows_exception is
        l_row_1 test_table$tapi.rt := test_table$tapi.rt_defaults(pk => '1');
        l_row_2 test_table$tapi.rt := test_table$tapi.rt_defaults(pk => '2');
        l_rows  test_table$tapi.rows_tab := test_table$tapi.rows_tab(l_row_1, l_row_2);
        l_cur   test_table$tapi.strong_ref_cursor;
    begin
        test_table$tapi.ins_rows(l_rows);
    
        l_row_1.number_t        := null;
        l_row_2.identity_column := null;
        l_rows                  := test_table$tapi.rows_tab(l_row_1, l_row_2);
    
        test_table$tapi.upd_rows(l_rows);
    end;

    procedure test_merge is
        l_pk1    test_table$tapi.pk_t := 1;
        l_rows   test_table$tapi.rows_tab;
        l_number number := 999999999;
        l_tapi   test_table$tapi.rt;
    begin
        ut.expect(test_table$tapi.counts()).to_equal(0);
        test_table$tapi.upsert(test_table$tapi.rt_defaults(pk => l_pk1));
    
        ut.expect(test_table$tapi.counts()).to_equal(1);
    
        test_table$tapi.upsert(test_table$tapi.rt_defaults(pk => l_pk1, number_t => l_number));
    
        l_tapi := test_table$tapi.sel(p_pk => l_pk1);
        ut.expect(l_tapi.number_t).to_equal(l_number);
    end;

    procedure test_delete is
        l_pk1  test_table$tapi.pk_t := 1;
        l_tapi test_table$tapi.rt;
    begin
        l_tapi := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        ut.expect(test_table$tapi.counts(p_pk => l_pk1)).to_equal(1);
    
        test_table$tapi.del(l_tapi);
        ut.expect(test_table$tapi.counts(p_pk => l_pk1)).to_equal(0);
    end;

    procedure test_delete_unique_col is
        l_pk1        test_table$tapi.pk_t := 1;
        l_unique_col test_table$tapi.varchar2_t_t := '1';
        l_tapi       test_table$tapi.rt;
    begin
        l_tapi := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1, varchar2_t => l_unique_col));
        ut.expect(test_table$tapi.counts_unique_col(p_varchar2_t => l_unique_col)).to_equal(1);
    
        test_table$tapi.del_unique_col(p_varchar2_t => l_unique_col);
        ut.expect(test_table$tapi.counts_unique_col(p_varchar2_t => l_unique_col)).to_equal(0);
    end;

    procedure test_delete_no_data_found is
        l_pk1 test_table$tapi.pk_t := 1;
    begin
        test_table$tapi.del(p_pk => l_pk1);
    end;

    procedure test_delete_unique_col_no_data_found is
        l_unique_col test_table$tapi.varchar2_t_t := '1';
    begin
        test_table$tapi.del_unique_col(p_varchar2_t => l_unique_col);
    end;

    procedure test_audit_insert is
        l_tapi   test_table$tapi.rt;
        l_before date := sysdate;
    begin
        l_tapi := test_table$tapi.ins(test_table$tapi.rt(pk => 1, number_t => 2));
        ut.expect(l_tapi.created_by).to_equal('me');
        ut.expect(l_tapi.created_at).to_be_greater_or_equal(l_before);
        ut.expect(l_tapi.modified_by).to_equal('me');
        ut.expect(cast(l_tapi.modified_at as date)).to_be_greater_or_equal(l_before);
    end;

    procedure test_audit_insert_rows is
        l_rows   test_table$tapi.rows_tab;
        l_before date := sysdate;
    begin
        l_rows := test_table$tapi.rows_tab(test_table$tapi.rt(pk => 1, number_t => 2),
                                           test_table$tapi.rt(pk => 2, number_t => 3));
        test_table$tapi.ins_rows(l_rows);
        ut.expect(l_rows(1).created_by).to_equal('me');
        ut.expect(l_rows(1).created_at).to_be_greater_or_equal(l_before);
        ut.expect(l_rows(1).modified_by).to_equal('me');
        ut.expect(cast(l_rows(1).modified_at as date)).to_be_greater_or_equal(l_before);
    end;

    procedure test_audit_update is
        l_pk       test_table$tapi.pk_t := 1;
        l_before   date := sysdate;
        l_tapi_ins test_table$tapi.rt;
        l_tapi_upd test_table$tapi.rt;
    begin
        l_tapi_ins := test_table$tapi.ins(test_table$tapi.rt(pk => l_pk, number_t => 2));
        update test_table a
           set a.modified_by = 'others'
         where a.pk = l_pk;
    
        l_tapi_upd          := l_tapi_ins;
        l_tapi_upd.number_t := 42;
        test_table$tapi.upd(l_tapi_upd);
    
        ut.expect(l_tapi_upd.created_by).to_equal(l_tapi_ins.created_by);
        ut.expect(l_tapi_upd.created_at).to_equal(l_tapi_ins.created_at);
        ut.expect(cast(l_tapi_upd.modified_at as date)).to_be_greater_or_equal(l_before);
        ut.expect(l_tapi_upd.modified_by).to_equal('me');
    end;

    procedure test_audit_update_rows is
        l_rows   test_table$tapi.rows_tab;
        l_before date := sysdate;
    begin
        l_rows := test_table$tapi.rows_tab(test_table$tapi.rt(pk => 1, number_t => 2),
                                           test_table$tapi.rt(pk => 2, number_t => 3));
        test_table$tapi.ins_rows(l_rows);
    
        l_rows(1).number_t := l_rows(1).number_t * 2;
        l_rows(2).number_t := l_rows(2).number_t * 2;
    
        test_table$tapi.upd_rows(l_rows);
    
        ut.expect(l_rows(2).created_by).to_equal(l_rows(2).created_by);
        ut.expect(l_rows(2).created_at).to_equal(l_rows(2).created_at);
        ut.expect(cast(l_rows(2).modified_at as date)).to_be_greater_or_equal(l_before);
        ut.expect(l_rows(2).modified_by).to_equal('me');
    end;

    procedure test_audit_merge_insert is
        l_pk     test_table$tapi.pk_t := 1;
        l_tapi   test_table$tapi.rt;
        l_before date := sysdate;
    begin
        test_table$tapi.upsert(test_table$tapi.rt(pk => l_pk, number_t => 2));
    
        l_tapi := test_table$tapi.sel(p_pk => l_pk);
        ut.expect(l_tapi.created_by).to_equal('me');
        ut.expect(l_tapi.created_at).to_be_greater_or_equal(l_before);
    end;

    procedure test_audit_merge_update is
        l_pk       test_table$tapi.pk_t := 1;
        l_before   date := sysdate;
        l_tapi_ins test_table$tapi.rt;
        l_tapi_upd test_table$tapi.rt;
    begin
        l_tapi_ins := test_table$tapi.ins(test_table$tapi.rt(pk => l_pk, number_t => 2));
        update test_table a
           set a.modified_by = 'others'
         where a.pk = l_pk;
    
        l_tapi_upd          := l_tapi_ins;
        l_tapi_upd.number_t := 42;
        test_table$tapi.upsert(l_tapi_upd);
    
        ut.expect(l_tapi_upd.created_by).to_equal(l_tapi_ins.created_by);
        ut.expect(l_tapi_upd.created_at).to_equal(l_tapi_ins.created_at);
        ut.expect(cast(l_tapi_upd.modified_at as date)).to_be_greater_or_equal(l_before);
        ut.expect(l_tapi_upd.modified_by).to_equal('me');
    end;

    procedure test_checksum is
        l_pk1              test_table$tapi.pk_t := 1;
        l_tapi             test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        l_checksum         test_table$tapi.checksum_t;
        l_checksum_changed test_table$tapi.checksum_t;
    begin
        l_checksum := test_table$tapi.checksum(l_tapi);
    
        l_tapi.blob_t      := null;
        l_checksum_changed := test_table$tapi.checksum(l_tapi);
    
        ut.expect(l_checksum).not_to_equal(l_checksum_changed);
    end;

    procedure test_select_opt is
        l_pk1        test_table$tapi.pk_t := 1;
        l_unique_col test_table$tapi.varchar2_t_t := '1';
        l_tapi       test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk         => l_pk1,
                                                                                           varchar2_t => l_unique_col));
        l_tapi_opt   test_table$tapi.rt_opt;
    begin
        l_tapi_opt := test_table$tapi.sel_opt(p_pk => l_pk1);
        ut.expect(l_tapi_opt.pk).to_equal(l_pk1);
    
        l_tapi_opt := test_table$tapi.sel_unique_col_opt(p_varchar2_t => l_unique_col);
        ut.expect(l_tapi_opt.varchar2_t).to_equal(l_unique_col);
    end;

    procedure test_update_opt is
        l_pk1      test_table$tapi.pk_t := 1;
        l_number   number := 999999999;
        l_tapi     test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        l_tapi_opt test_table$tapi.rt_opt := test_table$tapi.sel_opt(p_pk => l_pk1);
    begin
        l_tapi_opt.number_t := l_number;
        test_table$tapi.upd_opt(l_tapi_opt);
    
        l_tapi := test_table$tapi.sel(p_pk => l_pk1);
        ut.expect(l_tapi.number_t).to_equal(l_number);
    end;

    procedure test_update_opt_not_found is
        l_pk1      test_table$tapi.pk_t := 1;
        l_number   number := 999999999;
        l_tapi     test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        l_tapi_opt test_table$tapi.rt_opt := test_table$tapi.sel_opt(p_pk => l_pk1);
    begin
        test_table$tapi.del(l_tapi_opt.pk);
        test_table$tapi.upd_opt(l_tapi_opt);
    end;

    procedure test_update_opt_changed is
        l_pk1      test_table$tapi.pk_t := 1;
        l_number   number := 999999999;
        l_tapi     test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        l_tapi_opt test_table$tapi.rt_opt := test_table$tapi.sel_opt(p_pk => l_pk1);
    begin
        l_tapi.number_t := '111111111';
        test_table$tapi.upd(l_tapi);
    
        l_tapi_opt.number_t := l_number;
        test_table$tapi.upd_opt(l_tapi_opt);
    end;

    procedure test_to_json is
        l_pk1  test_table$tapi.pk_t := 1;
        l_tapi test_table$tapi.rt := test_table$tapi.rt_defaults(pk => l_pk1);
        l_json json_object_t;
    begin
        l_json := test_table$tapi.json_obj(l_tapi);
    
        ut.expect(l_json.get_number('pk')).to_equal(l_pk1);
    end;

    procedure test_of_json is
        l_pk1    test_table$tapi.pk_t := 1;
        l_tapi   test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        l_json_1 json_object_t := test_table$tapi.json_obj(l_tapi);
        l_json_2 json_object_t;
    begin
        l_tapi := test_table$tapi.of_json(l_json_1);
    
        l_json_2 := test_table$tapi.json_obj(l_tapi);
        ut.expect(l_json_1).to_equal(l_json_2);
    end;

    procedure test_to_json_array is
        l_rows       test_table$tapi.rows_tab;
        l_json_array json_array_t;
    begin
        l_rows       := test_table$tapi.rows_tab(test_table$tapi.rt_defaults(pk => '1'),
                                                 test_table$tapi.rt_defaults(pk => '2'));
        l_json_array := test_table$tapi.json_arr(l_rows);
    
        ut.expect(l_json_array.get_size()).to_equal(2);
    end;

    procedure test_diff is
        l_pk1      test_table$tapi.pk_t := 1;
        l_tapi_1   test_table$tapi.rt := test_table$tapi.rt_defaults(pk => l_pk1);
        l_tapi_2   test_table$tapi.rt := l_tapi_1;
        l_json     json_object_t;
        l_number_1 number := 111111111;
        l_number_2 number := 222222222;
    begin
        l_tapi_1.number_t := l_number_1;
        l_tapi_2.number_t := l_number_2;
        l_json            := test_table$tapi.diff(l_tapi_1, l_tapi_2);
    
        ut.expect(l_json.get_string('mode')).to_equal('update');
        ut.expect(l_json.get_object('old').get_number('number_t')).to_equal(l_number_1);
        ut.expect(l_json.get_object('new').get_number('number_t')).to_equal(l_number_2);
    end;

    procedure test_diff_equals is
        l_pk1  test_table$tapi.pk_t := 1;
        l_rec  test_table$tapi.rt := test_table$tapi.rt_defaults(pk => l_pk1);
        l_json json_object_t;
        l_list json_key_list;
    begin
        l_json := test_table$tapi.diff(test_table$tapi.rt(), null);
        l_list := l_json.get_keys;
        ut.expect(l_list.count).to_equal(0);
    
        l_json := test_table$tapi.diff(l_rec, l_rec);
        l_list := l_json.get_keys;
        ut.expect(l_list.count).to_equal(0);
    end;

    procedure test_diff_delete is
        l_pk1  test_table$tapi.pk_t := 1;
        l_rec  test_table$tapi.rt := test_table$tapi.rt_defaults(pk => l_pk1);
        l_json json_object_t;
        l_list json_key_list;
    begin
        l_json := test_table$tapi.diff(l_rec, test_table$tapi.rt());
        ut.expect(l_json.get_string('mode')).to_equal('delete');
        ut.expect(l_json.has('new')).to_equal(false);
    end;

    procedure test_diff_insert is
        l_pk1  test_table$tapi.pk_t := 1;
        l_rec  test_table$tapi.rt := test_table$tapi.rt_defaults(pk => l_pk1);
        l_json json_object_t;
        l_list json_key_list;
    begin
        l_json := test_table$tapi.diff(test_table$tapi.rt(), l_rec);
        ut.expect(l_json.get_string('mode')).to_equal('insert');
        ut.expect(l_json.has('old')).to_equal(false);
    end;

    procedure test_undo_insert is
        l_pk1    test_table$tapi.pk_t := 1;
        l_rec    test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        l_diff   json_object_t;
        l_exists boolean;
    begin
        l_diff := test_table$tapi.diff(test_table$tapi.rt(), l_rec);
        test_table$tapi.undo(l_diff);
    
        l_exists := test_table$tapi.exist(p_pk => l_pk1);
        ut.expect(l_exists).to_be_false();
    end;

    procedure test_undo_delete is
        l_pk1    test_table$tapi.pk_t := 1;
        l_tapi_1 test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        l_tapi_2 test_table$tapi.rt;
        l_exists boolean;
        l_diff   json_object_t;
    begin
        test_table$tapi.del(l_pk1);
        l_exists := test_table$tapi.exists_sel(p_pk => l_pk1, p_rec => l_tapi_2);
        ut.expect(l_exists).to_be_false();
    
        l_diff := test_table$tapi.diff(l_tapi_1, l_tapi_2);
        test_table$tapi.undo(l_diff);
        l_exists := test_table$tapi.exists_sel(p_pk => l_pk1, p_rec => l_tapi_2);
        ut.expect(l_exists).to_be_true();
    end;

    procedure test_undo_update is
        l_pk1           test_table$tapi.pk_t := 1;
        l_tapi_1        test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        l_tapi_2        test_table$tapi.rt;
        l_old_num_value number;
        l_diff          json_object_t;
    begin
        l_tapi_2          := test_table$tapi.sel(p_pk => l_pk1);
        l_old_num_value   := l_tapi_2.number_t;
        l_tapi_2.number_t := '999999999';
        test_table$tapi.upd(l_tapi_2);
        l_diff := test_table$tapi.diff(l_tapi_1, l_tapi_2);
    
        test_table$tapi.undo(l_diff);
        l_tapi_2 := test_table$tapi.sel(p_pk => l_pk1);
        ut.expect(l_tapi_2.number_t).to_equal(l_old_num_value);
    
        l_tapi_2 := test_table$tapi.rt_defaults(pk => l_pk1);
        l_diff   := test_table$tapi.diff(l_tapi_1, l_tapi_2);
        test_table$tapi.undo(l_diff);
    end;

    procedure test_redo_insert is
        l_pk1         test_table$tapi.pk_t := 1;
        l_rec         test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        l_diff_insert json_object_t;
    begin
        l_diff_insert := test_table$tapi.diff(test_table$tapi.rt(), l_rec);
        test_table$tapi.undo(l_diff_insert);
        ut.expect(test_table$tapi.exist(p_pk => l_pk1)).to_be_false();
    
        test_table$tapi.redo(l_diff_insert);
        ut.expect(test_table$tapi.exist(p_pk => l_pk1)).to_be_true();
    end;

    procedure test_redo_delete is
        l_pk1  test_table$tapi.pk_t := 1;
        l_rec  test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        l_diff json_object_t;
    begin
        l_diff := test_table$tapi.diff(l_rec, test_table$tapi.rt());
    
        test_table$tapi.redo(l_diff);
    
        ut.expect(test_table$tapi.exist(p_pk => l_pk1)).to_be_false();
    end;

    procedure test_redo_update is
        l_pk1     test_table$tapi.pk_t := 1;
        l_rec     test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
        l_rec_upd test_table$tapi.rt := l_rec;
        l_diff    json_object_t;
    begin
        l_rec_upd.varchar2_t                       := 'varchar2';
        l_rec_upd.char_t                           := 'c';
        l_rec_upd.nchar_t                          := 'n';
        l_rec_upd.nvarchar2_t                      := 'nvarchar2';
        l_rec_upd.number_t                         := 1234567890;
        l_rec_upd.float_t                          := to_number(1234567890);
        l_rec_upd.binary_float_t                   := to_number(1234567890);
        l_rec_upd.binary_double_t                  := to_number(1234567890);
        l_rec_upd.date_t                           := trunc(sysdate) - 10;
        l_rec_upd.timestamp_t                      := trunc(systimestamp);
        l_rec_upd.timestamp_with_local_time_zone_t := trunc(systimestamp) - 10;
        l_rec_upd.timestamp_with_time_zone_t       := trunc(systimestamp) - 10;
        l_rec_upd.interval_year_to_month_t         := (systimestamp - to_date('1970', 'YYYY')) year(9) to month;
        l_rec_upd.interval_day_to_second_t         := (systimestamp - to_date(to_char(sysdate - 2, 'YYYY'), 'YYYY'))
                                                      day(9) to second;
        l_rec_upd.blob_t                           := empty_blob();
        l_rec_upd.clob_t                           := empty_clob();
        l_rec_upd.nclob_t                          := 'nclob';
        l_rec_upd.raw_t                            := hextoraw('45D');
        l_rec_upd.bool_t                           := false;
        l_rec_upd.created_by                       := 'other';
        l_rec_upd.created_at                       := trunc(sysdate) - 10;
        l_rec_upd.modified_by                      := 'other';
        l_rec_upd.modified_at                      := trunc(sysdate) - 10;
        l_diff                                     := test_table$tapi.diff(l_rec_upd, l_rec);
        test_table$tapi.undo(l_diff);
    
        test_table$tapi.redo(l_diff);
    end;

end;
/
