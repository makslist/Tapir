create or replace package body test_tapi is

   procedure test_pk_str is
      l_pk1  test_table$tapi.pk_t := 1;
      l_str  varchar2(32767);
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

   procedure test_insert_dup_val_on_index is
      l_pk1  test_table$tapi.pk_t := 1;
      l_tapi test_table$tapi.rt;
   begin
      l_tapi := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
      l_tapi := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
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
         select l_pk1 as pk
               ,null as identity_column
               ,sys.dbms_random.string('L', round(sys.dbms_random.value(1, 100))) as t_varchar2
               ,sys.dbms_random.string('L', 1) as t_char
               ,sys.dbms_random.string('L', 1) as t_nchar
               ,sys.dbms_random.string('L', round(sys.dbms_random.value(1, 100))) as t_nvarchar2
               ,round(sys.dbms_random.value(1, 1024 * 1024)) as t_number
               ,to_number(2) as t_float
               ,round(sys.dbms_random.value(1, 1024 * 1024)) as t_binary_float
               ,round(sys.dbms_random.value(1, 1024 * 1024)) as t_binary_double
               ,sysdate as t_date
               ,systimestamp as t_timestamp
               ,systimestamp as t_timestamp_with_local_time_zone
               ,systimestamp as t_timestamp_with_time_zone
               ,(systimestamp - to_date('2024', 'YYYY')) year(9) to month as t_interval_year_to_month
               ,(systimestamp - to_date('2024', 'YYYY')) day(9) to second as t_interval_day_to_second_t
               ,utl_raw.cast_to_raw('blob') as t_blob
               ,substr(sys_guid(), 1, 20) as t_clob
               ,substr(sys_guid(), 1, 20) as t_nclob
               ,utl_raw.cast_to_raw('raw') as t_raw
               ,true as t_bool
               ,null as virtual_col
               ,null as created_by_t
               ,null as created_at_t
               ,null as modified_by_t
               ,null as modified_at_t
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

   procedure test_exists is
      l_pk1  test_table$tapi.pk_t := 1;
      l_unique_col  test_table$tapi.pk_t := '1';
      l_tapi test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1, varchar2_t => l_unique_col));
      l_exists boolean;
   begin
      l_exists := test_table$tapi.exist(l_tapi);
      ut.expect(l_exists).to_equal(true);

      l_exists := test_table$tapi.exist_unique_col(p_varchar2_t => l_unique_col);
      ut.expect(l_exists).to_equal(true);
   end;

   procedure test_counts is
      l_pk1  test_table$tapi.pk_t := 1;
      l_unique_col  test_table$tapi.pk_t := '1';
      l_tapi test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1, varchar2_t => l_unique_col));
      l_count number;
   begin
      l_count := test_table$tapi.counts(p_pk => l_pk1);
      ut.expect(l_count).to_equal(1);

      l_count := test_table$tapi.counts_unique_col(p_varchar2_t => l_unique_col);
      ut.expect(l_count).to_equal(1);
   end;

   procedure test_exists_select is
      l_pk1  test_table$tapi.pk_t := 1;
      l_unique_col  test_table$tapi.pk_t := '1';
      l_tapi test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1, varchar2_t => l_unique_col));
      l_exists boolean;
   begin
      l_exists := test_table$tapi.exists_sel(l_tapi);
      ut.expect(l_exists).to_equal(true);

      l_exists := test_table$tapi.exists_sel_unique_col(p_varchar2_t => l_unique_col, p_rec => l_tapi);
      ut.expect(l_exists).to_equal(true);
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
      l_pk1  test_table$tapi.pk_t := 1;
      l_unique_col  test_table$tapi.varchar2_t_t := '1';
      l_tapi test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1, varchar2_t => l_unique_col));
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
      l_pk1  test_table$tapi.pk_t := 1;
      l_unique_col  test_table$tapi.varchar2_t_t := '1';
      l_tapi test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1, varchar2_t => l_unique_col));
   begin   
      l_tapi := test_table$tapi.sel_lock_unique_col(p_varchar2_t => l_unique_col);
      ut.expect(l_tapi.varchar2_t).to_equal(l_unique_col);
   end;

   procedure test_select_unique_col_for_update_no_data_found is
      l_pk1  test_table$tapi.pk_t := 1;
      l_unique_col  test_table$tapi.varchar2_t_t := '1';
      l_tapi test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1, varchar2_t => l_unique_col));
   begin   
      l_tapi := test_table$tapi.sel_lock_unique_col(p_varchar2_t => 'aaa');
   end;

   procedure test_select_rows is
      l_pk1        test_table$tapi.pk_t := 1;
      l_rows       test_table$tapi.rows_tab;
      l_errors     test_table$tapi.rows_tab;
      l_cur test_table$tapi.strong_ref_cursor;
   begin
      l_rows := test_table$tapi.rows_tab(test_table$tapi.rt_defaults(pk => l_pk1), test_table$tapi.rt_defaults(pk => '2'));
      l_rows := test_table$tapi.ins_rows(l_rows, l_errors);
   
      open l_cur for
         select *
           from test_table
          where pk = l_pk1;
      l_rows := test_table$tapi.sel_rows(l_cur);
   
      ut.expect(l_rows.count).to_equal(1);
   end;

   procedure test_update is
      l_pk1  test_table$tapi.pk_t := 1;
      l_number number := 999999999;
      l_tapi test_table$tapi.rt;
   begin
      l_tapi := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
      l_tapi.number_t := l_number;
      test_table$tapi.upd(l_tapi);
   
      l_tapi := test_table$tapi.sel(p_pk => l_pk1);
      ut.expect(l_tapi.number_t).to_equal(l_number);
   end;

   procedure test_update_rows is
      l_row_1 test_table$tapi.rt := test_table$tapi.rt_defaults(pk => '1');
      l_row_2 test_table$tapi.rt := test_table$tapi.rt_defaults(pk => '2');
      l_number number := 999999999;
      l_rows test_table$tapi.rows_tab;
      l_cur test_table$tapi.strong_ref_cursor;
   begin
      test_table$tapi.ins(l_row_1);
      test_table$tapi.ins(l_row_2);
      
      l_row_1.number_t := l_number;
      l_row_2.number_t := l_number;
      l_rows := test_table$tapi.rows_tab(l_row_1, l_row_2);
      test_table$tapi.upd_rows(l_rows);

      open l_cur for
         select *
           from test_table
          where number_t = l_number;
      l_rows := test_table$tapi.sel_rows(l_cur);
      ut.expect(l_rows.count).to_equal(2);
   end;

   procedure test_merge is
      l_pk1  test_table$tapi.pk_t := 1;
      l_rows test_table$tapi.rows_tab;
      l_number number := 999999999;
      l_tapi test_table$tapi.rt;
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
      l_pk1  test_table$tapi.pk_t := 1;
      l_unique_col  test_table$tapi.varchar2_t_t := '1';
      l_tapi test_table$tapi.rt;
   begin
      l_tapi := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1, varchar2_t => l_unique_col));
      ut.expect(test_table$tapi.counts_unique_col(p_varchar2_t => l_unique_col)).to_equal(1);

      test_table$tapi.del_unique_col(p_varchar2_t => l_unique_col);
      ut.expect(test_table$tapi.counts_unique_col(p_varchar2_t => l_unique_col)).to_equal(0);
   end;

   procedure test_delete_no_data_found is
      l_pk1  test_table$tapi.pk_t := 1;
   begin
      test_table$tapi.del(p_pk=> l_pk1);
   end;

   procedure test_delete_unique_col_no_data_found is
      l_unique_col  test_table$tapi.varchar2_t_t := '1';
   begin
      test_table$tapi.del_unique_col(p_varchar2_t => l_unique_col);
   end;

   procedure test_audit_insert is
      l_tapi test_table$tapi.rt;
      l_before date := sysdate;
   begin
      l_tapi := test_table$tapi.ins(test_table$tapi.rt(pk => 1, number_t => 2));
      ut.expect(l_tapi.created_by).to_equal('me');
      ut.expect(l_tapi.created_at).to_be_greater_or_equal(l_before);
      ut.expect(l_tapi.modified_by).to_equal('me');
      ut.expect(cast(l_tapi.modified_at as date)).to_be_greater_or_equal(l_before);
   end;

   procedure test_audit_insert_rows is
      l_rows test_table$tapi.rows_tab;
      l_before date := sysdate;
   begin
      l_rows := test_table$tapi.rows_tab(test_table$tapi.rt(pk => 1, number_t => 2)
                                              ,test_table$tapi.rt(pk => 2, number_t => 3));
      test_table$tapi.ins_rows(l_rows);
      ut.expect(l_rows(1).created_by).to_equal('me');
      ut.expect(l_rows(1).created_at).to_be_greater_or_equal(l_before);
      ut.expect(l_rows(1).modified_by).to_equal('me');
      ut.expect(cast(l_rows(1).modified_at as date)).to_be_greater_or_equal(l_before);
   end;

   procedure test_audit_update is
      l_pk   test_table$tapi.pk_t := 1;
      l_before date := sysdate;
      l_tapi_ins test_table$tapi.rt;
      l_tapi_upd test_table$tapi.rt;
   begin
      l_tapi_ins := test_table$tapi.ins(test_table$tapi.rt(pk => l_pk, number_t => 2));
      update test_table a
         set a.modified_by = 'others'
       where a.pk = l_pk;
      
      l_tapi_upd := l_tapi_ins;
      l_tapi_upd.number_t := 42;
      test_table$tapi.upd(l_tapi_upd);

      ut.expect(l_tapi_upd.created_by).to_equal(l_tapi_ins.created_by);
      ut.expect(l_tapi_upd.created_at).to_equal(l_tapi_ins.created_at);
      ut.expect(cast(l_tapi_upd.modified_at as date)).to_be_greater_or_equal(l_before);
      ut.expect(l_tapi_upd.modified_by).to_equal('me');
   end;

   procedure test_audit_update_rows is
      l_rows test_table$tapi.rows_tab;
      l_before date := sysdate;
   begin
      l_rows := test_table$tapi.rows_tab(test_table$tapi.rt(pk => 1, number_t => 2)
                                              ,test_table$tapi.rt(pk => 2, number_t => 3));
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
      l_pk   test_table$tapi.pk_t := 1;
      l_tapi test_table$tapi.rt;
      l_before date := sysdate;
   begin
      test_table$tapi.upsert(test_table$tapi.rt(pk => l_pk, number_t => 2));

      l_tapi := test_table$tapi.sel(p_pk => l_pk);
      ut.expect(l_tapi.created_by).to_equal('me');
      ut.expect(l_tapi.created_at).to_be_greater_or_equal(l_before);
   end;

   procedure test_audit_merge_update is
      l_pk   test_table$tapi.pk_t := 1;
      l_before date := sysdate;
      l_tapi_ins test_table$tapi.rt;
      l_tapi_upd test_table$tapi.rt;
   begin
      l_tapi_ins := test_table$tapi.ins(test_table$tapi.rt(pk => l_pk, number_t => 2));
      update test_table a
         set a.modified_by = 'others'
       where a.pk = l_pk;
      
      l_tapi_upd := l_tapi_ins;
      l_tapi_upd.number_t := 42;
      test_table$tapi.upsert(l_tapi_upd);

      ut.expect(l_tapi_upd.created_by).to_equal(l_tapi_ins.created_by);
      ut.expect(l_tapi_upd.created_at).to_equal(l_tapi_ins.created_at);
      ut.expect(cast(l_tapi_upd.modified_at as date)).to_be_greater_or_equal(l_before);
      ut.expect(l_tapi_upd.modified_by).to_equal('me');
   end;

   procedure test_checksum is
      l_pk1  test_table$tapi.pk_t := 1;
      l_tapi test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
      l_checksum test_table$tapi.checksum_t;
      l_checksum_changed test_table$tapi.checksum_t;
   begin
      l_checksum := test_table$tapi.checksum(l_tapi);

      l_tapi.blob_t := null;
      l_checksum_changed := test_table$tapi.checksum(l_tapi);

      ut.expect(l_checksum).not_to_equal(l_checksum_changed);
   end;

   procedure test_select_opt is
      l_pk1  test_table$tapi.pk_t := 1;
      l_tapi test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
      l_tapi_opt test_table$tapi.rt_opt;
   begin
      l_tapi_opt := test_table$tapi.sel_opt(p_pk => l_pk1);

      ut.expect(l_tapi_opt.pk).to_equal(l_pk1);
   end;

   procedure test_update_opt is
      l_pk1  test_table$tapi.pk_t := 1;
      l_number number := 999999999;
      l_tapi test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
      l_tapi_opt test_table$tapi.rt_opt := test_table$tapi.sel_opt(p_pk => l_pk1);
   begin
      l_tapi_opt.number_t := l_number;
      test_table$tapi.upd_opt(l_tapi_opt);
   
      l_tapi := test_table$tapi.sel(p_pk => l_pk1);
      ut.expect(l_tapi.number_t).to_equal(l_number);
   end;

   procedure test_update_opt_not_found is
      l_pk1  test_table$tapi.pk_t := 1;
      l_number number := 999999999;
      l_tapi test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
      l_tapi_opt test_table$tapi.rt_opt := test_table$tapi.sel_opt(p_pk => l_pk1);
   begin
      test_table$tapi.del(l_tapi_opt.pk);
      test_table$tapi.upd_opt(l_tapi_opt);
   end;

   procedure test_update_opt_changed is
      l_pk1  test_table$tapi.pk_t := 1;
      l_number number := 999999999;
      l_tapi test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
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
      l_pk1  test_table$tapi.pk_t := 1;
      l_tapi test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
      l_json_1 json_object_t := test_table$tapi.json_obj(l_tapi);
      l_json_2 json_object_t;
   begin
      l_tapi := test_table$tapi.of_json(l_json_1);

      l_json_2 := test_table$tapi.json_obj(l_tapi);
      ut.expect(l_json_1).to_equal(l_json_2);
   end;

   procedure test_to_json_array is
      l_rows test_table$tapi.rows_tab;
      l_json_array json_array_t;
   begin
      l_rows := test_table$tapi.rows_tab(test_table$tapi.rt_defaults(pk => '1'),
                                         test_table$tapi.rt_defaults(pk => '2'));
      l_json_array := test_table$tapi.json_arr(l_rows);

      ut.expect(l_json_array.get_size()).to_equal(2);
   end;

   procedure test_diff is
      l_pk1  test_table$tapi.pk_t := 1;
      l_tapi_1 test_table$tapi.rt := test_table$tapi.rt_defaults(pk => l_pk1);
      l_tapi_2 test_table$tapi.rt := l_tapi_1;
      l_json json_object_t;
      l_number_1 number := 111111111;
      l_number_2 number := 222222222;
   begin
      l_tapi_1.number_t := l_number_1;
      l_tapi_2.number_t := l_number_2;
      l_json := test_table$tapi.diff(l_tapi_1, l_tapi_2);

      ut.expect(l_json.get_string('mode')).to_equal('update');
      ut.expect(l_json.get_object('old').get_number('number_t')).to_equal(l_number_1);
      ut.expect(l_json.get_object('new').get_number('number_t')).to_equal(l_number_2);
   end;

   procedure test_diff_null_objects is
      l_pk1  test_table$tapi.pk_t := 1;
      l_tapi_1 test_table$tapi.rt := test_table$tapi.rt_defaults(pk => l_pk1);
      l_tapi_2 test_table$tapi.rt := test_table$tapi.rt();
      l_json json_object_t;
      l_list json_key_list;
   begin
      l_json := test_table$tapi.diff(l_tapi_2, l_tapi_2);
      l_list := l_json.get_keys;
      ut.expect(l_list.count).to_equal(0);

      l_json := test_table$tapi.diff(l_tapi_1, l_tapi_2);
      ut.expect(l_json.get_string('mode')).to_equal('delete');
      ut.expect(l_json.has('new')).to_equal(false);

      l_json := test_table$tapi.diff(l_tapi_2, l_tapi_1);
      ut.expect(l_json.get_string('mode')).to_equal('insert');
      ut.expect(l_json.has('old')).to_equal(false);
   end;

   procedure test_diff_undo is
      l_pk1  test_table$tapi.pk_t := 1;
      l_tapi_1 test_table$tapi.rt := test_table$tapi.ins(test_table$tapi.rt_defaults(pk => l_pk1));
      l_tapi_2 test_table$tapi.rt;
      l_diff json_object_t;
   begin
      l_tapi_2 := test_table$tapi.sel(p_pk => l_pk1);
      l_tapi_2.number_t := '999999999';
      test_table$tapi.upd(l_tapi_2);
      l_diff := test_table$tapi.diff(l_tapi_1, l_tapi_2);
      test_table$tapi.undo(l_diff);
   end;

end;
/
