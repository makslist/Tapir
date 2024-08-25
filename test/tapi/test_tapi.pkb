create or replace package body test_tapi is

   procedure test_pk_str is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_str  varchar2(32767);
   begin
      l_str := tapir_all_types$crud.pk_str(tapir_all_types$crud.rt_defaults(pk => l_pk1));
   
      ut.expect(l_str).to_equal(to_char(l_pk1));
   end;

   procedure test_insert_with_custom_defaults is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt;
   begin
      l_tapi := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
   
      ut.expect(tapir_all_types$crud.counts(p_pk => l_pk1)).to_equal(1);
   end;

   procedure test_insert_dup_val_on_index is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt;
   begin
      l_tapi := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
      l_tapi := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
   end;

   procedure test_parameter_is_null is
      l_tapi tapir_all_types$crud.rt;
   begin
      l_tapi := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => null));
   end;

   procedure test_insert_rows is
      l_rows tapir_all_types$crud.rows_tab;
   begin
      l_rows := tapir_all_types$crud.rows_tab(tapir_all_types$crud.rt_defaults(pk => '1'),
                                              tapir_all_types$crud.rt_defaults(pk => '2'));
      tapir_all_types$crud.ins_rows(l_rows);
   
      ut.expect(tapir_all_types$crud.counts()).to_equal(2);
   end;

   procedure test_ins_cursor is
      l_pk1 tapir_all_types$crud.pk_t := 1;
      l_cur tapir_all_types$crud.strong_ref_cursor;
   begin
      open l_cur for
         select l_pk1 as pk
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
               ,null as created_by_t
               ,null as created_at_t
               ,null as modified_by_t
               ,null as modified_at_t
           from dual;
      tapir_all_types$crud.ins_cur(l_cur);
   
      ut.expect(tapir_all_types$crud.counts(p_pk => l_pk1)).to_equal(1);
   end;

   procedure test_insert_rows_return_errors is
      l_pk1        tapir_all_types$crud.pk_t := 1;
      l_unique_row tapir_all_types$crud.rt := tapir_all_types$crud.rt_defaults(pk => l_pk1);
      l_rows       tapir_all_types$crud.rows_tab;
      l_errors     tapir_all_types$crud.rows_tab;
   begin
      tapir_all_types$crud.ins(l_unique_row);
      l_rows := tapir_all_types$crud.rows_tab(l_unique_row, tapir_all_types$crud.rt_defaults(pk => '2'));
      l_rows := tapir_all_types$crud.ins_rows(l_rows, l_errors);

      ut.expect(l_rows.count).to_equal(1);
      ut.expect(l_errors.count).to_equal(1);
      ut.expect(tapir_all_types$crud.counts()).to_equal(2);
   end;

   procedure test_exists is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
      l_exists boolean;
   begin
      l_exists := tapir_all_types$crud.exist(l_tapi);

      ut.expect(l_exists).to_equal(true);
   end;

   procedure test_exists_select is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
      l_exists boolean;
   begin
      l_exists := tapir_all_types$crud.exists_sel(l_tapi);

      ut.expect(l_exists).to_equal(true);
   end;

   procedure test_select is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
   begin
      tapir_all_types$crud.sel(l_tapi);
      l_tapi := tapir_all_types$crud.sel(l_tapi);

      ut.expect(l_tapi.pk).to_equal(l_pk1);
   end;

   procedure test_select_no_data_found is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt;
   begin
      l_tapi := tapir_all_types$crud.sel(p_pk => l_pk1);
   end;

   procedure test_select_for_update is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
   begin   
      tapir_all_types$crud.sel_lock(l_tapi);
      l_tapi := tapir_all_types$crud.sel_lock(l_tapi);

      ut.expect(l_tapi.pk).to_equal(l_pk1);
   end;

   procedure test_select_for_update_no_data_found is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt;
   begin
      l_tapi := tapir_all_types$crud.sel_lock(p_pk => l_pk1);
   end;

   procedure test_select_rows is
      l_pk1        tapir_all_types$crud.pk_t := 1;
      l_rows       tapir_all_types$crud.rows_tab;
      l_errors     tapir_all_types$crud.rows_tab;
      l_cur tapir_all_types$crud.strong_ref_cursor;
   begin
      l_rows := tapir_all_types$crud.rows_tab(tapir_all_types$crud.rt_defaults(pk => l_pk1), tapir_all_types$crud.rt_defaults(pk => '2'));
      l_rows := tapir_all_types$crud.ins_rows(l_rows, l_errors);
   
      open l_cur for
         select *
           from test_table_all_types
          where pk = l_pk1;
      l_rows := tapir_all_types$crud.sel_rows(l_cur);
   
      ut.expect(l_rows.count).to_equal(1);
   end;

   procedure test_update is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_number number := 999999999;
      l_tapi tapir_all_types$crud.rt;
   begin
      l_tapi := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
      l_tapi.number_t := l_number;
      tapir_all_types$crud.upd(l_tapi);
   
      l_tapi := tapir_all_types$crud.sel(p_pk => l_pk1);
      ut.expect(l_tapi.number_t).to_equal(l_number);
   end;

   procedure test_update_rows is
      l_row_1 tapir_all_types$crud.rt := tapir_all_types$crud.rt_defaults(pk => '1');
      l_row_2 tapir_all_types$crud.rt := tapir_all_types$crud.rt_defaults(pk => '2');
      l_number number := 999999999;
      l_rows tapir_all_types$crud.rows_tab;
      l_cur tapir_all_types$crud.strong_ref_cursor;
   begin
      tapir_all_types$crud.ins(l_row_1);
      tapir_all_types$crud.ins(l_row_2);
      
      l_row_1.number_t := l_number;
      l_row_2.number_t := l_number;
      l_rows := tapir_all_types$crud.rows_tab(l_row_1, l_row_2);
      tapir_all_types$crud.upd_rows(l_rows);

      open l_cur for
         select *
           from test_table_all_types
          where number_t = l_number;
      l_rows := tapir_all_types$crud.sel_rows(l_cur);
      ut.expect(l_rows.count).to_equal(2);
   end;

   procedure test_merge is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_rows tapir_all_types$crud.rows_tab;
      l_number number := 999999999;
      l_tapi tapir_all_types$crud.rt;
   begin
      ut.expect(tapir_all_types$crud.counts()).to_equal(0);
      tapir_all_types$crud.upsert(tapir_all_types$crud.rt_defaults(pk => l_pk1));

      ut.expect(tapir_all_types$crud.counts()).to_equal(1);
      
      tapir_all_types$crud.upsert(tapir_all_types$crud.rt_defaults(pk => l_pk1, number_t => l_number));
   
      l_tapi := tapir_all_types$crud.sel(p_pk => l_pk1);
      ut.expect(l_tapi.number_t).to_equal(l_number);
   end;

   procedure test_delete is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt;
   begin
      l_tapi := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
      ut.expect(tapir_all_types$crud.counts(p_pk => l_pk1)).to_equal(1);

      tapir_all_types$crud.del(l_tapi);
      ut.expect(tapir_all_types$crud.counts(p_pk => l_pk1)).to_equal(0);
   end;

   procedure test_delete_no_data_found is
      l_pk1  tapir_all_types$crud.pk_t := 1;
   begin
      tapir_all_types$crud.del(p_pk=> l_pk1);
   end;

   procedure test_audit_insert is
      l_tapi tapir_all_types$crud.rt;
      l_before date := sysdate;
   begin
      l_tapi := tapir_all_types$crud.ins(tapir_all_types$crud.rt(pk => 1, number_t => 2));
      ut.expect(l_tapi.created_by).to_equal('me');
      ut.expect(l_tapi.created_at).to_be_greater_or_equal(l_before);
      ut.expect(l_tapi.modified_by).to_equal('me');
      ut.expect(cast(l_tapi.modified_at as date)).to_be_greater_or_equal(l_before);
   end;

   procedure test_audit_insert_rows is
      l_rows tapir_all_types$crud.rows_tab;
      l_before date := sysdate;
   begin
      l_rows := tapir_all_types$crud.rows_tab(tapir_all_types$crud.rt(pk => 1, number_t => 2)
                                              ,tapir_all_types$crud.rt(pk => 2, number_t => 3));
      tapir_all_types$crud.ins_rows(l_rows);
      ut.expect(l_rows(1).created_by).to_equal('me');
      ut.expect(l_rows(1).created_at).to_be_greater_or_equal(l_before);
      ut.expect(l_rows(1).modified_by).to_equal('me');
      ut.expect(cast(l_rows(1).modified_at as date)).to_be_greater_or_equal(l_before);
   end;

   procedure test_audit_update is
      l_pk   tapir_all_types$crud.pk_t := 1;
      l_before date := sysdate;
      l_tapi_ins tapir_all_types$crud.rt;
      l_tapi_upd tapir_all_types$crud.rt;
   begin
      l_tapi_ins := tapir_all_types$crud.ins(tapir_all_types$crud.rt(pk => l_pk, number_t => 2));
      update test_table_all_types a
         set a.modified_by = 'others'
       where a.pk = l_pk;
      
      l_tapi_upd := l_tapi_ins;
      l_tapi_upd.number_t := 42;
      tapir_all_types$crud.upd(l_tapi_upd);

      ut.expect(l_tapi_upd.created_by).to_equal(l_tapi_ins.created_by);
      ut.expect(l_tapi_upd.created_at).to_equal(l_tapi_ins.created_at);
      ut.expect(cast(l_tapi_upd.modified_at as date)).to_be_greater_or_equal(l_before);
      ut.expect(l_tapi_upd.modified_by).to_equal('me');
   end;

   procedure test_audit_update_rows is
      l_rows tapir_all_types$crud.rows_tab;
      l_before date := sysdate;
   begin
      l_rows := tapir_all_types$crud.rows_tab(tapir_all_types$crud.rt(pk => 1, number_t => 2)
                                              ,tapir_all_types$crud.rt(pk => 2, number_t => 3));
      tapir_all_types$crud.ins_rows(l_rows);

      l_rows(1).number_t := l_rows(1).number_t * 2;
      l_rows(2).number_t := l_rows(2).number_t * 2;

      tapir_all_types$crud.upd_rows(l_rows);

      ut.expect(l_rows(2).created_by).to_equal(l_rows(2).created_by);
      ut.expect(l_rows(2).created_at).to_equal(l_rows(2).created_at);
      ut.expect(cast(l_rows(2).modified_at as date)).to_be_greater_or_equal(l_before);
      ut.expect(l_rows(2).modified_by).to_equal('me');
   end;

   procedure test_audit_merge_insert is
      l_pk   tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt;
      l_before date := sysdate;
   begin
      tapir_all_types$crud.upsert(tapir_all_types$crud.rt(pk => l_pk, number_t => 2));

      l_tapi := tapir_all_types$crud.sel(p_pk => l_pk);
      ut.expect(l_tapi.created_by).to_equal('me');
      ut.expect(l_tapi.created_at).to_be_greater_or_equal(l_before);
   end;

   procedure test_audit_merge_update is
      l_pk   tapir_all_types$crud.pk_t := 1;
      l_before date := sysdate;
      l_tapi_ins tapir_all_types$crud.rt;
      l_tapi_upd tapir_all_types$crud.rt;
   begin
      l_tapi_ins := tapir_all_types$crud.ins(tapir_all_types$crud.rt(pk => l_pk, number_t => 2));
      update test_table_all_types a
         set a.modified_by = 'others'
       where a.pk = l_pk;
      
      l_tapi_upd := l_tapi_ins;
      l_tapi_upd.number_t := 42;
      tapir_all_types$crud.upsert(l_tapi_upd);

      ut.expect(l_tapi_upd.created_by).to_equal(l_tapi_ins.created_by);
      ut.expect(l_tapi_upd.created_at).to_equal(l_tapi_ins.created_at);
      ut.expect(cast(l_tapi_upd.modified_at as date)).to_be_greater_or_equal(l_before);
      ut.expect(l_tapi_upd.modified_by).to_equal('me');
   end;

   procedure test_checksum is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
      l_checksum tapir_all_types$crud.checksum_t;
      l_checksum_changed tapir_all_types$crud.checksum_t;
   begin
      l_checksum := tapir_all_types$crud.checksum(l_tapi);

      l_tapi.blob_t := null;
      l_checksum_changed := tapir_all_types$crud.checksum(l_tapi);

      ut.expect(l_checksum).not_to_equal(l_checksum_changed);
   end;

   procedure test_select_opt is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
      l_tapi_opt tapir_all_types$crud.rt_opt;
   begin
      l_tapi_opt := tapir_all_types$crud.sel_opt(p_pk => l_pk1);

      ut.expect(l_tapi_opt.pk).to_equal(l_pk1);
   end;

   procedure test_update_opt is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_number number := 999999999;
      l_tapi tapir_all_types$crud.rt := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
      l_tapi_opt tapir_all_types$crud.rt_opt := tapir_all_types$crud.sel_opt(p_pk => l_pk1);
   begin
      l_tapi_opt.number_t := l_number;
      tapir_all_types$crud.upd_opt(l_tapi_opt);
   
      l_tapi := tapir_all_types$crud.sel(p_pk => l_pk1);
      ut.expect(l_tapi.number_t).to_equal(l_number);
   end;

   procedure test_update_opt_not_found is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_number number := 999999999;
      l_tapi tapir_all_types$crud.rt := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
      l_tapi_opt tapir_all_types$crud.rt_opt := tapir_all_types$crud.sel_opt(p_pk => l_pk1);
   begin
      tapir_all_types$crud.del(l_tapi_opt.pk);
      tapir_all_types$crud.upd_opt(l_tapi_opt);
   end;

   procedure test_update_opt_changed is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_number number := 999999999;
      l_tapi tapir_all_types$crud.rt := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
      l_tapi_opt tapir_all_types$crud.rt_opt := tapir_all_types$crud.sel_opt(p_pk => l_pk1);
   begin
      l_tapi.number_t := '111111111';
      tapir_all_types$crud.upd(l_tapi);

      l_tapi_opt.number_t := l_number;
      tapir_all_types$crud.upd_opt(l_tapi_opt);
   end;

   procedure test_to_json is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt := tapir_all_types$crud.rt_defaults(pk => l_pk1);
      l_json json_object_t;
   begin
      l_json := tapir_all_types$crud.json_obj(l_tapi);

      ut.expect(l_json.get_number('pk')).to_equal(l_pk1);
   end;

   procedure test_of_json is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt := tapir_all_types$crud.ins(tapir_all_types$crud.rt_defaults(pk => l_pk1));
      l_json_1 json_object_t := tapir_all_types$crud.json_obj(l_tapi);
      l_json_2 json_object_t;
   begin
      l_tapi := tapir_all_types$crud.of_json(l_json_1);

      l_json_2 := tapir_all_types$crud.json_obj(l_tapi);
      ut.expect(l_json_1).to_equal(l_json_2);
   end;

   procedure test_diff is
      l_pk1  tapir_all_types$crud.pk_t := 1;
      l_tapi_1 tapir_all_types$crud.rt := tapir_all_types$crud.rt_defaults(pk => l_pk1);
      l_tapi_2 tapir_all_types$crud.rt := l_tapi_1;
      l_json json_object_t;
      l_number_1 number := 111111111;
      l_number_2 number := 222222222;
   begin
      l_tapi_1.number_t := l_number_1;
      l_tapi_2.number_t := l_number_2;
      l_json := tapir_all_types$crud.diff(l_tapi_1, l_tapi_2);

      ut.expect(l_json.get_string('mode')).to_equal('update');
      ut.expect(l_json.get_object('old').get_number('number_t')).to_equal(l_number_1);
      ut.expect(l_json.get_object('new').get_number('number_t')).to_equal(l_number_2);
   end;

end;
/
