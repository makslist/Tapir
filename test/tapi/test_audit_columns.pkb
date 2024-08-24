create or replace package body test_audit_columns is

   procedure test_insert is
      l_tapi tapir_all_types$crud.rt;
      l_before date := sysdate;
   begin
      l_tapi := tapir_all_types$crud.ins(tapir_all_types$crud.rt(pk => 1, number_t => 2));
      ut.expect(l_tapi.created_by).to_equal('me');
      ut.expect(l_tapi.created_at).to_be_greater_or_equal(l_before);
      ut.expect(l_tapi.modified_by).to_equal('me');
      ut.expect(cast(l_tapi.modified_at as date)).to_be_greater_or_equal(l_before);
   end;

   procedure test_insert_rows is
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

   procedure test_update is
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

   procedure test_update_rows is
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

   procedure test_merge_insert is
      l_pk   tapir_all_types$crud.pk_t := 1;
      l_tapi tapir_all_types$crud.rt;
      l_before date := sysdate;
   begin
      tapir_all_types$crud.upsert(tapir_all_types$crud.rt(pk => l_pk, number_t => 2));

      l_tapi := tapir_all_types$crud.sel(p_pk => l_pk);
      ut.expect(l_tapi.created_by).to_equal('me');
      ut.expect(l_tapi.created_at).to_be_greater_or_equal(l_before);
   end;

   procedure test_merge_update is
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

end;
/
