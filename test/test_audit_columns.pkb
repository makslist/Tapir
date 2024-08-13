create or replace package body test_audit_columns is

   procedure test_insert is
      l_tapi tapir_audit_cols$tapi.rt;
      l_before date := sysdate;
   begin
      l_tapi := tapir_audit_cols$tapi.ins(tapir_audit_cols$tapi.rt(pk => '1', non_pk => 2));
      ut.expect(l_tapi.created_by).to_equal('me');
      ut.expect(l_tapi.created_at).to_be_greater_or_equal(l_before);
      ut.expect(l_tapi.modified_by).to_equal('me');
      ut.expect(l_tapi.modified_at).to_be_greater_or_equal(l_before);
   end;

   procedure test_insert_rows is
      l_rows tapir_audit_cols$tapi.rows_tab;
      l_before date := sysdate;
   begin
      l_rows := tapir_audit_cols$tapi.rows_tab(tapir_audit_cols$tapi.rt(pk => '1', non_pk => 2)
                                              ,tapir_audit_cols$tapi.rt(pk => '2', non_pk => 3));
      tapir_audit_cols$tapi.ins_rows(l_rows);
      ut.expect(l_rows(1).created_by).to_equal('me');
      ut.expect(l_rows(1).created_at).to_be_greater_or_equal(l_before);
      ut.expect(l_rows(1).modified_by).to_equal('me');
      ut.expect(l_rows(1).modified_at).to_be_greater_or_equal(l_before);
   end;

   procedure test_update is
      l_pk   tapir_audit_cols$tapi.pk_t := '1';
      l_before date := sysdate;
      l_tapi_ins tapir_audit_cols$tapi.rt;
      l_tapi_upd tapir_audit_cols$tapi.rt;
   begin
      l_tapi_ins := tapir_audit_cols$tapi.ins(tapir_audit_cols$tapi.rt(pk => l_pk, non_pk => 2));
      update tapir_audit_cols a
         set a.modified_by = 'others'
       where a.pk = l_pk;
      
      l_tapi_upd := l_tapi_ins;
      l_tapi_upd.non_pk := 42;
      tapir_audit_cols$tapi.upd(l_tapi_upd);

      ut.expect(l_tapi_upd.created_by).to_equal(l_tapi_ins.created_by);
      ut.expect(l_tapi_upd.created_at).to_equal(l_tapi_ins.created_at);
      ut.expect(l_tapi_upd.modified_at).to_be_greater_or_equal(l_before);
      ut.expect(l_tapi_upd.modified_by).to_equal('me');
   end;

   procedure test_update_rows is
      l_rows tapir_audit_cols$tapi.rows_tab;
      l_before date := sysdate;
   begin
      l_rows := tapir_audit_cols$tapi.rows_tab(tapir_audit_cols$tapi.rt(pk => '1', non_pk => 2)
                                              ,tapir_audit_cols$tapi.rt(pk => '2', non_pk => 3));
      tapir_audit_cols$tapi.ins_rows(l_rows);

      l_rows(1).non_pk := l_rows(1).non_pk * 2;
      l_rows(2).non_pk := l_rows(2).non_pk * 2;

      tapir_audit_cols$tapi.upd_rows(l_rows);

      ut.expect(l_rows(2).created_by).to_equal(l_rows(2).created_by);
      ut.expect(l_rows(2).created_at).to_equal(l_rows(2).created_at);
      ut.expect(l_rows(2).modified_at).to_be_greater_or_equal(l_before);
      ut.expect(l_rows(2).modified_by).to_equal('me');
   end;

   procedure test_merge_insert is
      l_pk   tapir_audit_cols$tapi.pk_t := '1';
      l_tapi tapir_audit_cols$tapi.rt;
      l_before date := sysdate;
   begin
      tapir_audit_cols$tapi.upsert(tapir_audit_cols$tapi.rt(pk => l_pk, non_pk => 2));

      l_tapi := tapir_audit_cols$tapi.sel(p_pk => l_pk);
      ut.expect(l_tapi.created_by).to_equal('me');
      ut.expect(l_tapi.created_at).to_be_greater_or_equal(l_before);
   end;

   procedure test_merge_update is
      l_pk   tapir_audit_cols$tapi.pk_t := '1';
      l_before date := sysdate;
      l_tapi_ins tapir_audit_cols$tapi.rt;
      l_tapi_upd tapir_audit_cols$tapi.rt;
   begin
      l_tapi_ins := tapir_audit_cols$tapi.ins(tapir_audit_cols$tapi.rt(pk => l_pk, non_pk => 2));
      update tapir_audit_cols a
         set a.modified_by = 'others'
       where a.pk = l_pk;
      
      l_tapi_upd := l_tapi_ins;
      l_tapi_upd.non_pk := 42;
      tapir_audit_cols$tapi.upsert(l_tapi_upd);

      ut.expect(l_tapi_upd.created_by).to_equal(l_tapi_ins.created_by);
      ut.expect(l_tapi_upd.created_at).to_equal(l_tapi_ins.created_at);
      ut.expect(l_tapi_upd.modified_at).to_be_greater_or_equal(l_before);
      ut.expect(l_tapi_upd.modified_by).to_equal('me');
   end;

end;
/
