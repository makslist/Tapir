create or replace package body test_crud_update is

   procedure test_update is
      l_pk1  varchar2(1) := '1';
      l_number number := 999999999;
      l_tapi tapir_all_types$tapi.rt;
   begin
      l_tapi := tapir_all_types$tapi.ins(tapir_all_types$tapi.rt_defaults(t_varchar2 => l_pk1));
      l_tapi.t_number := l_number;
      tapir_all_types$tapi.upd(l_tapi);
   
      l_tapi := tapir_all_types$tapi.sel(p_t_varchar2 => l_pk1);
      ut.expect(l_tapi.t_number).to_equal(l_number);
   end;

   procedure test_update_rows is
      l_row_1 tapir_all_types$tapi.rt := tapir_all_types$tapi.rt_defaults(t_varchar2 => '1');
      l_row_2 tapir_all_types$tapi.rt := tapir_all_types$tapi.rt_defaults(t_varchar2 => '2');
      l_number number := 999999999;
      l_rows tapir_all_types$tapi.rows_tab;
      l_cur tapir_all_types$tapi.strong_ref_cursor;
   begin
      tapir_all_types$tapi.ins(l_row_1);
      tapir_all_types$tapi.ins(l_row_2);
      
      l_row_1.t_number := l_number;
      l_row_2.t_number := l_number;
      l_rows := tapir_all_types$tapi.rows_tab(l_row_1, l_row_2);
      tapir_all_types$tapi.upd_rows(l_rows);

      open l_cur for
         select *
           from tapir_all_types
          where t_number = l_number;
      l_rows := tapir_all_types$tapi.sel_rows(l_cur);
      ut.expect(l_rows.count).to_equal(2);
   end;

   procedure test_merge is
      l_pk1  varchar2(1) := '1';
      l_rows tapir_all_types$tapi.rows_tab;
      l_number number := 999999999;
      l_tapi tapir_all_types$tapi.rt;
   begin
      ut.expect(tapir_all_types$tapi.counts()).to_equal(0);
      tapir_all_types$tapi.upsert(tapir_all_types$tapi.rt_defaults(t_varchar2 => l_pk1));

      ut.expect(tapir_all_types$tapi.counts()).to_equal(1);
      
      tapir_all_types$tapi.upsert(tapir_all_types$tapi.rt_defaults(t_varchar2 => l_pk1, t_number => l_number));
   
      l_tapi := tapir_all_types$tapi.sel(p_t_varchar2 => l_pk1);
      ut.expect(l_tapi.t_number).to_equal(l_number);
   end;

end;
/
