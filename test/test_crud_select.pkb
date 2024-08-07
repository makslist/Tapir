create or replace package body test_crud_select is

   procedure test_select is
      l_pk1  varchar2(1) := '1';
      l_tapi tapir_all_types$tapi.rt := tapir_all_types$tapi.rt_defaults(t_varchar2 => l_pk1);
   begin
      tapir_all_types$tapi.ins(l_tapi);
   
      l_tapi := tapir_all_types$tapi.sel(p_t_varchar2 => l_pk1);

      ut.expect(l_tapi.t_varchar2).to_equal(l_pk1);
   end;

   procedure test_select_no_data_found is
      l_pk1  tapir_all_types$tapi.t_varchar2_t := '1';
      l_tapi tapir_all_types$tapi.rt;
   begin
      l_tapi := tapir_all_types$tapi.sel(p_t_varchar2 => l_pk1);
   end;

   procedure test_select_for_update is
      l_pk1  varchar2(1) := '1';
      l_tapi tapir_all_types$tapi.rt := tapir_all_types$tapi.rt_defaults(t_varchar2 => l_pk1);
   begin
      tapir_all_types$tapi.ins(l_tapi);
   
      l_tapi := tapir_all_types$tapi.sel_lock(p_t_varchar2 => l_pk1);

      ut.expect(l_tapi.t_varchar2).to_equal(l_pk1);
   end;

   procedure test_select_rows is
      l_pk1        tapir_all_types$tapi.t_varchar2_t := '1';
      l_rows       tapir_all_types$tapi.rows_tab;
      l_errors     tapir_all_types$tapi.rows_tab;
      l_cur tapir_all_types$tapi.strong_ref_cursor;
   begin
      l_rows := tapir_all_types$tapi.rows_tab(tapir_all_types$tapi.rt_defaults(t_varchar2 => l_pk1), tapir_all_types$tapi.rt_defaults(t_varchar2 => '2'));
      l_rows := tapir_all_types$tapi.ins_rows(l_rows, l_errors);
   
      open l_cur for
         select *
           from tapir_all_types
          where t_varchar2 = l_pk1;
      l_rows := tapir_all_types$tapi.sel_rows(l_cur);
   
      ut.expect(l_rows.count).to_equal(1);
   end;

end;
/
