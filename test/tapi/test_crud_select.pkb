create or replace package body test_crud_select is

   procedure test_select is
      l_pk1  varchar2(1) := '1';
      l_tapi tapir_all_types$crud.rt := tapir_all_types$crud.rt_defaults(t_varchar2 => l_pk1);
   begin
      tapir_all_types$crud.ins(l_tapi);
   
      l_tapi := tapir_all_types$crud.sel(p_t_varchar2 => l_pk1);

      ut.expect(l_tapi.t_varchar2).to_equal(l_pk1);
   end;

   procedure test_select_no_data_found is
      l_pk1  tapir_all_types$crud.t_varchar2_t := '1';
      l_tapi tapir_all_types$crud.rt;
   begin
      l_tapi := tapir_all_types$crud.sel(p_t_varchar2 => l_pk1);
   end;

   procedure test_select_for_update is
      l_pk1  varchar2(1) := '1';
      l_tapi tapir_all_types$crud.rt := tapir_all_types$crud.rt_defaults(t_varchar2 => l_pk1);
   begin
      tapir_all_types$crud.ins(l_tapi);
   
      l_tapi := tapir_all_types$crud.sel_lock(p_t_varchar2 => l_pk1);

      ut.expect(l_tapi.t_varchar2).to_equal(l_pk1);
   end;

   procedure test_select_rows is
      l_pk1        tapir_all_types$crud.t_varchar2_t := '1';
      l_rows       tapir_all_types$crud.rows_tab;
      l_errors     tapir_all_types$crud.rows_tab;
      l_cur tapir_all_types$crud.strong_ref_cursor;
   begin
      l_rows := tapir_all_types$crud.rows_tab(tapir_all_types$crud.rt_defaults(t_varchar2 => l_pk1), tapir_all_types$crud.rt_defaults(t_varchar2 => '2'));
      l_rows := tapir_all_types$crud.ins_rows(l_rows, l_errors);
   
      open l_cur for
         select *
           from tapir_all_types
          where t_varchar2 = l_pk1;
      l_rows := tapir_all_types$crud.sel_rows(l_cur);
   
      ut.expect(l_rows.count).to_equal(1);
   end;

end;
/
