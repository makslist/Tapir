create or replace package body test_crud_insert is

   procedure test_insert_with_custom_defaults is
      l_pk1  varchar2(1) := '1';
      l_tapi tapir_all_types$tapi.rt;
   begin
      l_tapi := tapir_all_types$tapi.ins(tapir_all_types$tapi.rt_defaults(t_varchar2 => l_pk1));
   
      ut.expect(tapir_all_types$tapi.counts(p_t_varchar2 => l_pk1)).to_equal(1);
   end;

   procedure test_insert_dup_val_on_index is
      l_pk1  tapir_all_types$tapi.t_varchar2_t := '1';
      l_tapi tapir_all_types$tapi.rt;
   begin
      l_tapi := tapir_all_types$tapi.ins(tapir_all_types$tapi.rt_defaults(t_varchar2 => l_pk1));
      l_tapi := tapir_all_types$tapi.ins(tapir_all_types$tapi.rt_defaults(t_varchar2 => l_pk1));
   end;

   procedure test_insert_rows is
      l_rows tapir_all_types$tapi.rows_tab;
   begin
      l_rows := tapir_all_types$tapi.rows_tab(tapir_all_types$tapi.rt_defaults(t_varchar2 => '1'),
                                              tapir_all_types$tapi.rt_defaults(t_varchar2 => '2'));
      tapir_all_types$tapi.ins_rows(l_rows);
   
      ut.expect(tapir_all_types$tapi.counts()).to_equal(2);
   end;

   procedure test_ins_cursor is
      l_pk1 tapir_all_types$tapi.t_varchar2_t := '1';
      l_cur tapir_all_types$tapi.strong_ref_cursor;
   begin
      open l_cur for
         select l_pk1 as t_varchar2
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
           from dual;
      tapir_all_types$tapi.ins_cur(l_cur);
   
      ut.expect(tapir_all_types$tapi.counts(p_t_varchar2 => l_pk1)).to_equal(1);
   end;

   procedure test_insert_rows_return_errors is
      l_pk1        tapir_all_types$tapi.t_varchar2_t := '1';
      l_unique_row tapir_all_types$tapi.rt := tapir_all_types$tapi.rt_defaults(t_varchar2 => l_pk1);
      l_rows       tapir_all_types$tapi.rows_tab;
      l_errors     tapir_all_types$tapi.rows_tab;
   begin
      tapir_all_types$tapi.ins(l_unique_row);
      l_rows := tapir_all_types$tapi.rows_tab(l_unique_row, tapir_all_types$tapi.rt_defaults(t_varchar2 => '2'));
      l_rows := tapir_all_types$tapi.ins_rows(l_rows, l_errors);
   
      /*for idx in 1 .. l_errors.count loop
         tapir_all_types$tapi.print(l_errors);
      end loop;*/
   
      ut.expect(l_rows.count).to_equal(0);
      ut.expect(l_errors.count).to_equal(1);
      ut.expect(tapir_all_types$tapi.counts()).to_equal(1);
   end;

end;
/
