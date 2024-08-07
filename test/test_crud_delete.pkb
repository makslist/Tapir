create or replace package body test_crud_delete is

   procedure test_delete is
      l_pk1  varchar2(1) := '1';
      l_tapi tapir_all_types$tapi.rt;
   begin
      l_tapi := tapir_all_types$tapi.ins(tapir_all_types$tapi.rt_defaults(t_varchar2 => l_pk1));
      ut.expect(tapir_all_types$tapi.counts(p_t_varchar2 => l_pk1)).to_equal(1);

      tapir_all_types$tapi.del(p_t_varchar2 => l_pk1);
      ut.expect(tapir_all_types$tapi.counts(p_t_varchar2 => l_pk1)).to_equal(0);
   end;

   procedure test_delete_no_data_found is
      l_pk1  tapir_all_types$tapi.t_varchar2_t := '1';
   begin
      tapir_all_types$tapi.del(p_t_varchar2 => l_pk1);
   end;

end;
/
