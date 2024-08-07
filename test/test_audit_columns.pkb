create or replace package body test_audit_columns is

   procedure test_insert_audit is
      l_tapi tapir_audit_cols$tapi.rt;
   begin
      l_tapi := tapir_all_types$tapi.ins(p_pk1 => '1', p_pk2 => 2);
      tapir.assert(l_tapi.creator = 'me' and l_tapi.modifier = 'me', 'audit fields failed');
   end;

end;
/
