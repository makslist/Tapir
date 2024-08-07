create or replace package test_audit_columns authid definer is

   --%suite('test_audit_columns')
   --%suitepath(all.globaltests)

   --%test
   procedure test_insert_audit;

   --%test
   procedure test_insert_rows_audit;

   --%test
   procedure test_update_audit;

   --%test
   procedure test_update_rows_audit;

   --%test
   procedure test_merge_audit;

end;
/
