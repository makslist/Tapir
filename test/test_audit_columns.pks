create or replace package test_audit_columns authid definer is

   --%suite('test_audit_columns')
   --%suitepath(all.globaltests)

   --%test
   procedure test_insert;

   --%test
   procedure test_insert_rows;

   --%test
   procedure test_update;

   --%test
   procedure test_update_rows;

   --%test
   procedure test_merge_insert;

   --%test
   procedure test_merge_update;

end;
/
