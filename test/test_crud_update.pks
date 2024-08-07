create or replace package test_crud_update authid definer is

   --%suite('test_crud_update')
   --%suitepath(all.globaltests)

   --%test
   procedure test_update;

   --%test
   procedure test_update_rows;

   --%test
   procedure test_merge;

end;
/
