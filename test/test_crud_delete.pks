create or replace package test_crud_delete authid definer is

   --%suite('test_crud_delete')
   --%suitepath(all.globaltests)

   --%test
   procedure test_delete;

   --%test
   --%throws(-1403)
   procedure test_delete_no_data_found;

end;
/
