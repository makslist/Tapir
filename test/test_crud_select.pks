create or replace package test_crud_select authid definer is

   --%suite('test_crud_select')
   --%suitepath(all.globaltests)

   --%test
   procedure test_select;

   --%test
   --%throws(-1403)
   procedure test_select_no_data_found;

   --%test
   procedure test_select_for_update;

   --%test
   procedure test_select_rows;

end;
/
