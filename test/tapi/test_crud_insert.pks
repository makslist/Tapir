create or replace package test_crud_insert authid definer is

   --%suite('test_crud_insert')
   --%suitepath(all.globaltests)

   --%test
   procedure test_insert_with_custom_defaults;

   --%test
   --%throws(-1)
   procedure test_insert_dup_val_on_index;

   --%test
   procedure test_insert_rows;

   --%test
   procedure test_ins_cursor;

   --%test
   procedure test_insert_rows_return_errors;

end;
/
