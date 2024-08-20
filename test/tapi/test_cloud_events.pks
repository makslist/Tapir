create or replace package test_cloud_events is

   --%suite('cloud_events')
   --%suitepath(all.globaltests)

   --%test
   --%rollback(manual)
   procedure test_create_ce_table;

   --%test
   --%rollback(manual)
   procedure test_create_ce_queue;

   --%test
   --%rollback(manual)
   procedure test_drop_ce_queue;

end;
/
