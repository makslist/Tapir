set serveroutput on

prompt RUN INSERT TESTS
execute ut.run('test_crud_insert');

prompt RUN SELECT TESTS
execute ut.run('test_crud_select');

prompt RUN UPDATE TESTS
execute ut.run('test_crud_update');

prompt RUN DELETE TESTS
execute ut.run('test_crud_delete');

prompt RUN CLOUD_EVENT TESTS
--execute ut.run('test_cloud_events');

--exec ut.run(ut_documentation_reporter());

