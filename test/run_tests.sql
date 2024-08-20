set serveroutput on
set trimspool on

prompt RUN GENERATOR TEST
execute ut.run(a_include_object_expr => 'tapir');

-- prompt RUN INSERT TESTS
-- execute ut.run('test_crud_insert');

-- prompt RUN SELECT TESTS
-- execute ut.run('test_crud_select');

-- prompt RUN UPDATE TESTS
-- execute ut.run('test_crud_update');

-- prompt RUN DELETE TESTS
-- execute ut.run('test_crud_delete');

-- prompt RUN AUDIT COLUMNS TESTS
-- execute ut.run('test_audit_columns');

-- prompt RUN CLOUD_EVENT TESTS
--execute ut.run('test_cloud_events');
