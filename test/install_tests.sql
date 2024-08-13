set serveroutput on

prompt CREATE ALL_TYPE_TABLE
@table_all_types.sql;

prompt CREATE AUDIT_TABLE
@table_audit_cols.sql;

prompt GENERATE CRUD TAPI
@generate_crud_api.sql

prompt GENERATE AUDIT TAPI
@generate_audit_api.sql

prompt GENERATE JSON TAPI
@generate_json_api.sql

-- prompt RUN CLOUD_EVENT TESTS
-- --execute ut.run('test_cloud_events');

-- --exec ut.run(ut_documentation_reporter());

