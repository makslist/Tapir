set serveroutput on
set trimspool on

exec dbms_output.enable(NULL);

prompt RUN GENERATOR TEST CODE COVERAGE
spool tapir_coverage.html
execute ut.run(ut_coverage_html_reporter(), a_include_object_expr => 'tapir');
spool off

prompt RUN TAPI TEST CODE COVERAGE
spool tapi_coverage.html
execute ut.run(ut_coverage_html_reporter(), a_include_object_expr => 'test_table');
spool off
