set serveroutput on
set trimspool on

prompt RUN GENERATOR TEST CODE COVERAGE
spool tapir_coverage.html
execute ut.run(ut_coverage_html_reporter(), a_include_object_expr => 'tapir');
spool off
