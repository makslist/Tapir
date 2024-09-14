set serveroutput on
set trimspool on

prompt RUN TAPI TESTS
execute ut.run(a_include_object_expr => 'tapir');
