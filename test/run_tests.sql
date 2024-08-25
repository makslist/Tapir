set serveroutput on
set trimspool on

prompt RUN GENERATOR TEST
execute ut.run(a_include_object_expr => 'tapir');

prompt RUN TAPI TESTS
execute ut.run(a_include_object_expr => 'tapir_all_types');
