drop table if exists tapir_audit_cols;
/
create table if not exists tapir_audit_cols(
   pk                       varchar2(100),
   non_pk                   varchar2(100),
   created_by               varchar2(100),
   created_at               timestamp,
   modified_by              varchar2(100),
   modified_at              timestamp,
   constraint tapir_audit_cols_pk primary key(pk) using index
)
/
begin
   tapir.init(tapir.params_t(audit_col_created_by    => 'created_by',
                             audit_col_created_date  => 'created_at',
                             audit_col_modified_by   => 'modified_by',
                             audit_col_modified_date => 'modified_at'));
   tapir.compile_tapi(p_table_name => 'TAPIR_AUDIT_COLS');
end;
/
