drop table if exists tapir_audit_cols;
/
create table if not exists tapir_audit_cols(
   pk                       varchar2(100),
   non_pk                   varchar2(100),
   created_by               varchar2(100),
   created_at               date,
   modified_by              varchar2(100),
   modified_at              date,
   constraint tapir_audit_cols_pk primary key(pk) using index
)
/