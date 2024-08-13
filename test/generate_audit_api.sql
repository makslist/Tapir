begin
   tapir.init(tapir.params_t(audit_user_exp          => '''me''',
                             audit_col_created_by    => 'created_by',
                             audit_col_created_date  => 'created_at',
                             audit_col_modified_by   => 'modified_by',
                             audit_col_modified_date => 'modified_at'));
   tapir.compile_tapi(p_table_name => 'TAPIR_AUDIT_COLS');
end;
/
