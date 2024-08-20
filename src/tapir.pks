create or replace package tapir authid current_user is

   $if dbms_db_version.version > 12 or(dbms_db_version.version = 12 and dbms_db_version.release >= 2) $then
   subtype obj_col is varchar(128);
   $else
   subtype obj_col is varchar(30);
   $end
   gc_date_format_iso_8601 constant varchar(30) := 'YYYY-MM-DD"T"HH24:MI:SS"Z"';

   subtype str is varchar2(32767);
   type str_list is table of str;
   type mapping is table of obj_col index by obj_col;
   boolean_pseudo_yn constant mapping := mapping('true' => 'Y', 'false' => 'N');
   boolean_pseudo_10 constant mapping := mapping('true' => '1', 'false' => '0');

   /**
   *  Parameter record to initialize the TAPI generator
   *
   *  param tapi_name                     Substitution rule for the TAPI name based on the table name. '\1' stands for the captured table name.
   *                                      mapping('^(.*)$' => '\1$tapi') for table 'HR' leeds to TAPI name 'HR$tapi'
   *        proc_select
   *        proc_update
   *        proc_insert                   If not null, procedures are created to insert records (table%rowtype, table of rows).
   *        proc_insert_cur
   *        proc_delete
   *        proc_merge
   *        proc_exists
   *        proc_exists_and_select
   *        proc_lock_record
   *        proc_count
   *        proc_pk_string_delim
   *        proc_print
   *        proc_json_obj
   *        proc_of_json
   *        proc_checksum
   *        proc_diff                     Is dependend of "proc_json_obj"
   *        proc_pipe
   *        use_column_defaults           If true, the defaults in the data dictionary are assigned to columns which are null. DB defaults won't be assigned directly, because the TAPI assigns null values to columns, when no set otherwise.
   *        check_pk_values_before_select 
   *        raise_error_on_failed_update_delete
   *        return_null_when_no_data_found
   *        create_bulk_procedures
   *        default_bulk_limit            The limit for the set based read operation.
   *        create_occ_procedures         Optimistic concurrency control
   *        boolean_pseudo_type           Creates an aditional exists function returning a varchar2 instead of a boolean. For SQL for pre 23ai versions
   *        audit_user_exp                Define the expression to determine the user logged in the audit fields.
   *        audit_col_created_by          If not null, the column is set to the value as defined with audit_user_exp
   *        audit_col_created_date
   *        audit_col_modified_by
   *        audit_col_modified_date
   *        audit_ignore_when_comparing   
   *        export_date_format            The date format used to print or compare date fields. If null the format of the session is used.
   *        export_number_format          If you omit 'nlsparam' or any one of the parameters, then this function uses the default parameter values for your session. 
   *        logging_exception_procedure   If not null, occuring exceptions will be logged with this procedure
   *        column_default_expressions    mapping('id' => 'seq_table_id') If defined, the expresion return value  will be assigned to the column when creating.
   *        record_default_expressions    If not null, then all non excluded columns need an expression set
   *        increase_row_version_column   Acts like a default expression, but modifiying the column based of the previous value. Type of column needs to be numerical.
   *        exclude_column_when_writing   Columns which are ignored in writing api calls (insert, update). The columns need to be nullable or default values set on table level.
   *                                      Columns which are part of the primary key are not allowed.
   *        acquire_lock_timeout          The time in seconds until acquiring lock is canceled.
   *        use_result_cache
   *        double_quote_names            If true, object names will be quoted.
   *        parameter_prefix
   *        log_cloud_events              The cloud_event functionality requires the presence of the JSON function. If it is not needed for the external API, a private function is created. 
   *        warn_about_null_string_default Raises an exception, when encountering the string 'null' as default value       
   *        plsql_optimize_level
   */
   type params_t is record(
      tapi_name                             mapping default mapping('^(.*)$' => '\1$tapi'),
      proc_select                           obj_col not null default 'sel',
      proc_update                           obj_col not null default 'upd',
      proc_insert                           obj_col not null default 'ins',
      proc_insert_cur                       obj_col default 'ins_cur',
      proc_delete                           obj_col not null default 'del',
      proc_merge                            obj_col default 'upsert',
      proc_exists                           obj_col not null default 'exist',
      proc_exists_and_select                obj_col not null default 'exists_sel',
      proc_lock_record                      obj_col default 'sel_lock',
      proc_count                            obj_col default 'counts',
      proc_pk_string_delim                  obj_col not null default '/',
      proc_print                            obj_col default 'print',
      proc_json_obj                         obj_col default 'json_obj',
      proc_of_json                          obj_col default 'of_json',
      proc_checksum                         obj_col default 'checksum',
      proc_diff                             obj_col default 'diff',
      proc_pipe                             obj_col default 'pipe',
      use_column_defaults                   boolean not null default true,
      check_pk_values_before_select         boolean not null default true,
      raise_error_on_failed_update_delete   boolean not null default true,
      return_null_when_no_data_found        boolean not null default false,
      create_bulk_procedures                boolean not null default true,
      default_bulk_limit                    number not null default 1000,
      create_occ_procedures                 boolean not null default false,
      boolean_pseudo_type                   mapping default mapping(),
      audit_user_exp                        varchar2(1024) default 'coalesce(sys_context(''apex$session'' ,''app_user'') ,sys_context(''userenv'' ,''os_user'') ,sys_context(''userenv'' ,''session_user''))',
      audit_col_created_by                  obj_col default null,
      audit_col_created_date                obj_col default null,
      audit_col_modified_by                 obj_col default null,
      audit_col_modified_date               obj_col default null,
      audit_ignore_when_comparing           boolean not null default true,
      export_date_format                    obj_col default gc_date_format_iso_8601, 
      export_number_format                  obj_col default 'TM', 
      logging_exception_procedure           obj_col default null,
      column_default_expressions            mapping default mapping(),
      record_default_expressions            mapping default mapping(),
      increase_row_version_column           obj_col default null,
      exclude_column_when_writing           str_list default str_list(),
      acquire_lock_timeout                  number not null default -1,
      use_result_cache                      boolean not null default false,
      double_quote_names                    boolean not null default false,
      parameter_prefix                      obj_col default 'p_',
      log_cloud_events                      mapping not null default mapping('table_name'    => null,
                                                                             'aq_queue_name' => null),
      warn_about_null_string_default        boolean default true,
      plsql_optimize_level                  pls_integer default 2);

   function canonicalize_name
   (
      p_name_ref in varchar2,
      p_name     in varchar2
   ) return varchar2;

   /**
   *  Creates a table form storing emitted cloud_events.
   *  The calling user needs create table privileges.
   *
   *  param p_table_name      The name of the table.
   *        p_schema_name     The schema the queue is created in.
   *        p_immutable       If true, creates an immutable table. Available since version 19.11 of the Oracle database.
   *        p_retention_days  Set the ROW_RETENTION and TABLE_INACTIVITY_RETENTION of the immutable table to the number of days given.
   */
   procedure create_ce_table
   (
      p_table_name     in varchar2,
      p_schema_name    in varchar2 default user,
      p_immutable      in boolean default false,
      p_retention_days in pls_integer default null
   );

   /**
   *  Creates an AQ-queue and backing objects type and tables for cloud_events.
   *  The calling user needs access privileges to package ''DBMS_AQADM''.
   *
   *  param p_queue_name   The name of the queue.
   *        p_schema_name  The schema the queue is created in.
   */
   procedure create_ce_queue
   (
      p_queue_name  in varchar2,
      p_schema_name in varchar2 default user
   );

   /**
   *  Creates an AQ-queue and backing objects type and tables for cloud_events.
   *  The calling user needs access privileges to package ''DBMS_AQADM''.
   *
   *  param p_queue_name   The name of the queue.
   *        p_schema_name  The schema the queue recides in.
   *        p_drop_type    If true, the object type 'CLOUD_EVENT' will drop.
   */
   procedure drop_ce_queue
   (
      p_queue_name  in varchar2,
      p_schema_name in varchar2 default user,
      p_drop_type   in boolean default false
   );

   procedure init(p_params params_t);

   /**
   *  Returns the generated TAPi as a string.
   *
   *  param p_table_name       The table the TAPI to generate for.
   *        p_schema_name      The schema of the source table. If null, the current schema will be used.
   */
   function tapi_source
   (
      p_table_name     in varchar2,
      p_schema_name    in varchar2 default user
   ) return clob;

   /**
   *  Generates and compile the TAPi for the current user.
   *
   *  param p_table_name       The table the TAPI to generate for.
   *        p_schema_name      The schema of the source table. If null, the current schema will be used.
   */
   procedure compile_tapi
   (
      p_table_name     in varchar2,
      p_schema_name    in varchar2 default user
   );

   /**
   *  Generates TAPIs for 
   *
   *  param p_owner  The owner of the source tables. If null, the current user will be used.
   *                 
   */
   procedure compile_tapis
   (
      p_owner     in varchar2 default user,
      p_name_like in varchar2 default null
   );
end;
/
