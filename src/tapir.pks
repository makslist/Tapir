create or replace package tapir authid current_user is

    $if dbms_db_version.version > 12 or(dbms_db_version.version = 12 and dbms_db_version.release >= 2) $then
    subtype obj_col is varchar(128);
    $else
    subtype obj_col is varchar(30);
    $end
    date_format_iso_8601 constant varchar(30) := 'YYYY-MM-DD"T"HH24:MI:SS"Z"';

    subtype str is varchar2(32767);
    type str_list is table of str;
    type mapping is table of str index by obj_col;

    /**
    * Options for the default values of the columns:
    *
    * use_column_defaults       If true, columns that are null are assigned the value defined in the data dictionary.
    *                           When using the Tapi, DB default values are not assigned directly, as the TAPI assigns null values to all columns if they are not set otherwise.
    * column_expressions        If defined, the return value of the expression is assigned to the column when it is created.
    *                           Example: mapping('id' => 'seq_table_id.nextval', 'entry_date' => 'sysdate') 
    * init_record_expressions   If not null, a function is generated that returns a record whose values correspond to those of the expressions. With this function it is possible to create records filled with standard/random values.
    * row_version_column        If not null, the value of the specified version column is incremented by 1. The column type must be numeric.
    */
    type defaults_t is record(
        use_column_defaults     boolean not null default true,
        column_expressions      mapping default mapping(),
        init_record_expressions mapping default mapping(),
        row_version_column      obj_col default null);

    /**
    * Options for bulk processing
    *
    * generate          If true, bulk processing procedures/functions are created for selecting, inserting and updating records.
    * default_limit     The default limit for bulk processing procedures.
    * proc_insert_cur   If not null, a procedure with the specified name is created to insert records into the table returned by the given cursor.
    */
    type bulk_t is record(
        generate        boolean not null default true,
        default_limit   number default 1000,
        proc_insert_cur obj_col default 'ins_cur');

    /**
    * String format options:
    *
    * date_f    The date format used to print or export date fields. If null, the default format of the session is used.
    * number_f  The number format used to print or export number fields. If null, the default format of the session is used.
    */
    type export_format_t is record(
        date_f   obj_col default date_format_iso_8601,
        number_f obj_col default 'TM');

    /**
    * Options for boolean pseudotype:
    *
    * If set, an additional “exists” function is created that returns a varchar2 instead of a boolean.
    * For use in SQL for versions prior to 23c.
    *
    * true_value    Specifies which char value is returned instead of true.
    * false_value   Specifies which char value is returned instead of false.
    */
    type boolean_pseudo_type_t is record(
        true_value  varchar2(1) default 'Y',
        false_value varchar2(1) default 'N');

    /**
    * Options for handling audit columns:
    *
    * user_exp              Define the expression for determining the user when changing a data record.
    * col_created_by        If not null, the column is set to the value defined with audit_user_exp (@see record_user_override)
    * col_created_date      If not null, the column is updated with the current timestamp when the record is inserted.
    * col_modified_by       If not null, the column is set to the value defined with audit_user_exp (@see record_user_override).
    * col_modified_date     If not null, the column is updated with the current timestamp when the record is updated.
    * record_user_override  If true, an existing value (not null) in the user fields is not updated by the user_expression when inserting or updating.
    * ignore_when_comparing If true, the audit columns are ignored when comparing records before updating or merging.
    */
    type audit_t is record(
        user_exp              varchar2(1024) default 'coalesce(sys_context(''apex$session'' ,''app_user'') ,sys_context(''userenv'' ,''os_user'') ,sys_context(''userenv'' ,''session_user''))',
        col_created_by        obj_col default null,
        col_created_date      obj_col default null,
        col_modified_by       obj_col default null,
        col_modified_date     obj_col default null,
        record_user_override  boolean default false,
        ignore_when_comparing boolean not null default true);

    /**
    * Options for cloud_events (@see https://cloudevents.io/)
    *
    * Emits cloud_events when records are changed (insert, update, delete). If at least one of the parameters is set,
    * the cloud_events are saved in the defined table or queue.
    * Procedures to create the corresponding objects are available. (@see create_ce_table, create_ce_queue)
    *
    * table_name    If not null, cloud_events are saved in the table.
    * aq_queue_name If not null, cloud_events are enqued in the queue.
    */
    type cloud_events_t is record(
        table_name    obj_col default null,
        aq_queue_name obj_col default null);

    /**
    *   Parameter record to initialize the TAPI generator
    *
    *       param tapi_name                Substitution rule for the TAPI name based on the table name. '\1' stands for the captured table name.
    *                                      mapping('^(.*)$' => '\1$tapi') for table 'HR' leeds to TAPI name 'HR$tapi'
    *       proc_select                    Name of the procedures/functions to read a record (records) fro mthe underlying table.
    *       proc_insert                    Name of the procedures/functions to insert records (table%rowtype, table of rows).
    *       proc_update                    Name of the procedures to update records (table%rowtype, table of rows).
    *       proc_delete
    *       proc_merge
    *       proc_exists
    *       proc_exists_and_select
    *       proc_lock_record
    *       acquire_lock_timeout          The time in seconds until acquiring a row lock is canceled.
    *       proc_count                     If not null, the 
    *       proc_pk_string_delim           pk_str returns a string representaion of the concatenated primary key values. These
    *       proc_print
    *       proc_json_obj
    *       proc_of_json
    *       proc_checksum
    *       proc_diff                     Is dependend of "proc_json_obj"
    *       proc_pipe
    *       check_pk_values_before_select 
    *       raise_error_on_failed_update_delete
    *       return_null_when_no_data_found
    *       create_occ_procedures         Optimistic concurrency control
    *       log_exception_procedure   If not null, occuring exceptions will be logged with this procedure
    *       exclude_column_when_writing   Columns which are ignored in writing api calls (insert, update). The columns need to be nullable or default values set on table level.
    *                                      Columns which are part of the primary key are not allowed. Virtual and identity columns are automatically ignored.
    *       use_result_cache
    *       double_quote_names            If true, object names will be quoted.
    *       parameter_prefix
    *       warn_about_null_string_default Raises an exception during generating TAPI, when encountering the string (not value) 'null' as column default value in data dictionary
    *       plsql_optimize_level           The optimize level the generated TAPI is compiled with. Due to the
    *           nature of a TAPI (encapsuled SQL code) no big performance uplift is expected when compiling with a higher optimize level.
    */
    type params_t is record(
        tapi_name                           mapping default mapping('^(.*)$' => '\1$tapi'),
        proc_select                         obj_col not null default 'sel',
        proc_insert                         obj_col not null default 'ins',
        proc_update                         obj_col not null default 'upd',
        proc_delete                         obj_col not null default 'del',
        proc_exists                         obj_col not null default 'exist',
        proc_exists_and_select              obj_col not null default 'exists_sel',
        proc_merge                          obj_col default 'upsert',
        proc_lock_record                    obj_col default 'sel_lock',
        acquire_lock_timeout                number not null default -1,
        proc_count                          obj_col default 'counts',
        proc_pk_string_delim                obj_col not null default '/',
        proc_print                          obj_col default 'print',
        proc_json_obj                       obj_col default 'json_obj',
        proc_of_json                        obj_col default 'of_json',
        proc_checksum                       obj_col default 'checksum',
        proc_diff                           obj_col default 'diff',
        proc_pipe                           obj_col default 'pipe',
        proc_undo                           obj_col default 'undo',
        proc_redo                           obj_col default 'redo',
        check_pk_values_before_select       boolean not null default true,
        raise_error_on_failed_update_delete boolean not null default false,
        return_null_when_no_data_found      boolean not null default false,
        bulk_proc                           bulk_t not null default bulk_t(),
        create_occ_procedures               boolean not null default false,
        defaults                            defaults_t not null default defaults_t(),
        audit                               audit_t default audit_t(),
        boolean_pseudo_type                 boolean_pseudo_type_t default null,
        export_format                       export_format_t default export_format_t(),
        log_exception_procedure             obj_col default null,
        exclude_column_when_writing         str_list default str_list(),
        parameter_prefix                    obj_col default 'p_',
        double_quote_names                  boolean not null default false,
        use_result_cache                    boolean not null default false,
        cloud_events                        cloud_events_t default cloud_events_t(),
        warn_about_null_string_default      boolean not null default true,
        indent                              obj_col not null default '    ',
        plsql_optimize_level                pls_integer default 2);

    function canonicalize_name
    (
        p_name_ref in varchar2,
        p_name     in varchar2
    ) return varchar2;

    /**
    * Creates a table for saving emitted cloud_events.
    * The calling user requires create-table privileges.
    *
    * param
    * p_table_name Name of the table.
    * p_schema_name The schema in which the queue is created.
    * p_immutable If true, an immutable table is created. Available since version 19.11 of the Oracle database.
    * p_retention_days Sets the ROW_RETENTION and TABLE_INACTIVITY_RETENTION of the immutable table to the specified number of days.
    */
    /**
    *   Creates a table form storing emitted cloud_events.
    *   The calling user needs create table privileges.
    *
    *   param
    *       p_table_name        Name of the table.
    *       p_schema_name       The schema the queue is created in.
    *       p_immutable         If true, creates an immutable table. Available since version 19.11 of the Oracle database.
    *       p_retention_days    Set the ROW_RETENTION and TABLE_INACTIVITY_RETENTION of the immutable table to the number of days given.
    */
    procedure create_ce_table
    (
        p_table_name     in varchar2,
        p_schema_name    in varchar2 default user,
        p_immutable      in boolean default false,
        p_retention_days in pls_integer default null
    );

    /**
    *   Creates an AQ-queue and backing objects type and tables for cloud_events.
    *   The calling user needs access privileges to package ''DBMS_AQADM''.
    *
    *   param
    *       p_queue_name    The name of the queue.
    *       p_schema_name   The schema the queue is created in.
    */
    procedure create_ce_queue
    (
        p_queue_name  in varchar2,
        p_schema_name in varchar2 default user,
        p_event_type  in varchar2 default null
    );

    /**
    *   Creates an AQ-queue and backing object's type and tables for cloud_events.
    *   The calling user needs access privileges to package 'DBMS_AQADM'.
    *
    *   param
    *       p_queue_name    The name of the queue.
    *       p_schema_name   The schema the queue recides in.
    *       p_drop_type     If true, the object type 'CLOUD_EVENT' will drop.
    */
    procedure drop_ce_queue
    (
        p_queue_name  in varchar2,
        p_schema_name in varchar2 default user,
        p_drop_type   in boolean default false
    );

    procedure init(p_params params_t);

    /**
    *   Returns the generated TAPI package as a string.
    *
    *   param
    *       p_table_name    The table the TAPI to generate for.
    *       p_schema_name   The schema of the source table. If null, the current schema will be used.
    */
    function tapi_source
    (
        p_table_name  in varchar2,
        p_schema_name in varchar2 default user
    ) return clob;

    /**
    *   Generates and compile the TAPI packagefor the current user.
    *
    *   param
    *       p_table_name    The table the TAPI to generate for.
    *       p_schema_name   The schema of the source table. If null, the current schema will be used.
    */
    procedure compile_tapi
    (
        p_table_name  in varchar2,
        p_schema_name in varchar2 default user
    );

    /**
    *   Generates TAPI packages for all (matching) tables of the user.
    *
    *   param
    *       p_owner         The owner of the source tables. If null, the current user will be used.
    *       p_name_like     If not null, the substring that the table names need to match.
    */
    procedure compile_tapis
    (
        p_owner     in varchar2 default user,
        p_name_like in varchar2 default null
    );

end;
/
