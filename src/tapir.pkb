create or replace package body tapir is

    c_type_varchar2                       constant str := 'varchar2';
    c_type_char                           constant str := 'char';
    c_type_nchar                          constant str := 'nchar';
    c_type_nvarchar2                      constant str := 'nvarchar2';
    c_type_number                         constant str := 'number';
    c_type_float                          constant str := 'float';
    c_type_binary_float                   constant str := 'binary_float';
    c_type_binary_double                  constant str := 'binary_double';
    c_type_date                           constant str := 'date';
    c_type_timestamp                      constant str := 'timestamp(6)';
    c_type_timestamp_with_local_time_zone constant str := 'timestamp(6) with local time zone';
    c_type_timestamp_with_time_zone       constant str := 'timestamp(6) with time zone';
    c_type_interval_year_to_month         constant str := 'interval year(2) to month';
    c_type_interval_day_to_second         constant str := 'interval day(2) to second(6)';
    c_type_blob                           constant str := 'blob';
    c_type_clob                           constant str := 'clob';
    c_type_nclob                          constant str := 'nclob';
    c_type_long                           constant str := 'long';
    c_type_raw                            constant str := 'raw';
    c_type_long_raw                       constant str := 'long raw';
    c_type_bool                           constant str := 'boolean';
    --c_type_rowid                          constant str := 'rowid';
    c_type_json_obj constant str := 'json_object_t';

    scalar_types    constant str_list := str_list(c_type_varchar2,
                                                  c_type_number,
                                                  c_type_date,
                                                  c_type_timestamp,
                                                  c_type_raw);
    char_types      constant str_list := str_list(c_type_varchar2, c_type_char, c_type_nchar, c_type_nvarchar2);
    datetime_types  constant str_list := str_list(c_type_date,
                                                  c_type_timestamp,
                                                  c_type_timestamp_with_local_time_zone,
                                                  c_type_timestamp_with_time_zone,
                                                  c_type_interval_year_to_month,
                                                  c_type_interval_day_to_second);
    timestamp_types constant str_list := str_list(c_type_timestamp,
                                                  c_type_timestamp_with_local_time_zone,
                                                  c_type_timestamp_with_time_zone);
    number_types    constant str_list := str_list(c_type_number,
                                                  c_type_float,
                                                  c_type_binary_float,
                                                  c_type_binary_double);
    lob_types       constant str_list := str_list(c_type_blob, c_type_clob, c_type_nclob);
    binary_types    constant str_list := str_list(c_type_blob, c_type_long, c_type_raw, c_type_long_raw);

    type col_infos is record(
        col_name        obj_col,
        max_length      pls_integer,
        data_type       obj_col,
        data_default    obj_col,
        nullable        varchar2(1),
        virtual_column  varchar2(1),
        identity_column varchar2(1));
    type tab_cols_t is table of col_infos;
    type cons_r is record(
        c_name   obj_col,
        c_type   obj_col,
        c_suffix obj_col);
    type cons_t is table of cons_r;

    p_bulk_limit constant pls_integer := 1000;
    nl           constant str := chr(10);
    nll          constant str := nl || nl;

    tab     str;
    nlt     str;
    nltt    str;
    nlttt   str;
    nltttt  str;
    nlttttt str;

    tag_col             constant str := '<col_lower>';
    tag_col_pad         constant str := '<col_rpad>';
    tag_type            constant str := '<col_type>';
    tag_param           constant str := '<col_param>';
    tag_sig_param       constant str := '<sig_param>';
    tag_not_null        constant str := '<not_nullable/>';
    tag_quote_1         constant str := '<col_quote>';
    tag_quote_2         constant str := '</col_quote>';
    tag_rt_sig          constant str := '<col_rt_sig>';
    tag_rt_asg          constant str := '<col_rt_asg>';
    tag_rt_1            constant str := '<col_rt>';
    tag_rt_2            constant str := '</col_rt>';
    tag_col_default_1   constant str := '<default>';
    tag_col_default_2   constant str := '</default>';
    tag_col_to_char_1   constant str := '<to_char>';
    tag_col_to_char_2   constant str := '</to_char>';
    tag_rec_default     constant str := '<record_default>';
    tag_json_key        constant str := '<json_key>';
    tag_json_to_val_1   constant str := '<to_json_val>';
    tag_json_to_val_2   constant str := '</to_json_val>';
    tag_json_from_val_1 constant str := '<from_json_val>';
    tag_json_from_val_2 constant str := '</from_json_val>';

    type_name_suffix           constant str := '_t';
    type_rt_name               constant str := 'rt';
    opt_lock_suffix            constant str := '_opt';
    cur_checksum_name_suffix   constant str := '_checksum';
    type_rt_name_check         constant str := type_rt_name || cur_checksum_name_suffix;
    type_rows_tab              constant str := 'rows_tab';
    type_ref_cursor            constant str := 'strong_ref_cursor';
    type_col_hash              constant str := 'checksum_t';
    type_cloud_event           constant str := 'cloud_event';
    ce_table_name_suffix       constant str := '_tab';
    bulk_proc_suffix           constant str := '_rows';
    cursor_prefix              constant str := 'cur_idx';
    cursor_suffix_pk           constant str := '_pk';
    proc_name_assert_not_null  constant str := 'assert_not_null';
    proc_name_pk_string        constant str := 'pk_str';
    param_name_row             constant str := 'rec';
    proc_name_emit_cloud_event constant str := 'emit_cloud_event';
    ex_forall_error            constant str := 'forall_error';
    ex_cannot_insert_null      constant str := 'cannot_insert_null';
    ex_cannot_update_null      constant str := 'cannot_update_null';

    doc_sel            constant str := nlt || '* Returns a single record from the table.';
    doc_sel_null_check constant str := nlt || '* Checks for NULL values in the primary key columns.';

    g_owner               obj_col;
    g_table_name          obj_col;
    params                params_t;
    g_cols                tab_cols_t;
    g_includes_lobs       boolean := false;
    g_includes_binaries   boolean := false;
    with_cloud_events     boolean;
    g_pk_cols             tab_cols_t;
    g_cons                cons_t;
    g_audit_cols          str_list;
    g_audit_cols_created  str_list;
    g_audit_cols_modified str_list;
    g_audit_cols_user     str_list;
    g_audit_cols_date     str_list;
    priv_to_dbms_crypto   boolean;
    priv_to_dbms_aqadm    boolean;
    c_bignum constant integer := power(2, 32);

    function doc_sel_lock(p_for_update in boolean) return str is
    begin
        return case when p_for_update then nlt || '* Aquires a row level lock on this record.' end;
    end;

    function doc_sel_no_data_found return str is
    begin
        return case when params.return_null_when_no_data_found then nlt || '* Returns NULL if no record was found.' else nlt || '* Raises a no_data_found exception if no record was found.' end;
    end;

    function doc_sel_result_cache return str is
    begin
        return case when params.use_result_cache and not g_includes_lobs then nlt || '* Uses the PL/SQL Function Result Cache when using an Enterprise Edition.' end;
    end;

    function col_quote(p_var in varchar2 default null) return varchar2 is
    begin
        return tag_quote_1 || p_var || tag_quote_2;
    end;

    function col_default(p_var in varchar2 default null) return varchar2 is
    begin
        return tag_col_default_1 || p_var || tag_col_default_2;
    end;

    function col_char(p_var in varchar2 default null) return varchar2 is
    begin
        return tag_col_to_char_1 || p_var || tag_col_to_char_2;
    end;

    function col_rec(p_var in varchar2 default null) return varchar2 is
    begin
        return tag_rt_1 || p_var || '.' || tag_rt_2;
    end;

    function col_json_val(p_var in varchar2 default null) return varchar2 is
    begin
        return tag_json_to_val_1 || p_var || tag_json_to_val_2;
    end;

    function concat_if_not_null
    (
        p_prefix in str,
        p_string in clob,
        p_suffix in str default null
    ) return clob is
    begin
        return case when length(p_string) > 0 then p_prefix || p_string || p_suffix end;
    end;

    function owner_name return varchar2 is
    begin
        return case when params.double_quote_names then sys.dbms_assert.enquote_name(g_owner) else lower(g_owner) end;
    end;

    function table_name return varchar2 is
    begin
        return case when params.double_quote_names then sys.dbms_assert.enquote_name(g_table_name) else lower(g_table_name) end;
    end;

    function tapi_name return varchar2 is
    begin
        return trim(regexp_replace(trim(lower(g_table_name)),
                                   params.tapi_name.first,
                                   params.tapi_name(params.tapi_name.first)));
    end;

    procedure read_tab_cols is
        cursor p_ref_cursor is
            select c.column_name as col_name,
                   max(length(c.column_name)) over(partition by c.table_name) as max_length,
                   lower(c.data_type) as data_type,
                   c.data_default,
                   c.nullable,
                   substr(c.virtual_column, 1, 1) as virtual_column,
                   substr(c.identity_column, 1, 1) as identity_column
              from all_tab_cols c
             where c.owner = g_owner
                   and c.table_name = g_table_name
                   and c.column_id is not null
             order by c.column_id;
    begin
        g_cols := null;
        open p_ref_cursor;
        fetch p_ref_cursor bulk collect
            into g_cols limit p_bulk_limit;
        close p_ref_cursor;
        g_includes_lobs     := false;
        g_includes_binaries := false;
        for i in 1 .. g_cols.count loop
            g_cols(i).data_default := regexp_replace(g_cols(i).data_default,
                                                     '(^([[:space:]]|[[:cntrl:]])*|([[:space:]]|[[:cntrl:]])*$)');
            if params.warn_about_null_string_default and lower(g_cols(i).data_default) = '''null''' then
                raise_application_error(-20000,
                                        'Encountered default value ''null'' for column ' || lower(table_name) || '.' ||
                                        lower(g_cols(i).col_name));
            end if;
        
            if lower(g_cols(i).data_type) member of lob_types then
                g_includes_lobs := true;
            end if;
        
            if lower(g_cols(i).data_type) member of str_list(c_type_blob, c_type_raw) then
                g_includes_binaries := true;
            end if;
        end loop;
    end;

    function read_cons_cols(c_constraint_name in varchar2) return tab_cols_t is
        cursor c_ref_cursor is
            select c.column_name as name,
                   max(length(c.column_name)) over(partition by c.table_name) as max_length,
                   lower(c.data_type) as data_type,
                   c.data_default,
                   c.nullable,
                   substr(c.virtual_column, 1, 1) as virtual_column,
                   substr(c.identity_column, 1, 1) as identity_column
              from all_tab_cols c
              join (select *
                      from (select cc.owner,
                                   cc.table_name,
                                   cc.constraint_name,
                                   cc.column_name,
                                   cc.position
                              from all_cons_columns cc
                            union all
                            select ic.table_owner,
                                   ic.table_name,
                                   ic.index_name      as constraint_name,
                                   ic.column_name,
                                   ic.column_position as position
                              from all_ind_columns ic
                             where ic.index_name = c_constraint_name)
                     group by owner,
                              table_name,
                              constraint_name,
                              column_name,
                              position
                     order by position) con on c.owner = con.owner
                                               and c.table_name = con.table_name
                                               and c.column_name = con.column_name
             where con.owner = g_owner
                   and con.constraint_name = c_constraint_name
             order by c.column_id;
        l_cons_cols tab_cols_t := tab_cols_t();
    begin
        open c_ref_cursor;
        fetch c_ref_cursor bulk collect
            into l_cons_cols limit p_bulk_limit;
        close c_ref_cursor;
        return l_cons_cols;
    end;

    procedure read_constraints is
        cursor cur_constraints is
            select min(c_name) keep(dense_rank first order by decode(c_type, 'P', 1, 'U', 2, 3)) as c_name,
                   min(c_type) keep(dense_rank first order by decode(c_type, 'P', 1, 'U', 2, 3)) as c_type,
                   lower(min(c_suffix) keep(dense_rank first order by decode(c_type, 'P', 1, 'U', 2, 3))) as c_suffix
              from (select c.constraint_name as c_name,
                           c.constraint_type as c_type,
                           case
                               when c.constraint_type = 'P' then
                                null
                               when c.constraint_name like 'SYS%' then
                                '_' || listagg(cc.column_name, '_') within group(order by cc.position)
                               else
                                '_' || canonicalize_name(c.table_name, c.constraint_name)
                           end as c_suffix,
                           listagg(cc.column_name || cc.position) within group(order by cc.position) as key
                      from all_constraints c
                      join all_cons_columns cc on c.owner = cc.owner
                                                  and c.constraint_name = cc.constraint_name
                     where c.owner = g_owner
                           and c.table_name = g_table_name
                           and c.constraint_type in ('P', 'U')
                           and c.status = 'ENABLED'
                     group by c.constraint_name,
                              c.constraint_type,
                              c.table_name
                    union all
                    select i.index_name as c_name,
                           substr(i.uniqueness, 1, 1) as c_type,
                           case
                               when i.index_name = 'P' then
                                null
                               when i.index_name like 'SYS%' then
                                '_' || listagg(ic.column_name, '_') within group(order by ic.column_position)
                               else
                                '_' || canonicalize_name(i.table_name, i.index_name)
                           end as c_suffix,
                           listagg(ic.column_name || ic.column_position) within group(order by ic.column_position) as key
                      from all_indexes i
                      join all_ind_columns ic on i.owner = ic.index_owner
                                                 and i.index_name = ic.index_name
                     where i.owner = g_owner
                           and i.table_owner = g_owner
                           and i.table_name = g_table_name
                           and i.index_type = 'NORMAL'
                     group by i.index_name,
                              i.uniqueness,
                              i.table_name) i
             group by key
             order by decode(c_type, 'P', 1, 'U', 2, 3),
                      case
                          when c_name like '%_PK' then
                           1
                          else
                           2
                      end;
    begin
        g_cons := cons_t();
        open cur_constraints;
        fetch cur_constraints bulk collect
            into g_cons limit p_bulk_limit;
        close cur_constraints;
        if g_cons.count > 0 and g_cons(1).c_type = 'U' then
            g_cons(1).c_type := 'P';
            g_cons(1).c_suffix := null;
        end if;
        g_pk_cols := case
                         when g_cons.count > 0 and g_cons(1).c_type = 'P' then
                          read_cons_cols(g_cons(1).c_name)
                         else
                          tab_cols_t()
                     end;
    end;

    function has_priv_for_sys_
    (
        p_object  in varchar2,
        p_grantee in varchar2 default user
    ) return boolean is
        l_exists number;
    begin
        select count(grantee)
          into l_exists
          from all_tab_privs
         where table_name = upper(p_object)
               and grantee = upper(p_grantee)
               and grantor = 'SYS';
        return l_exists > 0;
    end;

    function changables(p_tab_cols in tab_cols_t default g_cols) return tab_cols_t is
        l_tab_cols tab_cols_t := tab_cols_t();
    begin
        for i in 1 .. p_tab_cols.count loop
            if p_tab_cols(i).virtual_column = 'N' and p_tab_cols(i).identity_column = 'N' and
                not nvl(p_tab_cols(i).col_name member of params.exclude_column_when_writing, false) then
                l_tab_cols.extend();
                l_tab_cols(l_tab_cols.last) := p_tab_cols(i);
            end if;
        end loop;
        return l_tab_cols;
    end;

    function comparables(p_tab_cols in tab_cols_t default g_cols) return tab_cols_t is
        l_tab_cols tab_cols_t := tab_cols_t();
    begin
        for i in 1 .. p_tab_cols.count loop
            if p_tab_cols(i).col_name member of scalar_types or p_tab_cols(i).data_type member of
              lob_types or
                (params.ignore_meta_data_columns_when_comparing and
                 (p_tab_cols(i).col_name member of
                  g_audit_cols or p_tab_cols(i).col_name = params.concurrency_control.row_version_column)) then
                null;
            else
                l_tab_cols.extend();
                l_tab_cols(l_tab_cols.last) := p_tab_cols(i);
            end if;
        end loop;
        return l_tab_cols;
    end;

    function exclude
    (
        p_tab_cols     in tab_cols_t,
        p_exclude_name in str_list default str_list(),
        p_exclude_type in str_list default str_list()
    ) return tab_cols_t is
        l_tab_cols tab_cols_t := tab_cols_t();
    begin
        for i in 1 .. p_tab_cols.count loop
            if p_tab_cols(i).col_name member of p_exclude_name or p_tab_cols(i).data_type member of p_exclude_type then
                null;
            else
                l_tab_cols.extend();
                l_tab_cols(l_tab_cols.last) := p_tab_cols(i);
            end if;
        end loop;
        return l_tab_cols;
    end;

    function include
    (
        p_tab_cols     in tab_cols_t,
        p_include_type in str_list default str_list(),
        p_include_name in str_list default str_list()
    ) return tab_cols_t is
        l_tab_cols tab_cols_t := tab_cols_t();
    begin
        for i in 1 .. p_tab_cols.count loop
            if p_tab_cols(i).col_name member of p_include_name or p_tab_cols(i).data_type member of p_include_type then
                l_tab_cols.extend();
                l_tab_cols(l_tab_cols.last) := p_tab_cols(i);
            end if;
        end loop;
        return l_tab_cols;
    end;

    function non_pk(p_tab_cols in tab_cols_t default g_cols) return tab_cols_t is
        function col_names(p_columns in tab_cols_t) return str_list is
            list str_list := str_list();
        begin
            for i in 1 .. p_columns.count loop
                list.extend();
                list(list.last) := p_columns(i).col_name;
            end loop;
            return list;
        end;
    begin
        return exclude(p_tab_cols, col_names(g_pk_cols));
    end;

    function non_audit(p_tab_cols in tab_cols_t default g_cols) return tab_cols_t is
        l_tab_cols tab_cols_t := tab_cols_t();
    begin
        for i in 1 .. p_tab_cols.count loop
            if p_tab_cols(i).col_name member of g_audit_cols then
                null;
            else
                l_tab_cols.extend();
                l_tab_cols(l_tab_cols.last) := p_tab_cols(i);
            end if;
        end loop;
        return l_tab_cols;
    end;

    function stringf
    (
        p_cols in tab_cols_t,
        p_mask in str
    ) return str_list is
        l_list str_list := str_list();
    
        function enquote(p_val in varchar2) return varchar2 is
        begin
            return case when params.double_quote_names then sys.dbms_assert.enquote_name(p_val) else lower(p_val) end;
        end;
    begin
        for i in 1 .. p_cols.count loop
            l_list.extend();
            l_list(l_list.last) := p_mask;
            l_list(l_list.last) := replace(l_list(l_list.last),
                                           tag_type,
                                           table_name() || '.' || enquote(p_cols(i).col_name) || '%type');
            l_list(l_list.last) := replace(l_list(l_list.last),
                                           tag_json_key,
                                           lower(p_cols(i).col_name) || case
                                               when p_cols(i).data_type member of binary_types then
                                                '_base64'
                                           end);
            l_list(l_list.last) := regexp_replace(l_list(l_list.last),
                                                  tag_rt_1 || '([^<]*)' || tag_rt_2,
                                                  '\1' || lower(p_cols(i).col_name) || case
                                                      when lower(p_cols(i).col_name) = lower(g_table_name) then
                                                       '_col'
                                                  end);
            l_list(l_list.last) := replace(l_list(l_list.last),
                                           tag_sig_param,
                                           lower(params.parameter_prefix) ||
                                           lower(rpad(p_cols(i).col_name, p_cols(i).max_length, ' ')));
            l_list(l_list.last) := replace(l_list(l_list.last),
                                           tag_param,
                                           lower(params.parameter_prefix || p_cols(i).col_name));
            l_list(l_list.last) := replace(l_list(l_list.last), tag_col, lower(p_cols(i).col_name));
            l_list(l_list.last) := replace(l_list(l_list.last),
                                           tag_col_pad,
                                           lower(rpad(p_cols(i).col_name, p_cols(i).max_length, ' ')));
            l_list(l_list.last) := replace(l_list(l_list.last),
                                           tag_rt_sig,
                                           lower(rpad(p_cols(i).col_name || case
                                                           when lower(p_cols(i).col_name) = lower(g_table_name) then
                                                            '_col'
                                                       end,
                                                      p_cols(i).max_length + 4,
                                                      ' ')));
            l_list(l_list.last) := replace(l_list(l_list.last),
                                           tag_rt_asg,
                                           lower(p_cols(i).col_name || case
                                                      when lower(p_cols(i).col_name) = lower(g_table_name) then
                                                       '_col'
                                                  end));
            l_list(l_list.last) := regexp_replace(l_list(l_list.last),
                                                  tag_not_null,
                                                  case
                                                      when p_cols(i).nullable = 'N' then
                                                       ' not null'
                                                      else
                                                       ''
                                                  end);
            l_list(l_list.last) := regexp_replace(l_list(l_list.last),
                                                  tag_col_default_1 || '(.*)' || tag_col_default_2,
                                                  case
                                                      when lower(p_cols(i).col_name) = lower(params.concurrency_control.row_version_column) then
                                                       'coalesce(\1, 0) + 1'
                                                      when p_cols(i).col_name member of g_audit_cols_date then
                                                       case p_cols(i).data_type
                                                           when c_type_date then
                                                            'sysdate'
                                                           when c_type_timestamp then
                                                            'systimestamp'
                                                       end
                                                      when p_cols(i).col_name member of g_audit_cols_user then
                                                       case
                                                           when params.audit.record_user_override then
                                                            'coalesce(\1, ' || params.audit.user_exp || ')'
                                                           else
                                                            params.audit.user_exp
                                                       end
                                                      when params.defaults.column_expressions.exists(p_cols(i).col_name) then
                                                       'coalesce(\1, ' || case
                                                           when p_cols(i).data_type in (c_type_nchar, c_type_nvarchar2) then
                                                            'to_nchar(' || params.defaults.column_expressions(p_cols(i).col_name) || ')'
                                                           when p_cols(i).data_type = c_type_clob then
                                                            'to_clob(' || params.defaults.column_expressions(p_cols(i).col_name) || ')'
                                                           when p_cols(i).data_type = c_type_nclob then
                                                            'to_nclob(' || params.defaults.column_expressions(p_cols(i).col_name) || ')'
                                                           when p_cols(i).data_type = c_type_blob then
                                                            'to_blob(' || params.defaults.column_expressions(p_cols(i).col_name) || ')'
                                                           when p_cols(i).data_type member of char_types then
                                                            'to_char(' || params.defaults.column_expressions(p_cols(i).col_name) || ')'
                                                           when p_cols(i).data_type member of number_types then
                                                            'to_number(' || params.defaults.column_expressions(p_cols(i).col_name) || ')'
                                                           else
                                                            params.defaults.column_expressions(p_cols(i).col_name)
                                                       end || ')'
                                                      when params.defaults.use_column_defaults and p_cols(i).data_default is not null then
                                                       'coalesce(\1, ' || case
                                                           when p_cols(i).data_type in (c_type_nchar, c_type_nvarchar2) then
                                                            'to_nchar(' || trim(p_cols(i).data_default) || ')'
                                                           when p_cols(i).data_type = c_type_clob then
                                                            'to_clob(' || trim(p_cols(i).data_default) || ')'
                                                           when p_cols(i).data_type = c_type_nclob then
                                                            'to_nclob(' || trim(p_cols(i).data_default) || ')'
                                                           when p_cols(i).data_type = c_type_blob then
                                                            'to_blob(' || trim(p_cols(i).data_default) || ')'
                                                           when p_cols(i).data_type member of char_types then
                                                            'to_char(' || trim(p_cols(i).data_default) || ')'
                                                           when p_cols(i).data_type member of number_types then
                                                            'to_number(' || trim(p_cols(i).data_default) || ')'
                                                           else
                                                            trim(p_cols(i).data_default)
                                                       end || ')'
                                                      else
                                                       '\1'
                                                  end);
            l_list(l_list.last) := replace(l_list(l_list.last),
                                           tag_rec_default,
                                           case
                                               when params.defaults.init_record_expressions.exists(p_cols(i).col_name) then
                                                ' default ' || params.defaults.init_record_expressions(p_cols(i).col_name)
                                               else
                                                ' default null'
                                           end);
            l_list(l_list.last) := regexp_replace(l_list(l_list.last),
                                                  tag_col_to_char_1 || '(.*)' || tag_col_to_char_2,
                                                  case
                                                      when p_cols(i).data_type member of number_types then
                                                       'to_char(\1' || case
                                                           when params.export_format.number_f is not null then
                                                            ', ''' || params.export_format.number_f || ''''
                                                       end || ')'
                                                      when p_cols(i).data_type member of datetime_types then
                                                       'to_char(\1' || case
                                                           when params.export_format.date_f is not null then
                                                            ', ''' || params.export_format.date_f || ''''
                                                       end || ')'
                                                      when p_cols(i).data_type member of binary_types then
                                                       'base64_encode(\1)'
                                                      when p_cols(i).data_type member of str_list(c_type_clob, c_type_nclob) then
                                                       '\1'
                                                      when p_cols(i).data_type = c_type_bool then
                                                       'case when \1 then ''true'' else ''false'' end'
                                                      when p_cols(i).data_type member of char_types then
                                                       '\1'
                                                  end);
            l_list(l_list.last) := regexp_replace(l_list(l_list.last),
                                                  tag_json_from_val_1 || '([^<]*)' || tag_json_from_val_2,
                                                  case
                                                      when p_cols(i).data_type member of number_types then
                                                       '\1.get_number(''' || lower(p_cols(i).col_name) || ''')'
                                                      when p_cols(i).data_type = c_type_date then
                                                       '\1.get_date(''' || lower(p_cols(i).col_name) || ''')'
                                                      when p_cols(i).data_type member of timestamp_types then
                                                       '\1.get_timestamp(''' || lower(p_cols(i).col_name) || ''')'
                                                      when p_cols(i).data_type = c_type_bool then
                                                       '\1.get_boolean(''' || lower(p_cols(i).col_name) || ''')'
                                                      when p_cols(i).data_type member of str_list(c_type_clob, c_type_nclob) then
                                                       '\1.get_clob(''' || lower(p_cols(i).col_name) || ''')'
                                                      when p_cols(i).data_type = c_type_blob then
                                                       'base64_decode(\1.get_clob(''' || lower(p_cols(i).col_name) || '_base64' || '''))'
                                                      when p_cols(i).data_type = c_type_raw then
                                                       'base64_decode(\1.get_string(''' || lower(p_cols(i).col_name) || '_base64' || '''))'
                                                      else
                                                       '\1.get_string(''' || lower(p_cols(i).col_name) || ''')'
                                                  end);
            l_list(l_list.last) := regexp_replace(l_list(l_list.last),
                                                  tag_quote_1 || '([^<]*)' || tag_quote_2,
                                                  '\1' || enquote(p_cols(i).col_name));
            l_list(l_list.last) := regexp_replace(l_list(l_list.last),
                                                  tag_json_to_val_1 || '([^<]*)' || tag_json_to_val_2,
                                                  case
                                                      when p_cols(i).data_type member of binary_types then
                                                       'base64_encode(\1)'
                                                      when p_cols(i).data_type member of datetime_types then
                                                       'to_char(\1' || case
                                                           when params.export_format.date_f is not null then
                                                            ', ''' || params.export_format.date_f || ''''
                                                       end || ')'
                                                      else
                                                       '\1'
                                                  end);
        end loop;
    
        return l_list;
    end;

    function indent(ref_str in varchar2) return varchar2 deterministic is
    begin
        return rpad(' ', length(ref_str), ' ');
    end;

    function str_join
    (
        p_list        in str_list,
        p_delimiter   in varchar2,
        p_append_null in boolean default false
    ) return clob is
        l_res clob;
    begin
        for i in 1 .. p_list.count loop
            l_res := l_res || case
                         when i > 1 and (p_append_null or p_list(i) is not null) then
                          p_delimiter
                     end || p_list(i);
        end loop;
        return l_res;
    end;

    function str_join
    (
        p_list_1 in str_list,
        p_list_2 in str_list
    ) return str_list is
        l_res str_list := str_list();
    begin
        for i in 1 .. p_list_1.count loop
            l_res.extend();
            l_res(l_res.last) := p_list_1(i);
        end loop;
        for i in 1 .. p_list_2.count loop
            l_res.extend();
            l_res(l_res.last) := p_list_2(i);
        end loop;
        return l_res;
    end;

    function canonicalize_name
    (
        p_name_ref in varchar2,
        p_name     in varchar2
    ) return varchar2 is
        l_delimiter  varchar2(2) := '_';
        l_table_name varchar2(100) := l_delimiter || replace(p_name_ref, l_delimiter, l_delimiter || l_delimiter) ||
                                      l_delimiter;
        split_count constant pls_integer := regexp_count(p_name_ref, l_delimiter) + 1;
        pattern     constant varchar2(100) := l_delimiter || '([^' || l_delimiter || ']*)' || l_delimiter;
        l_idx_name varchar2(100) := p_name;
    begin
        for i in 1 .. split_count loop
            l_idx_name := replace(l_idx_name, regexp_substr(l_table_name, pattern, 1, i, null, 1));
        end loop;
        l_idx_name := replace(l_idx_name, 'IDX', '_');
        l_idx_name := regexp_replace(l_idx_name, l_delimiter || '+', l_delimiter);
        l_idx_name := regexp_replace(l_idx_name,
                                     '^' || l_delimiter || '*([^' || l_delimiter || '].*[^' || l_delimiter || '])' ||
                                     l_delimiter || '*$',
                                     '\1');
        return l_idx_name;
    end;

    function not_equal_exp
    (
        p_val1 in varchar2,
        p_val2 in varchar2,
        p_type in varchar2 default null
    ) return varchar2 is
    begin
        return case when p_type member of lob_types then 'sys.dbms_lob.compare(' || p_val1 || ', ' || p_val2 || ') != 0' else 'decode(' || p_val1 || ', ' || p_val2 || ', 1, 0) = 0' end;
    end;

    function log_exception(errmsg in str) return str is
    begin
        return regexp_replace(errmsg, '^(.*)$', params.log_exception_procedure) || ';';
    end;

    function is_create_diff return boolean is
    begin
        return params.proc_diff is not null or(params.log_exception_procedure is not null and g_cons.count > 0);
    end;

    function name_proc_json_obj return varchar2 is
    begin
        return nvl(params.proc_json_obj, 'json_obj');
    end;

    function tapi_assert_not_null return str is
        prc_name constant str := 'procedure ' || proc_name_assert_not_null;
        p_val    constant str := params.parameter_prefix || 'val';
        p_param  constant str := params.parameter_prefix || 'param';
        sig      constant str := tab || prc_name || nlt || '(' || nltt || p_val || ' in varchar2' || ',' || nltt ||
                                 p_param || ' in varchar2' || nlt || ')';
        errmsg   constant str := '''Parameter '''''' || ' || p_param || ' || '''''' is null.''';
        bdy clob;
    begin
        bdy := nll || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'if ' || p_val || ' is null then';
        bdy := bdy || case
                   when params.log_exception_procedure is not null then
                    nlttt || log_exception(errmsg)
               end;
        bdy := bdy || nlttt || 'raise_application_error(-20000, ' || errmsg || ');';
        bdy := bdy || nltt || 'end if;';
        return bdy || nlt || 'end;';
    end;

    function tapi_base64_encode return clob is
        p_val constant str := params.parameter_prefix || 'val';
        sig   constant str := tab || 'function base64_encode' || nlt || '(' || nltt || p_val || ' in blob' || nlt ||
                              ') return clob';
        bdy clob;
    begin
        if not g_includes_binaries then
            return null;
        end if;
        bdy := nll || sig || ' is';
        bdy := bdy || nltt || 'c_step constant pls_integer := 8191 * 3;';
        bdy := bdy || nltt || 'l_b64 clob;';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'if ' || p_val || ' is null or dbms_lob.getlength(' || p_val || ') = 0 then';
        bdy := bdy || nlttt || 'return null;';
        bdy := bdy || nltt || 'end if;';
        bdy := bdy || nltt || 'for i in 0 .. trunc((sys.dbms_lob.getlength(' || p_val || ') - 1) / c_step)';
        bdy := bdy || nltt || 'loop';
        bdy := bdy || nlttt || 'l_b64 := l_b64 ||';
        bdy := bdy || nlttt || '         ' || 'utl_raw.cast_to_varchar2(utl_encode.base64_encode(sys.dbms_lob.substr(' ||
               p_val || ', c_step, i * c_step + 1)));';
        bdy := bdy || nltt || 'end loop;';
        bdy := bdy || nltt || 'return l_b64;';
        return bdy || nlt || 'end;';
    end;

    function tapi_base64_decode return clob is
        p_val constant str := params.parameter_prefix || 'val';
        sig   constant str := tab || 'function base64_decode' || nlt || '(' || nltt || p_val || ' in clob' || nlt ||
                              ') return blob';
        bdy clob;
    begin
        if not g_includes_binaries then
            return null;
        end if;
        bdy := nll || sig || ' is';
        bdy := bdy || nltt || 'c_step constant pls_integer := 8191 * 3;';
        bdy := bdy || nltt || 'temp  blob;';
        bdy := bdy || nltt || 'l_raw raw(32767);';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'if ' || p_val || ' is null or ' || p_val || ' = empty_clob() then';
        bdy := bdy || nlttt || 'return null;';
        bdy := bdy || nltt || 'end if;';
        bdy := bdy || nltt || 'sys.dbms_lob.createtemporary(temp, false, dbms_lob.call);';
        bdy := bdy || nltt || 'for i in 0 .. trunc((sys.dbms_lob.getlength(' || p_val || ') - 1) / c_step)';
        bdy := bdy || nltt || 'loop';
        bdy := bdy || nlttt || 'l_raw := sys.utl_raw.cast_to_raw(sys.dbms_lob.substr(' || p_val ||
               ', c_step, i * c_step + 1));';
        bdy := bdy || nlttt || 'sys.dbms_lob.append(temp, to_blob(sys.utl_encode.base64_decode(l_raw)));';
        bdy := bdy || nltt || 'end loop;';
        bdy := bdy || nltt || 'return temp;';
        return bdy || nlt || 'end;';
    end;

    function tapi_emit_cloud_event return clob is
        p_name constant str := 'procedure ' || proc_name_emit_cloud_event;
        sig    constant str := tab || p_name || nlt || '(' || nltt || 'p_type   in varchar2,' || nltt ||
                               'p_source in varchar2,' || nltt || 'p_data   in ' || c_type_json_obj || nlt || ')';
        bdy clob;
    begin
        if not nvl(with_cloud_events, false) then
            return null;
        end if;
        bdy := nll || sig || ' is';
        bdy := bdy || nltt || 'l_ce_id integer;';
        bdy := bdy || nltt || 'l_data clob;';
        bdy := bdy || nltt || 'l_message_id raw(16);';
        bdy := bdy || nltt || 'function snowflake_id return integer is';
        bdy := bdy || nlttt || 'b10         integer := 1023;';
        bdy := bdy || nlttt || 'b12         integer := 4095;';
        bdy := bdy || nlttt || 'shift12     integer := b12 + 1;';
        bdy := bdy || nlttt || 'shift22     integer := 4194304;';
        bdy := bdy || nlttt || 'b32         integer := 4294967295;';
        bdy := bdy || nlttt || 'shift32     integer := b32 + 1;';
        bdy := bdy || nlttt ||
               'epoch       integer := extract(day from(sys_extract_utc(systimestamp) - to_timestamp(''2015-01-01'', ''YYYY-MM-DD''))) *';
        bdy := bdy || nlttt || '            86400000 + to_number(to_char(sys_extract_utc(systimestamp), ''SSSSS''));';
        bdy := bdy || nlttt || 'ts_first_31 integer := bitand(epoch / 2, b32) * shift32;';
        bdy := bdy || nlttt || 'ts_last_10  integer := bitand(epoch, b10) * shift22;';
        bdy := bdy || nlttt || 'xid         varchar(144) := sys.dbms_transaction.local_transaction_id(true);';
        bdy := bdy || nlttt ||
               'xidsqn      integer := bitand(to_number(substr(xid, instr(xid, ''.'', 1, 2) + 1)), b10) * shift12;';
        bdy := bdy || nlttt || 'step_id     number := bitand(sys.dbms_transaction.step_id, b12);';
        bdy := bdy || nltt || 'begin';
        bdy := bdy || nlttt || 'return ts_first_31 + ts_last_10 + xidsqn + step_id;';
        bdy := bdy || nltt || 'end;';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'l_ce_id := snowflake_id;';
        bdy := bdy || nltt || 'l_data := p_data.to_string;';
    
        if params.cloud_events.table_name is not null then
            bdy := bdy || nltt || 'insert into ' || params.cloud_events.table_name;
            bdy := bdy || nlttt || '(ce_id,';
            bdy := bdy || nlttt || ' ce_time,';
            bdy := bdy || nlttt || ' ce_type,';
            bdy := bdy || nlttt || ' ce_source,';
            bdy := bdy || nlttt || ' ce_data)';
            bdy := bdy || nltt || 'values';
            bdy := bdy || nlttt || '(utl_raw.cast_from_number(l_ce_id),';
            bdy := bdy || nlttt || ' sys_extract_utc(systimestamp),';
            bdy := bdy || nlttt || ' p_type,';
            bdy := bdy || nlttt || ' p_source,';
            bdy := bdy || nlttt || ' l_data);';
        end if;
    
        if priv_to_dbms_aqadm and params.cloud_events.aq_queue_name is not null then
            declare
                l_type obj_col;
            begin
                select t.data_type
                  into l_type
                  from all_tab_cols t
                 where t.owner = upper(user)
                       and t.table_name = upper(sys.dbms_assert.simple_sql_name(params.cloud_events.aq_queue_name) ||
                                                ce_table_name_suffix)
                       and t.column_name = 'USER_DATA';
            
                bdy := bdy || nl;
                bdy := bdy || nltt || 'sys.dbms_aq.enqueue(queue_name         => ''' ||
                       params.cloud_events.aq_queue_name || ''',';
                bdy := bdy || nlttt || 'enqueue_options    => sys.dbms_aq.enqueue_options_t(),';
                bdy := bdy || nlttt || 'message_properties => sys.dbms_aq.message_properties_t(';
                bdy := bdy || nltttt || 'recipient_list => sys.dbms_aq.aq$_recipient_list_t(';
                for i in 1 .. params.cloud_events.aq_recipient_list.count loop
                    bdy := bdy || nlttttt || to_char(i) || ' => sys.aq$_agent(''' ||
                           params.cloud_events.aq_recipient_list(i) || ''', null, 0)';
                end loop;
                bdy := bdy || ')),';
                bdy := bdy || nlttt || 'payload            => ' || lower(l_type) || '(' ||
                       'utl_raw.cast_from_number(l_ce_id)' || ', sys_extract_utc(systimestamp)' || ', p_type' ||
                       ', p_source' || ', l_data' || '),';
                bdy := bdy || nlttt || 'msgid              => l_message_id);';
            end;
        end if;
    
        bdy := bdy || nlt || 'end;';
        return bdy;
    end;

    function tapi_diff_recs(p_only_header in boolean) return clob is
        doc       constant str := tab || '/**' || nlt ||
                                  '* Returns a JSON String containing all columns with different values.' || nlt ||
                                  '* For every contained column, the char representation of the values of both records are included.' || nlt || '*/';
        prc_name  constant str := 'function ' || nvl(params.proc_diff, 'diff');
        param_old constant str := params.parameter_prefix || 'old';
        param_new constant str := params.parameter_prefix || 'new';
        sig       constant str := prc_name || nlt || '(' || nltt || param_old || ' in ' || type_rt_name || ',' || nltt ||
                                  param_new || ' in ' || type_rt_name || nlt || ') return ' || c_type_json_obj;
        ret_val   constant str := 'diff';
        bdy clob;
    begin
        if not is_create_diff then
            return null;
        elsif p_only_header then
            return case when params.proc_diff is not null then nll || doc || nlt || sig || ';' end;
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nltt || ret_val || ' ' || c_type_json_obj || ' := ' || c_type_json_obj || '();';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'if (' || str_join(stringf(g_pk_cols, col_rec(param_old) || ' is null'), ' or ') ||
               ') and (' || str_join(stringf(g_pk_cols, col_rec(param_new) || ' is null'), ' and ') || ') then' ||
               nlttt || 'return ' || c_type_json_obj || '()' || ';' || nltt || 'end if;';
        bdy := bdy || nltt || str_join(stringf(g_pk_cols,
                                               ret_val || '.put(''' || tag_json_key || ''', nvl(' || col_rec(param_old) || ', ' ||
                                               col_rec(param_new) || '));'),
                                       nltt);
        bdy := bdy || nltt || 'if ' || str_join(stringf(g_pk_cols, col_rec(param_old) || ' is null'), ' or ') ||
               ' then';
        bdy := bdy || nlttt || ret_val || '.put(''mode'', ''insert'');';
        bdy := bdy || nlttt || ret_val || '.put(''new'', ' || name_proc_json_obj || '(' || param_new || '));';
        bdy := bdy || nlttt || 'return ' || ret_val || ';';
        bdy := bdy || nltt || 'elsif ' || str_join(stringf(g_pk_cols, col_rec(param_new) || ' is null'), ' or ') ||
               ' then';
        bdy := bdy || nlttt || ret_val || '.put(''mode'', ''delete'');';
        bdy := bdy || nlttt || ret_val || '.put(''old'', ' || name_proc_json_obj || '(' || param_old || '));';
        bdy := bdy || nlttt || 'return ' || ret_val || ';';
        bdy := bdy || nltt || 'else';
        bdy := bdy || nlttt || 'declare';
        bdy := bdy || nltttt || 'jo_old   ' || c_type_json_obj || ' := ' || name_proc_json_obj || '(' || param_old || ');';
        bdy := bdy || nltttt || 'jo_new   ' || c_type_json_obj || ':= ' || name_proc_json_obj || '(' || param_new || ');';
        bdy := bdy || nltttt || 'l_keys   json_key_list := jo_new.get_keys;';
        bdy := bdy || nlttt || 'begin';
        bdy := bdy || nltttt || ret_val || '.put(''mode'', ''update'');';
        bdy := bdy || nltttt || ret_val || '.put(''new'', ' || 'jo_new' || ');';
        bdy := bdy || nltttt || ret_val || '.put(''old'', ' || 'jo_old' || ');';
        bdy := bdy || nltttt || 'l_keys := jo_new.get_keys;';
        bdy := bdy || nltttt || 'for i in 1..l_keys.count loop';
        bdy := bdy || nlttttt || 'if (jo_old.get_string(l_keys(i)) is null and jo_new.get_string(l_keys(i)) is null)';
        bdy := bdy || nlttttt || tab || ' or jo_old.get_string(l_keys(i)) = jo_new.get_string(l_keys(i)) then';
        bdy := bdy || nlttttt || tab || 'jo_old.remove(l_keys(i));';
        bdy := bdy || nlttttt || tab || 'jo_new.remove(l_keys(i));';
        bdy := bdy || nlttttt || 'end if;';
        bdy := bdy || nltttt || 'end loop;';
        bdy := bdy || nltttt || 'if jo_new.get_size = 0 then';
        bdy := bdy || nlttttt || 'return json_object_t();';
        bdy := bdy || nltttt || 'else';
        bdy := bdy || nlttttt || ret_val || '.put(''mode'', ''update'');';
        bdy := bdy || nlttttt || ret_val || '.put(''old'', jo_old);';
        bdy := bdy || nlttttt || ret_val || '.put(''new'', jo_new);';
        bdy := bdy || nltttt || 'end if;';
        bdy := bdy || nlttt || 'end;';
        bdy := bdy || nlttt || 'return ' || ret_val || ';';
        bdy := bdy || nltt || 'end if;';
        return bdy || nlt || 'end;';
    end;

    function tapi_xxdo(p_only_header in boolean) return clob is
        prc_name constant str := 'procedure ' || 'xxdo';
        param    constant str := params.parameter_prefix || 'diff';
        l_rec    constant str := 'l_rec';
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || c_type_json_obj || ',' || nltt ||
                                 'p_forward in boolean' || nlt || ')';
        bdy clob;
    begin
        if (params.proc_redo is null and params.proc_undo is null) or params.proc_of_json is null then
            return null;
        elsif p_only_header then
            return null;
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nltt || l_rec || ' rt;';
        bdy := bdy || nltt || str_join(stringf(g_pk_cols, 'l_' || tag_col || ' ' || tag_type || ';'), nltt);
        bdy := bdy || nltt || 'l_mode varchar2(8) := ' || param || '.get_string(''mode'');';
        bdy := bdy || nltt || 'l_jo ' || c_type_json_obj || ' := ' || param || '.get_object(' ||
               'case when p_forward then ''new'' else ''old'' end' || ');';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt ||
               str_join(stringf(g_pk_cols,
                                'l_' || tag_col || ' := ' || param || '.' || 'get_string(''' || tag_col || ''');'),
                        nltt);
        bdy := bdy || nltt || 'case';
        bdy := bdy || nlttt || 'when ' || '(p_forward and ' || 'l_mode = ''insert'')' || ' or (not p_forward and ' ||
               'l_mode = ''delete'')' || ' then';
        bdy := bdy || nltttt || l_rec || ' := ' || params.proc_of_json || '(' || 'l_jo' || ');';
        bdy := bdy || nltttt || 'insert into ' || table_name() || ' t';
        bdy := bdy || nlttttt || '(' || str_join(stringf(changables, col_quote('t.')), ',' || nlttttt) || ')';
        bdy := bdy || nltttt || 'values';
        bdy := bdy || nlttttt || '(' || str_join(stringf(changables, col_rec(l_rec)), ',' || nlttttt) || ');';
        bdy := bdy || nlttt || 'when ' || '(p_forward and ' || 'l_mode = ''delete'')' || ' or (not p_forward and ' ||
               'l_mode = ''insert'')' || ' then';
        bdy := bdy || nltttt || 'delete ' || table_name() || ' t';
        bdy := bdy || nltttt || ' where ' ||
               str_join(stringf(g_pk_cols, col_quote('t.') || ' = ' || 'l_' || tag_col), nltttt || '   and ') || ';';
        bdy := bdy || nlttt || 'when ' || 'l_mode = ''update''' || ' then';
        bdy := bdy || nltttt || 'declare';
        bdy := bdy || nlttttt || str_join(stringf(non_pk(changables),
                                                  'l_upd_' || tag_col_pad || ' varchar2(1) := ' || 'case when l_jo' ||
                                                  '.has(''' || tag_json_key || ''') then ''1'' end;'),
                                          nlttttt);
        bdy := bdy || nltttt || 'begin';
        bdy := bdy || nlttttt || l_rec || ' := ' || params.proc_of_json || '(l_jo);';
        bdy := bdy || nlttttt || 'update ' || table_name() || ' t';
        bdy := bdy || nlttttt || '   set ' || str_join(stringf(non_pk(changables),
                                                               col_quote('t.') || ' = decode(' || 'l_upd_' || tag_col ||
                                                               ', 1, ' || col_rec(l_rec) || ', ' || col_quote('t.') || ')'),
                                                       ',' || nlttttt || '       ');
        bdy := bdy || nlttttt || ' where ' ||
               str_join(stringf(g_pk_cols, col_quote('t.') || ' = ' || 'l_' || tag_col), nlttttt || '   and ') || ';';
        bdy := bdy || nltttt || 'end;';
        bdy := bdy || nltt || 'end case;';
        return bdy || nlt || 'end;';
    end;

    function tapi_redo(p_only_header in boolean) return clob is
        doc      constant str := tab || '/**' || nlt ||
                                 '* Returns a JSON String containing all columns with different values.' || nlt ||
                                 '* For every contained column, the char representation of the values of both records are included.' || nlt || '*/';
        prc_name constant str := 'procedure ' || 'redo';
        param    constant str := params.parameter_prefix || 'diff';
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || c_type_json_obj || nlt || ')';
        bdy clob;
    begin
        if params.proc_redo is null or params.proc_of_json is null then
            return null;
        elsif p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'xxdo(' || param || ', true);';
        return bdy || nlt || 'end;';
    end;

    function tapi_undo(p_only_header in boolean) return clob is
        doc str := tab || '/**' || nlt || '* Returns a JSON String containing all columns with different values.' || nlt ||
                   '* For every contained column, the char representation of the values of both records are included.' || nlt || '*/';
        prc_name constant str := 'procedure ' || 'undo';
        param    constant str := params.parameter_prefix || 'diff';
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || c_type_json_obj || nlt || ')';
        bdy clob;
    begin
        if params.proc_undo is null or params.proc_of_json is null then
            return null;
        elsif p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'xxdo(' || param || ', false);';
        return bdy || nlt || 'end;';
    end;

    function tapi_counts_uk
    (
        p_cons        in tab_cols_t,
        p_only_header in boolean,
        p_suffix      in varchar2 default null
    ) return clob is
        doc      constant str := tab || '/**' || nlt || '* Counts the rows.' || nlt || '*/';
        prc_name constant str := 'function ' || params.proc_count || p_suffix;
        sig constant str := lower(prc_name) || nlt || '(' || nltt ||
                            str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type || ' default null'), ',' || nltt) || nlt ||
                            ') return number' || case
                                when params.use_result_cache and not g_includes_lobs then
                                 ' result_cache'
                            end;
        l_var    constant str := 'l_row_count';
        bdy clob;
    begin
        if p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nltt || l_var || ' ' || 'number;';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'select count(*)';
        bdy := bdy || nltt || '  into ' || l_var;
        bdy := bdy || nltt || '  from ' || table_name() || ' t';
        bdy := bdy || nltt || ' where ';
        bdy := bdy || str_join(stringf(p_cons,
                                       col_quote('t.') || ' = ' || 'nvl(' || tag_param || ', ' || col_quote('t.') || ')'),
                               nltt || '   and ') || ';';
        bdy := bdy || nltt || 'return ' || l_var || ';';
        return bdy || nlt || 'end;';
    end;

    function tapi_select_uk
    (
        p_cons        in tab_cols_t,
        p_only_header in boolean,
        p_suffix      in varchar2 default null,
        p_for_update  in boolean default false
    ) return clob is
        doc      constant str := tab || '/**' || doc_sel || doc_sel_lock(p_for_update) || doc_sel_result_cache ||
                                 doc_sel_null_check || doc_sel_no_data_found || nlt || '*/';
        prc_name constant str := 'function ' || case
                                     when p_for_update then
                                      params.concurrency_control.proc_lock_record
                                     else
                                      params.proc_select
                                 end || p_suffix;
        sig constant str := lower(prc_name) || nlt || '(' || nltt ||
                            str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type), ',' || nltt) || nlt ||
                            ') return ' || type_rt_name || case
                                when params.use_result_cache and not g_includes_lobs and not p_for_update then
                                 ' result_cache'
                            end;
        l_row    constant str := 'l_' || param_name_row;
        bdy clob;
    begin
        if params.concurrency_control.proc_lock_record is null then
            return null;
        elsif p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nltt || l_row || ' ' || type_rt_name || ';';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'if ' || params.proc_exists_and_select || p_suffix || case
                   when p_for_update then
                    '_lock'
               end || '(' || str_join(stringf(p_cons, tag_param), ', ') || ', ' || l_row || ') then';
        bdy := bdy || nlttt || 'return ' || l_row || ';';
        bdy := bdy || nltt || 'else';
        bdy := bdy || nlttt || case
                   when params.return_null_when_no_data_found then
                    l_row || ' := null;'
                   else
                    'raise no_data_found;'
               end;
        bdy := bdy || nltt || 'end if;';
        return bdy || nlt || 'end;';
    end;

    function tapi_select_occ_uk
    (
        p_cons        in tab_cols_t,
        p_only_header in boolean,
        p_suffix      in varchar2 default null
    ) return clob is
        doc       constant str := tab || '/**' || doc_sel || nlt ||
                                  '* Also captures the checksum of the record for later to perform an optimistic write lock check prior modifying the record in DB.' ||
                                  doc_sel_result_cache || doc_sel_null_check || doc_sel_no_data_found || nlt || '*/';
        prc_name  constant str := 'function ' || params.proc_select || p_suffix || opt_lock_suffix;
        sig       constant str := lower(prc_name) || nlt || '(' || nltt ||
                                  str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type), ',' || nltt) || nlt ||
                                  ') return ' || type_rt_name_check;
        l_var_occ constant str := 'l_' || param_name_row || cur_checksum_name_suffix;
        cur_name  constant str := cursor_prefix || coalesce(p_suffix, cursor_suffix_pk) || cur_checksum_name_suffix;
        bdy clob;
    begin
        if not priv_to_dbms_crypto or not params.concurrency_control.opt_lock_generate or
           params.concurrency_control.row_version_column is not null then
            return null;
        elsif p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nltt || l_var_occ || ' ' || type_rt_name_check || ';';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || case
                   when params.check_pk_values_before_select then
                    nltt ||
                    str_join(stringf(p_cons, proc_name_assert_not_null || '(' || tag_param || ', ''' || tag_col || ''');'),
                             nltt) || nl
               end;
        bdy := bdy || nltt || 'open ' || cur_name || '(' || str_join(stringf(p_cons, tag_param), ', ') || ');';
        bdy := bdy || nltt || 'fetch ' || cur_name;
        bdy := bdy || nltt || ' into ' || l_var_occ || ';';
        bdy := bdy || nltt || 'close ' || cur_name || ';';
        bdy := bdy || nltt || 'return ' || l_var_occ || ';';
        return bdy || nlt || 'end;';
    end;

    function tapi_exists_select_uk
    (
        p_cons        in tab_cols_t,
        p_only_header in boolean,
        p_suffix      in varchar2 default null,
        p_for_update  in boolean default false
    ) return clob is
        doc constant str := tab || '/**' || nlt || '* Returns true, if the row exists, false otherwise.' || nlt ||
                            '* If the record exists, the function will return the record as an output parameter' || case
                                when p_for_update then
                                 ' and aquires a write lock on this record'
                            end || doc_sel_result_cache || nlt || '* Checks for null primary key columns ' || nlt || '*/';
        prc_name constant str := 'function ' || params.proc_exists_and_select || p_suffix || case
                                     when p_for_update then
                                      '_lock'
                                 end;
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := lower(prc_name) || nlt || '(' || nltt ||
                                 str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type), ',' || nltt) || ',' || nltt ||
                                 param || ' out nocopy ' || type_rt_name || nlt || ') return boolean';
        l_exists constant str := 'l_found';
        cur_idx constant str := cursor_prefix || coalesce(p_suffix, cursor_suffix_pk) || case
                                    when p_for_update then
                                     '_lock'
                                end;
        bdy clob;
    begin
        if p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nltt || l_exists || ' boolean;';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || case
                   when params.check_pk_values_before_select then
                    nltt ||
                    str_join(stringf(p_cons, proc_name_assert_not_null || '(' || tag_param || ', ''' || tag_col || ''');'),
                             nltt) || nl
               end;
        bdy := bdy || nltt || 'open ' || cur_idx || '(' || str_join(stringf(p_cons, tag_param), ', ') || ');';
        bdy := bdy || nltt || 'fetch ' || cur_idx;
        bdy := bdy || nltt || ' into ' || param || ';';
        bdy := bdy || nltt || l_exists || ' := ' || cur_idx || '%found;';
        bdy := bdy || nltt || 'close ' || cur_idx || ';';
        bdy := bdy || nltt || 'return ' || l_exists || ';';
        return bdy || nlt || 'end;';
    end;

    function tapi_exists_rt(p_only_header in boolean) return clob is
        doc      constant str := tab || '/**' || nlt || '* Returns true, if the record exists, false otherwise.' || nlt || '*/';
        prc_name constant str := 'function ' || params.proc_exists;
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || type_rt_name || nlt ||
                                 ') return boolean';
        bdy clob;
    begin
        if p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'return ' || params.proc_exists || '(' ||
               str_join(stringf(g_pk_cols, col_rec(param)), ', ') || ');';
        return bdy || nlt || 'end;';
    end;

    function tapi_log_dup_val_on_index
    (
        p_param_name in varchar2,
        p_without_pk in boolean default false
    ) return clob is
        bdy         clob;
        exc_handler str_list := str_list();
    begin
        for i in 1 .. g_cons.count loop
            if (p_without_pk and g_cons(i).c_type = 'P') or g_cons(i).c_type = 'N' then
                continue;
            end if;
            exc_handler.extend;
            exc_handler(exc_handler.last) := 'when instr(lower(sqlerrm), ''' || lower(g_cons(i).c_name) ||
                                             ''') > 0 then';
            exc_handler.extend;
            exc_handler(exc_handler.last) := tab || log_exception('sqlerrm || '': '' || ' || proc_name_pk_string || '(' ||
                                                                  p_param_name || ')');
            exc_handler.extend;
            exc_handler(exc_handler.last) := tab || log_exception(params.proc_diff || '(' || params.proc_select || g_cons(i).c_suffix || '(' ||
                                                                  str_join(stringf(read_cons_cols(g_cons(i).c_name),
                                                                                   col_rec(p_param_name)),
                                                                           ', ') || '), ' || p_param_name || ')' ||
                                                                  '.to_string');
        end loop;
    
        if exc_handler.count > 0 then
            bdy := nltt || 'when dup_val_on_index then';
            bdy := bdy || nlttt || 'case';
            bdy := bdy || nltttt || str_join(exc_handler, nltttt);
            bdy := bdy || nlttt || 'end case;';
            return bdy || nlttt || 'raise;';
        else
            return null;
        end if;
    end;

    function exceptions_forall
    (
        rows_var   in str,
        errors_var in str,
        ret_rows   in str default null
    ) return clob is
        bdy clob;
    begin
        if params.log_exception_procedure is null then
            return null;
        end if;
    
        bdy := bdy || nlt || 'exception';
        bdy := bdy || nltt || 'when forall_error then';
        bdy := bdy || nlttt || 'for i in 1 .. sql%bulk_exceptions.count loop';
        bdy := bdy || nltttt || errors_var || '.extend;';
        bdy := bdy || nltttt || errors_var || '(' || errors_var || '.last' || ') := ' || rows_var ||
               '(sql%bulk_exceptions(i).error_index);';
        bdy := bdy || case
                   when params.log_exception_procedure is not null then
                    nltttt || log_exception('sqlerrm(-sql%bulk_exceptions(i).error_code)')
               end;
        bdy := bdy || nlttt || 'end loop;';
        return bdy || case when ret_rows is not null then nlttt || 'return ' || ret_rows || ';' end;
    end;

    function tapi_update_rt(p_only_header in boolean) return clob is
        doc      constant str := tab || '/**' || nlt || ' * Updates the row with the modified values.' || nlt || ' */';
        prc_name constant str := 'procedure ' || params.proc_update;
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in out nocopy ' || type_rt_name || nlt || ')';
        l_column_not_changed str_list;
        bdy                  clob;
    begin
        if p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt ||
               str_join(stringf(g_pk_cols,
                                proc_name_assert_not_null || '(' || col_rec(param) || ', ''' || tag_col || ''');'),
                        nltt);
        bdy := bdy || nl;
        bdy := bdy || nltt || 'update ' || table_name() || ' t';
        bdy := bdy || nltt || '   set ' || str_join(stringf(exclude(non_pk(changables), g_audit_cols_created),
                                                            col_quote('t.') || ' = ' || col_default(col_rec(param))),
                                                    ',' || nltt || '       ');
        bdy := bdy || nltt || ' where ' ||
               str_join(stringf(g_pk_cols, col_quote('t.') || ' = ' || col_rec(param)), nltt || '   and ');
    
        l_column_not_changed := str_join(stringf(non_pk(comparables), not_equal_exp(col_quote('t.'), col_rec(param))),
                                         stringf(include(g_cols, lob_types),
                                                 not_equal_exp(col_quote('t.'), col_rec(param), c_type_blob)));
        if l_column_not_changed.count > 0 then
            bdy := bdy || nltt || '   and    (' || str_join(l_column_not_changed, nltt || '        or ') || ')';
        end if;
    
        bdy := bdy || nltt || 'returning ' || str_join(stringf(g_cols, col_quote('t.')), ', ') || ' into ' || param || ';';
        if with_cloud_events then
            bdy := bdy || nltt || proc_name_emit_cloud_event || '(''update'', $$plsql_unit, ' || name_proc_json_obj || '(' ||
                   param || '));';
        end if;
    
        if params.log_exception_procedure is not null then
            bdy := bdy || nlt || 'exception';
            bdy := bdy || nltt || 'when ' || ex_cannot_update_null || ' then';
            bdy := bdy || nlttt || log_exception('sqlerrm');
            bdy := bdy || nlttt || 'raise;';
            bdy := bdy || tapi_log_dup_val_on_index(param, true);
        end if;
        return bdy || nlt || 'end;';
    end;

    function tapi_update_occ(p_only_header in boolean) return clob is
        doc      constant str := tab || '/**' || nlt || '* Updates the row with the modified values.' || nlt || '*/';
        prc_name constant str := 'procedure ' || params.proc_update || opt_lock_suffix;
        param    constant str := params.parameter_prefix || param_name_row;
        sig constant str := prc_name || nlt || '(' || nltt || param || ' in out nocopy ' || case
                                when params.concurrency_control.row_version_column is not null then
                                 type_rt_name
                                else
                                 type_rt_name_check
                            end || nlt || ')';
        bdy clob;
    begin
        if not priv_to_dbms_crypto or not params.concurrency_control.opt_lock_generate then
            return null;
        elsif p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt ||
               str_join(stringf(g_pk_cols,
                                proc_name_assert_not_null || '(' || col_rec(param) || ', ''' || tag_col || ''');'),
                        nltt);
        if params.concurrency_control.row_version_column is not null then
            bdy := bdy || nltt || proc_name_assert_not_null || '(' || param || '.' ||
                   lower(params.concurrency_control.row_version_column) || ', ''' ||
                   lower(params.concurrency_control.row_version_column) || ''');';
        end if;
        bdy := bdy || nl;
        bdy := bdy || nltt || 'update ' || table_name() || ' t';
        bdy := bdy || nltt || '   set ' || str_join(stringf(exclude(non_pk(changables), g_audit_cols_created),
                                                            col_quote('t.') || ' = ' || col_default(col_rec(param))),
                                                    ',' || nltt || '       ');
        bdy := bdy || nltt || ' where ' ||
               str_join(stringf(g_pk_cols, col_quote('t.') || ' = ' || col_rec(param)), nltt || '   and ');
        if params.concurrency_control.row_version_column is not null then
            bdy := bdy || nltt || '   and ' || 't.' || lower(params.concurrency_control.row_version_column) || ' = ' ||
                   param || '.' || lower(params.concurrency_control.row_version_column);
        else
            bdy := bdy || nltt || '   and ' || tapi_name || '.' || params.proc_checksum || '(' ||
                   str_join(stringf(non_audit, col_quote('t.')), ', ') || ') = ' || param || '.' ||
                   params.proc_checksum;
        end if;
        bdy := bdy || nltt || 'returning ' || str_join(stringf(g_cols, col_quote('t.')), ', ');
        if params.concurrency_control.row_version_column is null then
            bdy := bdy || ',' || nltt || '          ' || tapi_name || '.' || params.proc_checksum || '(' ||
                   str_join(stringf(non_audit, col_quote('t.')), ', ') || ')';
        end if;
        bdy := bdy || ' into ' || param || ';';
        bdy := bdy || nltt || 'if sql%rowcount = 0 then';
        bdy := bdy || nlttt ||
               'raise_application_error(-20000, ''Current version of data in database has changed since last read.'');';
        bdy := bdy || nltt || 'end if;';
        return bdy || nlt || 'end;';
    end;

    function tapi_delete_occ(p_only_header in boolean) return clob is
        doc      constant str := tab || '/**' || nlt || '* Updates the row with the modified values.' || nlt || '*/';
        prc_name constant str := 'procedure ' || params.proc_delete || opt_lock_suffix;
        param    constant str := params.parameter_prefix || param_name_row;
        sig constant str := prc_name || nlt || '(' || nltt || param || ' in ' || case
                                when params.concurrency_control.row_version_column is not null then
                                 type_rt_name
                                else
                                 type_rt_name_check
                            end || nlt || ')';
        bdy clob;
    begin
        if not priv_to_dbms_crypto or not params.concurrency_control.opt_lock_generate then
            return null;
        elsif p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt ||
               str_join(stringf(g_pk_cols,
                                proc_name_assert_not_null || '(' || col_rec(param) || ', ''' || tag_col || ''');'),
                        nltt);
        if params.concurrency_control.row_version_column is not null then
            bdy := bdy || nltt || proc_name_assert_not_null || '(' || param || '.' ||
                   lower(params.concurrency_control.row_version_column) || ', ''' ||
                   lower(params.concurrency_control.row_version_column) || ''');';
        end if;
        bdy := bdy || nl;
        bdy := bdy || nltt || 'delete ' || table_name() || ' t';
        bdy := bdy || nltt || ' where ' ||
               str_join(stringf(g_pk_cols, col_quote('t.') || ' = ' || col_rec(param)), nltt || '   and ');
        if params.concurrency_control.row_version_column is not null then
            bdy := bdy || nltt || '   and ' || 't.' || lower(params.concurrency_control.row_version_column) || ' = ' ||
                   param || '.' || lower(params.concurrency_control.row_version_column);
        else
            bdy := bdy || nltt || '   and ' || tapi_name || '.' || params.proc_checksum || '(' ||
                   str_join(stringf(non_audit, col_quote('t.')), ', ') || ') = ' || param || '.' ||
                   params.proc_checksum;
        end if;
        bdy := bdy || ';';
        bdy := bdy || nltt || 'if sql%notfound then';
        bdy := bdy || nlttt ||
               'raise_application_error(-20000, ''Current version of data in database has changed since last read.'');';
        bdy := bdy || nltt || 'end if;';
        return bdy || nlt || 'end;';
    end;

    function tapi_insert_rt_func(p_only_header in boolean) return clob is
        doc      constant str := tab || '/**' || nlt || '* Inserts a row into the table.' || nlt || '*/';
        prc_name constant str := 'function ' || params.proc_insert;
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || type_rt_name || nlt ||
                                 ') return ' || type_rt_name;
        l_var    constant str := 'ret_val';
        bdy        clob;
        pk_non_def tab_cols_t := tab_cols_t();
    begin
        if p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nltt || l_var || ' ' || type_rt_name || ' := ' || param || ';';
        bdy := bdy || nlt || 'begin';
        for i in 1 .. g_pk_cols.count loop
            if g_pk_cols(i).data_default is null then
                pk_non_def.extend();
                pk_non_def(pk_non_def.last) := g_pk_cols(i);
            end if;
        end loop;
    
        bdy := bdy || nltt ||
               str_join(stringf(pk_non_def,
                                proc_name_assert_not_null || '(' || col_rec(param) || ', ''' || tag_col || ''');'),
                        nltt);
        bdy := bdy || nl;
        bdy := bdy || nltt || 'insert into ' || table_name() || ' t';
        bdy := bdy || nlttt || '(' || str_join(stringf(changables, col_quote('t.')), ',' || nlttt || ' ') || ')';
        bdy := bdy || nltt || 'values';
        bdy := bdy || nlttt || '(' || str_join(stringf(changables, col_default(col_rec(param))), ',' || nlttt || ' ') || ')';
        bdy := bdy || nltt || 'returning ' || str_join(stringf(g_cols, col_quote('t.')), ', ') || ' into ' || l_var || ';';
        if with_cloud_events then
            bdy := bdy || nltt || proc_name_emit_cloud_event || '(''insert'', $$plsql_unit, ' || name_proc_json_obj || '(' ||
                   l_var || '));';
        end if;
    
        bdy := bdy || nltt || 'return ' || l_var || ';';
        if params.log_exception_procedure is not null then
            bdy := bdy || nlt || 'exception';
            bdy := bdy || nltt || 'when ' || ex_cannot_insert_null || ' then';
            bdy := bdy || nlttt || log_exception('sqlerrm');
            bdy := bdy || nlttt || 'raise;';
            bdy := bdy || tapi_log_dup_val_on_index(param);
        end if;
        return bdy || nlt || 'end;';
    end;

    function tapi_insert_rt_proc(p_only_header in boolean) return clob is
        doc      constant str := tab || '/**' || nlt || '* Inserts a row into the table.' || nlt || '*/';
        prc_name constant str := 'procedure ' || params.proc_insert;
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in out nocopy ' || type_rt_name || nlt || ')';
        bdy clob;
    begin
        if p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || param || ' := ' || params.proc_insert || '(' || param || ');';
        return bdy || nlt || 'end;';
    end;

    function tapi_subtype_defaults(p_only_header in boolean) return clob is
        doc      constant str := tab || '/**' || nlt || '* Returns a record with defaults as defined.' || nlt || '*/';
        prc_name constant str := 'function ' || type_rt_name || '_defaults';
        l_var    constant str := 'l_' || param_name_row;
        sig str;
        bdy clob;
    begin
        if params.defaults.init_record_expressions.count = 0 then
            return null;
        end if;
    
        sig := prc_name || nlt || '(' || nltt ||
               str_join(stringf(non_audit(changables), tag_rt_sig || ' ' || tag_type || tag_rec_default), ',' || nltt) || nlt ||
               ') return ' || type_rt_name;
        if p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nltt || l_var || ' ' || type_rt_name || ';';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt ||
               str_join(stringf(non_audit(changables), col_rec(l_var) || ' := ' || tag_rt_asg || ';'), nltt);
        bdy := bdy || nltt || 'return ' || l_var || ';';
        return bdy || nlt || 'end;';
    end;

    function tapi_exists_select_rt(p_only_header in boolean) return clob is
        doc      constant str := tab || '/**' || nlt || '* Returns true, if the row exists, false otherwise.' || nlt || '*/';
        prc_name constant str := 'function ' || params.proc_exists_and_select;
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in out nocopy ' || type_rt_name || nlt ||
                                 ') return boolean';
        bdy clob;
    begin
        if p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nltt || str_join(stringf(g_pk_cols,
                                               'l_' || tag_col || ' constant ' || tag_type || ' := ' || col_rec(param) || ';'),
                                       nltt);
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'return ' || params.proc_exists_and_select || '(' ||
               str_join(stringf(g_pk_cols, 'l_' || tag_col), ', ') || ', ' || param || ');';
        bdy := bdy || nlt || 'end;';
        return bdy;
    end;

    function tapi_select_rt_func
    (
        p_only_header in boolean,
        p_for_update  in boolean default false
    ) return clob is
        doc      constant str := tab || '/**' || doc_sel || doc_sel_lock(p_for_update) || doc_sel_result_cache ||
                                 doc_sel_null_check || doc_sel_no_data_found || nlt || '*/';
        prc_name constant str := 'function ' || case
                                     when p_for_update then
                                      params.concurrency_control.proc_lock_record
                                     else
                                      params.proc_select
                                 end;
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || type_rt_name || nlt ||
                                 ') return ' || type_rt_name;
        bdy clob;
    begin
        if params.concurrency_control.proc_lock_record is null then
            return null;
        elsif p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'return ' || case
                   when p_for_update then
                    params.concurrency_control.proc_lock_record
                   else
                    params.proc_select
               end || '(' || str_join(stringf(g_pk_cols, col_rec(param)), ', ') || ');';
        return bdy || nlt || 'end;';
    end;

    function tapi_select_rt_proc
    (
        p_only_header in boolean,
        p_for_update  in boolean default false
    ) return clob is
        doc      constant str := tab || '/**' || doc_sel || doc_sel_lock(p_for_update) || doc_sel_result_cache ||
                                 doc_sel_null_check || doc_sel_no_data_found || nlt || '*/';
        prc_name constant str := 'procedure ' || case
                                     when p_for_update then
                                      params.concurrency_control.proc_lock_record
                                     else
                                      params.proc_select
                                 end;
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in out nocopy ' || type_rt_name || nlt || ')';
        bdy clob;
    begin
        if params.concurrency_control.proc_lock_record is null then
            return null;
        elsif p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || param || ' := ' || case
                   when p_for_update then
                    params.concurrency_control.proc_lock_record
                   else
                    params.proc_select
               end || '(' || str_join(stringf(g_pk_cols, col_rec(param)), ', ') || ');';
        return bdy || nlt || 'end;';
    end;

    function tapi_print_rt(p_only_header in boolean default false) return clob is
        doc      constant str := tab || '/**' || nlt || '* Prints out all fieldnames and values of the record.' || nlt || '*/';
        prc_name constant str := 'procedure ' || params.print_proc.proc_print;
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || type_rt_name || nlt || ')';
        bdy clob;
    begin
        if params.print_proc.proc_print is null or params.print_proc.print_function is null then
            return null;
        elsif p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || str_join(stringf(g_cols,
                                               regexp_replace('''' || tag_col || ' = '' || ' || col_char(col_rec(param)),
                                                              '^(.*)$',
                                                              params.print_proc.print_function) || ';'),
                                       nltt);
        return bdy || nlt || 'end;';
    end;

    function tapi_to_string_rt(p_only_header in boolean default false) return clob is
        doc      constant str := tab || '/**' || nlt ||
                                 '* Returns a string representation of the concatenated primary key values.' || nlt || '*/';
        prc_name constant str := 'function ' || proc_name_pk_string;
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || type_rt_name || nlt ||
                                 ') return varchar2';
        bdy clob;
    begin
        if p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'return ' ||
               str_join(stringf(g_pk_cols, col_char(col_rec(param))),
                        ' || ''' || params.proc_pk_string_delim || ''' || ') || ';';
        return bdy || nlt || 'end;';
    end;

    function tapi_json_obj(p_only_header in boolean default false) return clob is
        doc      constant str := tab || '/**' || nlt || '* Returns a stringified JSON of the record.' || nlt || '*/';
        prc_name constant str := 'function ' || name_proc_json_obj;
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || type_rt_name || nlt ||
                                 ') return ' || c_type_json_obj;
        bdy clob;
    begin
        if params.proc_json_obj is null and not is_create_diff then
            return null;
        elsif p_only_header then
            return case when params.proc_json_obj is not null then nll || doc || nlt || sig || ';' end;
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nltt || 'jo ' || c_type_json_obj || ' := ' || c_type_json_obj || ';';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt ||
               str_join(stringf(g_cols, 'jo.put(''' || tag_json_key || ''', ' || col_json_val(col_rec(param)) || ');'),
                        nltt);
        bdy := bdy || nltt || 'return jo;';
        return bdy || nlt || 'end;';
    end;

    function tapi_json_import(p_only_header in boolean default false) return clob is
        doc      constant str := tab || '/**' || nlt || '* Returns a record from the given JSON object.' || nlt ||
                                 '* Complex data like timestamps and binary data is deserialized to the native Oracle types.' || nlt || '*/';
        prc_name constant str := 'function ' || params.proc_of_json;
        param    constant str := params.parameter_prefix || 'json';
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || c_type_json_obj || nlt ||
                                 ') return ' || type_rt_name;
        bdy clob;
    begin
        if params.proc_of_json is null then
            return null;
        elsif p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nltt || param_name_row || ' ' || type_rt_name || ' := ' || type_rt_name || '();';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || str_join(stringf(g_cols,
                                               col_rec(param_name_row) || ' := case' || nlttt || 'when (' || param ||
                                               '.has(''' || tag_json_key || ''') and not ' || param || '.get(''' ||
                                               tag_json_key || ''').is_null) then' || nltttt || tag_json_from_val_1 ||
                                               param || tag_json_from_val_2 || nlttt || 'end;'),
                                       nltt);
        bdy := bdy || nltt || 'return ' || param_name_row || ';';
        return bdy || nlt || 'end;';
    end;

    function tapi_json_arr(p_only_header in boolean default false) return clob is
        doc      constant str := tab || '/**' || nlt || '* Returns a stringified JSON of the record.' || nlt || '*/';
        prc_name constant str := 'function ' || 'json' || '_arr';
        param    constant str := params.parameter_prefix || 'rows';
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || type_rows_tab || nlt ||
                                 ') return json_array_t';
        bdy clob;
    begin
        if params.proc_json_obj is null then
            return null;
        elsif p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nltt || 'i pls_integer := ' || param || '.first;';
        bdy := bdy || nltt || 'ja  json_array_t := json_array_t;';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'while i is not null loop';
        bdy := bdy || nlttt || 'ja.append(' || name_proc_json_obj || '(' || param || '(i)));';
        bdy := bdy || nlttt || 'i := ' || param || '.next(i);';
        bdy := bdy || nltt || 'end loop;';
        bdy := bdy || nltt || 'return ja;';
        return bdy || nlt || 'end;';
    end;

    function tapi_merge_rt(p_only_header in boolean default false) return clob is
        doc      constant str := tab || '/**' || nlt ||
                                 '* Insert or updates the record on whether the record already exists.' || nlt || '*/';
        prc_name constant str := 'procedure ' || params.proc_merge;
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || type_rt_name || nlt || ')';
        bdy clob;
    begin
        if p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt ||
               str_join(stringf(g_pk_cols,
                                proc_name_assert_not_null || '(' || col_rec(param) || ', ''' || tag_col || ''');'),
                        nltt);
        bdy := bdy || nl;
        bdy := bdy || nltt || 'merge into ' || table_name() || ' x';
        bdy := bdy || nltt || 'using (select ';
        bdy := bdy || str_join(stringf(g_cols, col_rec(param) || ' as ' || tag_col), ',' || nltt || '              ');
        bdy := bdy || nltt || '         from dual) y';
        bdy := bdy || nltt || 'on (' ||
               str_join(stringf(g_pk_cols, col_quote('x.') || ' = ' || col_quote('y.')), ' and ') || ')';
        bdy := bdy || nltt || 'when matched then';
        bdy := bdy || nlttt || 'update';
        bdy := bdy || nlttt || '   set ';
        bdy := bdy || str_join(stringf(exclude(non_pk(changables), g_audit_cols_created),
                                       col_quote('x.') || ' = ' || col_default(col_quote('y.'))),
                               ',' || nlttt || '       ');
        bdy := bdy ||
               concat_if_not_null(nlttt || ' where ',
                                  str_join(stringf(non_pk(comparables), not_equal_exp(col_quote('x.'), col_quote('y.'))),
                                           nlttt || '    or '));
        bdy := bdy || concat_if_not_null(nlttt || '    or ',
                                         str_join(stringf(include(g_cols, lob_types),
                                                          not_equal_exp(col_quote('x.'), col_quote('y.'), c_type_blob)),
                                                  nlttt || '    or '));
        bdy := bdy || nltt || 'when not matched then';
        bdy := bdy || nlttt || 'insert';
        bdy := bdy || nltttt || '(' || str_join(stringf(changables, col_quote('x.')), ',' || nltttt || ' ') || ')';
        bdy := bdy || nlttt || 'values';
        bdy := bdy || nltttt || '(' ||
               str_join(stringf(changables, col_default(col_quote('y.'))), ',' || nltttt || ' ') || ');';
        return bdy || nlt || 'end;';
    end;

    function tapi_exist_uk
    (
        p_cons        in tab_cols_t,
        p_only_header in boolean default false,
        p_suffix      in varchar2 default null
    ) return clob is
        doc      constant str := tab || '/**' || nlt || '* Returns true, if the record exists, false otherwise.' || nlt || '*/';
        prc_name constant str := 'function ' || params.proc_exists || p_suffix;
        sig constant str := lower(prc_name) || nlt || '(' || nltt ||
                            str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type || ' default null'), ',' || nltt) || nlt ||
                            ') return boolean' || case
                                when params.use_result_cache and not g_includes_lobs then
                                 ' result_cache'
                            end;
        l_var    constant str := 'l_' || param_name_row;
        l_exists constant str := 'l_found';
        cur_idx  constant str := cursor_prefix || coalesce(p_suffix, cursor_suffix_pk);
        bdy clob;
    begin
        if p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nltt || l_var || ' ' || type_rt_name || ';';
        bdy := bdy || nltt || l_exists || ' boolean;';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'open ' || cur_idx || '(' || str_join(stringf(p_cons, tag_param), ', ') || ');';
        bdy := bdy || nltt || 'fetch ' || cur_idx;
        bdy := bdy || nltt || ' into ' || l_var || ';';
        bdy := bdy || nltt || l_exists || ' := ' || cur_idx || '%found;';
        bdy := bdy || nltt || 'close ' || cur_idx || ';';
        bdy := bdy || nltt || 'return ' || l_exists || ';';
        return bdy || nlt || 'end;';
    end;

    function tapi_exist_uk_yn
    (
        p_cons        in tab_cols_t,
        p_only_header in boolean default false,
        p_suffix      in varchar2 default null
    ) return clob is
        doc constant str := tab || '/**' || nlt || '* Returns true, if the record exists, false otherwise.' || nlt || '*/';
        prc_name str;
        sig      str;
        bdy      clob;
    begin
        if params.boolean_pseudo_type.true_value is null or params.boolean_pseudo_type.false_value is null then
            return null;
        end if;
        prc_name := 'function ' || params.proc_exists || p_suffix || '_' ||
                    lower(params.boolean_pseudo_type.true_value) || lower(params.boolean_pseudo_type.false_value);
        sig      := lower(prc_name) || nlt || '(' || nltt ||
                    str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type || ' default null'), ',' || nltt) || nlt ||
                    ') return varchar2';
    
        if p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'return case when ' || params.proc_exists || p_suffix || '(' ||
               str_join(stringf(p_cons, tag_param), ', ') || ') then ''' || params.boolean_pseudo_type.true_value ||
               ''' else ''' || params.boolean_pseudo_type.false_value || ''' end;';
        return bdy || nlt || 'end;';
    end;

    function tapi_delete_rt(p_only_header in boolean) return clob is
        doc      constant str := tab || '/**' || nlt || '* Deletes a row with the same primary key from the table.' || nlt || '*/';
        prc_name constant str := 'procedure ' || params.proc_delete;
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || type_rt_name || nlt || ')';
        bdy clob;
    begin
        if p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'del' || '(' || str_join(stringf(g_pk_cols, col_rec(param)), ', ') || ');';
        return bdy || nlt || 'end;';
    end;

    function tapi_delete_uk
    (
        p_cons        in tab_cols_t,
        p_only_header in boolean,
        p_suffix      in varchar2 default null
    ) return clob is
        doc      constant str := tab || '/**' || nlt || '* Deletes a row with the same primary key from the table.' || nlt || '*/';
        prc_name constant str := 'procedure del' || p_suffix;
        sig      constant str := lower(prc_name) || nlt || '(' || nltt ||
                                 str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type), ',' || nltt) || nlt || ')';
        bdy clob;
    begin
        if p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || case
                   when params.check_pk_values_before_select then
                    nltt ||
                    str_join(stringf(p_cons, proc_name_assert_not_null || '(' || tag_param || ', ''' || tag_col || ''');'),
                             nltt) || nl
               end;
        bdy := bdy || nltt || 'delete ' || table_name() || ' t';
        bdy := bdy || nltt || ' where ' ||
               str_join(stringf(p_cons, col_quote('t.') || ' = ' || tag_param), nltt || '   and ') || ';';
        if params.raise_error_on_failed_update_delete then
            bdy := bdy || nltt || 'if sql%notfound then';
            bdy := bdy || nlttt || 'raise no_data_found;';
            bdy := bdy || nltt || 'end if;';
        end if;
    
        return bdy || nlt || 'end;';
    end;

    function tapi_select_rows(p_only_header in boolean default false) return clob is
        doc      constant str := tab || '/**' || nlt || '* Returns the records captured by the ref cursor.' || nlt ||
                                 '* If more than the limited number of records exist, the functions has to be called as long as the return table is empty.' || nlt ||
                                 '* When the ref cursor doesn''t return any more rows, it is automacally closed.' || nlt || '*/';
        prc_name constant str := 'function ' || params.proc_select || bulk_proc_suffix;
        p_cursor constant str := params.parameter_prefix || 'ref_cursor';
        p_limit  constant str := params.parameter_prefix || 'bulk_limit';
        sig      constant str := tab || prc_name || nlt || '(' || nltt || p_cursor || ' in ' || type_ref_cursor || ',' || nltt ||
                                 p_limit || ' in pls_integer default ' || params.bulk_proc.default_limit || nlt ||
                                 ') return ' || type_rows_tab;
        l_rows   constant str := 'l_rows';
        bdy clob;
    begin
        if not params.bulk_proc.generate then
            return null;
        elsif p_only_header then
            return nll || doc || nl || sig || ';';
        end if;
    
        bdy := nll || sig || ' is';
        bdy := bdy || nltt || l_rows || ' ' || type_rows_tab || ';';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'if (' || p_cursor || '%isopen) then';
        bdy := bdy || nlttt || 'fetch ' || p_cursor || ' bulk collect';
        bdy := bdy || nlttt || ' into ' || l_rows || ' limit ' || p_limit || ';';
        bdy := bdy || nlttt || 'if (' || l_rows || '.count < ' || p_limit || ') then';
        bdy := bdy || nltttt || 'close ' || p_cursor || ';';
        bdy := bdy || nlttt || 'end if;';
        bdy := bdy || nlttt || 'return ' || l_rows || ';';
        bdy := bdy || nltt || 'else';
        bdy := bdy || nlttt || 'return ' || type_rows_tab || '();';
        bdy := bdy || nltt || 'end if;';
        return bdy || nlt || 'end;';
    end;

    function tapi_insert_rows(p_only_header in boolean) return clob is
        prc_name constant str := 'procedure ' || params.proc_insert || bulk_proc_suffix;
        param    constant str := params.parameter_prefix || 'rows';
        sig      constant str := tab || prc_name || '(' || param || ' in out nocopy ' || type_rows_tab || ')';
        ret_tab  constant str := 'ret_tab';
        bdy clob;
    begin
        if not params.bulk_proc.generate then
            return null;
        elsif p_only_header then
            return nll || sig || ';';
        end if;
        bdy := nll || sig || ' is';
        bdy := bdy || nltt || ret_tab || ' ' || type_rows_tab || ';';
        bdy := bdy || nltt || 'errors' || ' ' || type_rows_tab || ';';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || ret_tab || ' := ' || params.proc_insert || bulk_proc_suffix || '(' || param || ', ' ||
               'errors' || ');';
        bdy := bdy || nltt || 'if errors.count > 0 then';
        bdy := bdy || nlttt || 'raise ' || ex_forall_error || ';';
        bdy := bdy || nltt || 'end if;';
        bdy := bdy || nltt || param || ' := ' || ret_tab || ';';
        return bdy || nlt || 'end;';
    end;

    function tapi_insert_rows_save_exc(p_only_header in boolean) return clob is
        prc_name   constant str := 'function ' || params.proc_insert || bulk_proc_suffix;
        param      constant str := params.parameter_prefix || 'rows';
        errors_var constant str := params.parameter_prefix || 'errors';
        sig        constant str := tab || prc_name || nlt || '(' || nltt || param || ' in ' || type_rows_tab || ',' || nltt ||
                                   errors_var || ' out nocopy ' || type_rows_tab || nlt || ') return ' || type_rows_tab;
        ret_rows   constant str := 'ret_tab';
        bdy clob;
    begin
        if not params.bulk_proc.generate then
            return null;
        elsif p_only_header then
            return nll || sig || ';';
        end if;
        bdy := nll || sig || ' is';
        bdy := bdy || nltt || ret_rows || ' ' || type_rows_tab || ' := ' || type_rows_tab || '();';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || errors_var || ' := ' || type_rows_tab || '();';
        bdy := bdy || nltt || 'forall i in indices of ' || param || ' save exceptions';
        bdy := bdy || nlttt || 'insert into ' || table_name() || ' t';
        bdy := bdy || nltttt || '(' || str_join(stringf(changables, col_quote('t.')), ',' || nltttt || ' ') || ')';
        bdy := bdy || nlttt || 'values';
        bdy := bdy || nltttt || '(' ||
               str_join(stringf(changables, col_default(col_rec(param || '(i)'))), ',' || nltttt || ' ') || ')';
        bdy := bdy || nlttt || 'returning' || ' ' || str_join(stringf(g_cols, col_quote('t.')), ', ') ||
               ' bulk collect into ' || ret_rows || ';';
        if with_cloud_events then
            bdy := bdy || nltt || 'for i in 1 .. ' || ret_rows || '.count loop';
            bdy := bdy || nlttt || proc_name_emit_cloud_event || '(''insert'', $$plsql_unit, ' || 'json_obj' || '(' ||
                   ret_rows || '(i)));';
            bdy := bdy || nltt || 'end loop;';
        end if;
        bdy := bdy || nltt || 'return ' || ret_rows || ';';
    
        bdy := bdy || exceptions_forall(param, errors_var, ret_rows);
        return bdy || nlt || 'end;';
    end;

    function tapi_insert_cur(p_only_header in boolean default false) return clob is
        prc_name constant str := 'procedure ' || params.bulk_proc.proc_insert_cur;
        p_cursor constant str := params.parameter_prefix || 'ref_cursor';
        p_limit  constant str := params.parameter_prefix || 'bulk_limit';
        sig      constant str := tab || prc_name || nlt || '(' || nltt || p_cursor || ' in ' || type_ref_cursor || ',' || nltt ||
                                 p_limit || ' in pls_integer default ' || params.bulk_proc.default_limit || nlt || ')';
        bdy clob;
    begin
        if not params.bulk_proc.generate then
            return null;
        elsif p_only_header then
            return nll || sig || ';';
        end if;
        bdy := nll || sig || ' is';
        bdy := bdy || nltt || 'l_tab ' || type_rows_tab || ';';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'loop';
        bdy := bdy || nlttt || 'l_tab := ' || params.proc_select || bulk_proc_suffix || '(' || p_cursor || ', ' ||
               p_limit || ');';
        bdy := bdy || nlttt || 'exit when l_tab.count = 0;';
        bdy := bdy || nl;
        bdy := bdy || nlttt || params.proc_insert || bulk_proc_suffix || '(l_tab);';
        bdy := bdy || nltt || 'end loop;';
        return bdy || nlt || 'end;';
    end;

    function tapi_update_rows(p_only_header in boolean) return clob is
        prc_name constant str := 'procedure ' || params.proc_update || bulk_proc_suffix;
        param    constant str := params.parameter_prefix || 'rows';
        sig      constant str := tab || prc_name || nlt || '(' || nltt || param || ' in out nocopy ' || type_rows_tab || nlt || ')';
        ret_tab  constant str := 'ret_tab';
        bdy clob;
    begin
        if not params.bulk_proc.generate then
            return null;
        elsif p_only_header then
            return nll || sig || ';';
        end if;
        bdy := nll || sig || ' is';
        bdy := bdy || nltt || ret_tab || ' ' || type_rows_tab || ';';
        bdy := bdy || nltt || 'errors' || ' ' || type_rows_tab || ';';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || ret_tab || ' := ' || params.proc_update || bulk_proc_suffix || '(' || param || ', ' ||
               'errors' || ');';
        bdy := bdy || nltt || 'if errors.count > 0 then';
        bdy := bdy || nlttt || 'raise ' || ex_forall_error || ';';
        bdy := bdy || nltt || 'end if;';
        bdy := bdy || nltt || param || ' := ' || ret_tab || ';';
        return bdy || nlt || 'end;';
    end;

    function tapi_update_rows_save_exc(p_only_header in boolean) return clob is
        prc_name   constant str := 'function ' || params.proc_update || bulk_proc_suffix;
        param      constant str := params.parameter_prefix || 'rows';
        errors_var constant str := params.parameter_prefix || 'errors';
        sig        constant str := tab || prc_name || nlt || '(' || nltt || param || ' in ' || type_rows_tab || ',' || nltt ||
                                   errors_var || ' out nocopy ' || type_rows_tab || nlt || ') return ' || type_rows_tab;
        ret_rows   constant str := 'ret_tab';
        l_column_not_changed str_list;
        bdy                  clob;
    begin
        if not params.bulk_proc.generate then
            return null;
        elsif p_only_header then
            return nll || sig || ';';
        end if;
        bdy := nll || sig || ' is';
        bdy := bdy || nltt || ret_rows || ' ' || type_rows_tab || ' := ' || type_rows_tab || '();';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || errors_var || ' := ' || type_rows_tab || '();';
        bdy := bdy || nltt || 'forall i in indices of ' || param || ' save exceptions';
        bdy := bdy || nlttt || 'update ' || table_name() || ' t';
        bdy := bdy || nlttt || '   set ' ||
               str_join(stringf(exclude(non_pk(changables), g_audit_cols_created),
                                col_quote('t.') || ' = ' || col_default(col_rec(param || '(i)'))),
                        ',' || nltttt || '    ');
        bdy := bdy || nlttt || ' where ' ||
               str_join(stringf(g_pk_cols, col_quote('t.') || ' = ' || col_rec(param || '(i)')), nlttt || '   and ');
    
        l_column_not_changed := str_join(stringf(non_pk(comparables),
                                                 not_equal_exp(col_quote('t.'), col_rec(param || '(i)'))),
                                         stringf(include(g_cols, lob_types),
                                                 not_equal_exp(col_quote('t.'), col_rec(param || '(i)'), c_type_blob)));
        if l_column_not_changed.count > 0 then
            bdy := bdy || nlttt || '   and    (' || str_join(l_column_not_changed, nlttt || '        or ') || ')';
        end if;
    
        bdy := bdy || nlttt || 'returning' || ' ' || str_join(stringf(g_cols, col_quote), ', ') ||
               ' bulk collect into ' || ret_rows || ';';
        if with_cloud_events then
            bdy := bdy || nltt || 'for i in 1 .. ' || ret_rows || '.count loop';
            bdy := bdy || nlttt || proc_name_emit_cloud_event || '(''update'', $$plsql_unit, ' || 'json_obj' || '(' ||
                   ret_rows || '(i)));';
            bdy := bdy || nltt || 'end loop;';
        end if;
        bdy := bdy || nltt || 'return ' || ret_rows || ';';
    
        bdy := bdy || exceptions_forall(param, errors_var, ret_rows);
        return bdy || nlt || 'end;';
    end;

    function tapi_delete_rows(p_only_header in boolean) return clob is
        prc_name constant str := 'procedure ' || params.proc_delete || bulk_proc_suffix;
        param    constant str := params.parameter_prefix || 'rows';
        sig      constant str := tab || prc_name || nlt || '(' || nltt || param || ' in ' || type_rows_tab || nlt || ')';
        bdy clob;
    begin
        if not params.bulk_proc.generate then
            return null;
        elsif p_only_header then
            return nll || sig || ';';
        end if;
        bdy := nll || sig || ' is';
        bdy := bdy || nltt || 'errors' || ' ' || type_rows_tab || ';';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || params.proc_delete || bulk_proc_suffix || '(' || param || ', ' || 'errors' || ');';
        bdy := bdy || nltt || 'if errors.count > 0 then';
        bdy := bdy || nlttt || 'raise ' || ex_forall_error || ';';
        bdy := bdy || nltt || 'end if;';
        return bdy || nlt || 'end;';
    end;

    function tapi_delete_rows_save_exc(p_only_header in boolean) return clob is
        prc_name   constant str := 'procedure ' || params.proc_delete || bulk_proc_suffix;
        param      constant str := params.parameter_prefix || 'rows';
        errors_var constant str := params.parameter_prefix || 'errors';
        sig        constant str := tab || prc_name || nlt || '(' || nltt || param || ' in ' || type_rows_tab || ',' || nltt ||
                                   errors_var || ' out nocopy ' || type_rows_tab || nlt || ')';
        bdy clob;
    begin
        if not params.bulk_proc.generate then
            return null;
        elsif p_only_header then
            return nll || sig || ';';
        end if;
        bdy := nll || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || errors_var || ' := ' || type_rows_tab || '();';
        bdy := bdy || nltt || 'forall i in indices of ' || param || ' save exceptions';
        bdy := bdy || nlttt || 'delete ' || table_name() || ' t';
        bdy := bdy || nlttt || ' where ';
        bdy := bdy ||
               str_join(stringf(g_pk_cols, col_quote('t.') || ' = ' || col_rec(param || '(i)')), nlttt || '   and ') || ';';
        if with_cloud_events then
            bdy := bdy || nltt || 'for i in 1 .. ' || param || '.count loop';
            bdy := bdy || nlttt || proc_name_emit_cloud_event || '(''delete'', $$plsql_unit, ' || 'json_obj' || '(' ||
                   param || '(i)));';
            bdy := bdy || nltt || 'end loop;';
        end if;
    
        bdy := bdy || exceptions_forall(param, errors_var);
        return bdy || nlt || 'end;';
    end;

    function tapi_pipe_rows(p_only_header in boolean default false) return clob is
        prc_name constant str := 'function ' || params.proc_pipe;
        param    constant str := params.parameter_prefix || 'ref_cursor';
        sig      constant str := tab || prc_name || '(' || param || ' in ' || type_ref_cursor || ') return ' ||
                                 type_rows_tab || nltt || 'pipelined';
        l_var    constant str := 'l_' || param_name_row;
        bdy clob;
    begin
        if params.proc_pipe is null then
            return null;
        elsif p_only_header then
            return nll || sig || ';';
        end if;
        bdy := nll || sig || ' is';
        bdy := bdy || nltt || l_var || ' ' || type_rt_name || ';';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'if not ' || param || '%isopen then';
        bdy := bdy || nlttt || 'return;';
        bdy := bdy || nltt || 'end if;';
        bdy := bdy || nltt || 'loop';
        bdy := bdy || nlttt || 'fetch ' || param;
        bdy := bdy || nlttt || ' into ' || l_var || ';';
        bdy := bdy || nlttt || 'exit when ' || param || '%notfound;';
        bdy := bdy || nlttt || 'pipe row(' || l_var || ');';
        bdy := bdy || nltt || 'end loop;';
        bdy := bdy || nltt || 'close ' || param || ';';
        bdy := bdy || nltt || 'return;';
        bdy := bdy || nlt || 'exception';
        bdy := bdy || nltt || 'when no_data_needed then';
        bdy := bdy || nlttt || 'close ' || param || ';';
        bdy := bdy || nlttt || 'raise;';
        return bdy || nlt || 'end;';
    end;

    function tapi_checksum_col(p_only_header in boolean) return clob is
        doc       constant str := tab || '/**' || nlt || '* Returns a SHA512 hash of the concatenated values.' || nlt || '*/';
        prc_name  constant str := 'function ' || params.proc_checksum;
        sig       constant str := prc_name || nlt || '(' || nltt ||
                                  str_join(stringf(non_audit, tag_sig_param || ' in ' || tag_type), ',' || nltt) || nlt ||
                                  ') return varchar2 deterministic';
        proc_hash constant str := 'sys.dbms_crypto.hash';
        bdy clob;
    begin
        if not priv_to_dbms_crypto then
            return null;
        elsif p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'return ' || proc_hash || '(';
        bdy := bdy || str_join(stringf(non_audit, col_char(tag_param)), ' || ') || ',';
        bdy := bdy || nltt || '        ' || indent(proc_hash) || 'sys.dbms_crypto.hash_sh512);';
        return bdy || nlt || 'end;';
    end;

    function tapi_checksum_rt(p_only_header in boolean) return clob is
        doc      constant str := tab || '/**' || nlt || '* Returns an SHA512 hash of the record.' || nlt || '*/';
        prc_name constant str := 'function ' || params.proc_checksum;
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || type_rt_name || nlt ||
                                 ') return varchar2';
        bdy clob;
    begin
        if not priv_to_dbms_crypto or not params.concurrency_control.opt_lock_generate then
            return '';
        elsif p_only_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || 'return ' || params.proc_checksum || '(' ||
               str_join(stringf(non_audit, col_rec(param)), ', ') || ');';
        return bdy || nlt || 'end;';
    end;

    function tapi_subtypes return clob is
        spc clob;
    begin
        spc := nll || tab ||
               str_join(stringf(g_cols,
                                'subtype ' || tag_col || type_name_suffix || ' is ' || tag_type || tag_not_null || ';'),
                        nlt);
        return spc || nlt || 'subtype ' || type_col_hash || ' is varchar2(128);';
    end;

    function tapi_record_rt return clob is
        decl str := 'type ' || type_rt_name || ' is record';
        spc  clob;
    begin
        spc := nll || tab || decl || nlt || '(' || nltt ||
               str_join(stringf(g_cols, tag_rt_sig || ' ' || tag_type), ',' || nltt) || nlt || ');';
    
        if priv_to_dbms_crypto and params.concurrency_control.opt_lock_generate and
           params.concurrency_control.row_version_column is null then
            decl := 'type ' || type_rt_name_check || ' is record';
            spc  := spc || nll || tab || decl || '(' ||
                    str_join(stringf(g_cols, tag_rt_asg || ' ' || tag_type), nlt || indent(decl) || ',');
            spc  := spc || nlt || indent(decl) || ',' || 'checksum' || '    ' || type_col_hash;
            spc  := spc || ');';
        end if;
    
        return spc;
    end;

    function tapi_strip_checksum(gen_header in boolean default false) return clob is
        doc      constant str := tab || '/**' || nlt || '* Returns a record of type ' || type_rt_name ||
                                 ' after removing the checksum of the given record.' || nlt || '*/';
        prc_name constant str := 'function ' || 'strip_checksum';
        param    constant str := params.parameter_prefix || param_name_row;
        sig      constant str := prc_name || nlt || '(' || nltt || param || ' in ' || type_rt_name_check || nlt ||
                                 ') return ' || type_rt_name;
        bdy   clob;
        l_var str := 'l_rt';
    begin
        if not priv_to_dbms_crypto or not params.concurrency_control.opt_lock_generate or
           params.concurrency_control.row_version_column is not null then
            return '';
        elsif gen_header then
            return nll || doc || nlt || sig || ';';
        end if;
    
        bdy := nll || tab || sig || ' is';
        bdy := bdy || nltt || l_var || ' ' || type_rt_name || ';';
        bdy := bdy || nlt || 'begin';
        bdy := bdy || nltt || str_join(stringf(g_cols, col_rec('l_rt') || ' := ' || col_rec(param) || ';'), nltt);
        bdy := bdy || nltt || 'return ' || l_var || ';';
        return bdy || nlt || 'end;';
    end;

    function gen_procedures(gen_header in boolean default false) return clob is
        aquire_lock constant boolean := true;
        bdy clob;
    begin
        bdy := bdy || tapi_subtype_defaults(gen_header);
        bdy := bdy || tapi_checksum_rt(gen_header);
        bdy := bdy || tapi_strip_checksum(gen_header);
        bdy := bdy || tapi_json_obj(gen_header);
        bdy := bdy || tapi_json_arr(gen_header);
        bdy := bdy || tapi_json_import(gen_header);
        bdy := bdy || tapi_diff_recs(gen_header);
        bdy := bdy || tapi_xxdo(gen_header);
        bdy := bdy || tapi_redo(gen_header);
        bdy := bdy || tapi_undo(gen_header);
        for i in 1 .. g_cons.count loop
            if g_cons(i).c_type = 'N' then
                continue;
            end if;
            declare
                l_cons_cols tab_cols_t := read_cons_cols(g_cons(i).c_name);
            begin
                bdy := bdy || tapi_exists_select_uk(l_cons_cols, gen_header, g_cons(i).c_suffix);
                bdy := bdy || tapi_exists_select_uk(l_cons_cols, gen_header, g_cons(i).c_suffix, aquire_lock);
                bdy := bdy || tapi_select_uk(l_cons_cols, gen_header, g_cons(i).c_suffix);
                bdy := bdy || tapi_select_uk(l_cons_cols, gen_header, g_cons(i).c_suffix, aquire_lock);
                bdy := bdy || tapi_select_occ_uk(l_cons_cols, gen_header, g_cons(i).c_suffix);
                bdy := bdy || tapi_exist_uk(l_cons_cols, gen_header, g_cons(i).c_suffix);
                bdy := bdy || tapi_exist_uk_yn(l_cons_cols, gen_header, g_cons(i).c_suffix);
                bdy := bdy || tapi_counts_uk(l_cons_cols, gen_header, g_cons(i).c_suffix);
                bdy := bdy || tapi_delete_uk(l_cons_cols, gen_header, g_cons(i).c_suffix);
            end;
        end loop;
        bdy := bdy || tapi_exists_select_rt(gen_header);
        bdy := bdy || tapi_exists_rt(gen_header);
        bdy := bdy || tapi_select_rt_func(gen_header);
        bdy := bdy || tapi_select_rt_proc(gen_header);
        bdy := bdy || tapi_select_rt_func(gen_header, aquire_lock);
        bdy := bdy || tapi_select_rt_proc(gen_header, aquire_lock);
        bdy := bdy || tapi_insert_rt_func(gen_header);
        bdy := bdy || tapi_insert_rt_proc(gen_header);
        bdy := bdy || tapi_update_rt(gen_header);
        bdy := bdy || tapi_update_occ(gen_header);
        bdy := bdy || tapi_delete_rt(gen_header);
        bdy := bdy || tapi_delete_occ(gen_header);
        bdy := bdy || tapi_merge_rt(gen_header);
        bdy := bdy || tapi_select_rows(gen_header);
        bdy := bdy || tapi_insert_rows(gen_header);
        bdy := bdy || tapi_insert_rows_save_exc(gen_header);
        bdy := bdy || tapi_insert_cur(gen_header);
        bdy := bdy || tapi_update_rows(gen_header);
        bdy := bdy || tapi_update_rows_save_exc(gen_header);
        bdy := bdy || tapi_delete_rows(gen_header);
        bdy := bdy || tapi_delete_rows_save_exc(gen_header);
        bdy := bdy || tapi_pipe_rows(gen_header);
        bdy := bdy || tapi_print_rt(gen_header);
        bdy := bdy || tapi_to_string_rt(gen_header);
        return bdy;
    end;

    function tapi_cursor_indices(p_only_header in boolean) return clob is
        bdy clob;
    
        function cursor_idx
        (
            p_cols_tab    in tab_cols_t,
            p_suffix      in varchar2,
            p_for_update  in boolean,
            p_only_header in boolean default false
        ) return clob is
            cur_name constant str := 'cursor ' || cursor_prefix || coalesce(p_suffix, cursor_suffix_pk) || case
                                         when p_for_update then
                                          '_lock'
                                     end;
            sig      constant str := nll || tab || lower(cur_name) || nlt || '(' || nltt ||
                                     str_join(stringf(p_cols_tab, tag_sig_param || ' ' || tag_type || ' default null'),
                                              ',' || nltt) || nlt || ') return ' || type_rt_name;
            spc clob;
        begin
            if p_only_header then
                return sig || ';';
            end if;
        
            spc := sig || ' is';
            spc := spc || nltt || 'select *';
            spc := spc || nltt || '  from ' || table_name() || ' t';
            spc := spc || nltt || ' where ' ||
                   str_join(stringf(p_cols_tab,
                                    col_quote('t.') || ' = nvl(' || tag_param || ', ' || col_quote('t.') || ')'),
                            nltt || '   and ');
            spc := spc || case
                       when p_for_update then
                        nltt || '   for update' || case
                            when params.concurrency_control.acquire_lock_timeout < 0 then
                             null
                            when params.concurrency_control.acquire_lock_timeout = 0 then
                             ' nowait'
                            else
                             ' wait ' || params.concurrency_control.acquire_lock_timeout
                        end
                   end;
            return spc || ';';
        end;
    
        function cursor_idx_occ
        (
            p_cols_tab    in tab_cols_t,
            p_suffix      in varchar2,
            p_only_header in boolean default false
        ) return clob is
            cur_name constant str := 'cursor ' || cursor_prefix || coalesce(p_suffix, cursor_suffix_pk) ||
                                     cur_checksum_name_suffix;
            sig      constant str := nll || tab || lower(cur_name) || nlt || '(' || nltt ||
                                     str_join(stringf(p_cols_tab, tag_sig_param || ' ' || tag_type || ' default null'),
                                              ',' || nltt) || nlt || ') return ' || type_rt_name_check;
            spc clob;
        begin
            if not priv_to_dbms_crypto or not params.concurrency_control.opt_lock_generate or
               params.concurrency_control.row_version_column is not null then
                return null;
            elsif p_only_header then
                return sig || ';';
            end if;
        
            spc := sig || ' is';
            spc := spc || nltt || 'select t.*,';
            spc := spc || nltt || '       ' || tapi_name || '.' || params.proc_checksum || '(' ||
                   str_join(stringf(non_audit, col_quote('t.')), ', ') || ')';
            spc := spc || nltt || '  from ' || table_name() || ' t';
            spc := spc || nltt || ' where ' ||
                   str_join(stringf(p_cols_tab,
                                    col_quote('t.') || ' = nvl(' || tag_param || ', ' || col_quote('t.') || ')'),
                            nltt || '   and ');
            return spc || ';';
        end;
    begin
        for i in 1 .. g_cons.count loop
            declare
                l_cons_cols tab_cols_t := read_cons_cols(g_cons(i).c_name);
            begin
                bdy := bdy || cursor_idx(l_cons_cols, g_cons(i).c_suffix, false, p_only_header);
                bdy := bdy || cursor_idx(l_cons_cols, g_cons(i).c_suffix, true, p_only_header);
                if g_cons(i).c_type member of str_list('P', 'U') then
                    bdy := bdy || cursor_idx_occ(l_cons_cols, g_cons(i).c_suffix, p_only_header);
                end if;
            end;
        end loop;
    
        return bdy;
    end;

    function create_header(p_gen_info in clob default null) return clob is
        doc constant str := '/**' || nl || '* Generated package for table ' || table_name() || nl || '*/';
        spc clob;
    begin
        spc := doc || nl || 'create or replace package ' || owner_name() || '.' || tapi_name() || ' authid definer is';
        if p_gen_info is not null then
            spc := spc || nl || p_gen_info;
        end if;
        spc := spc || tapi_subtypes();
        spc := spc || tapi_record_rt();
        spc := spc || nll || tab || 'type ' || type_rows_tab || '          is table of ' || type_rt_name || ';';
        spc := spc || nll || tab || 'type ' || type_ref_cursor || ' is ref cursor return ' || type_rt_name || ';';
        spc := spc || tapi_checksum_col(true);
        spc := spc || tapi_cursor_indices(true);
        spc := spc || gen_procedures(true);
        return spc || nll || 'end;';
    end;

    function create_body(p_gen_info in clob default null) return clob is
        bdy clob;
    begin
        bdy := 'create or replace package body ' || owner_name() || '.' || tapi_name() || ' is';
        if p_gen_info is not null then
            bdy := bdy || nl || p_gen_info;
        end if;
        bdy := bdy || nl;
        bdy := bdy || nlt || ex_cannot_insert_null || ' exception;';
        bdy := bdy || nlt || 'pragma exception_init(' || ex_cannot_insert_null || ', -1400);';
        bdy := bdy || nlt || ex_cannot_update_null || ' exception;';
        bdy := bdy || nlt || 'pragma exception_init(' || ex_cannot_update_null || ', -1407);';
        bdy := bdy || nlt || ex_forall_error || ' exception;';
        bdy := bdy || nlt || 'pragma exception_init (' || ex_forall_error || ', -24381);';
        bdy := bdy || tapi_cursor_indices(false);
        bdy := bdy || tapi_assert_not_null;
        bdy := bdy || tapi_base64_encode;
        bdy := bdy || tapi_base64_decode;
        bdy := bdy || tapi_emit_cloud_event;
        bdy := bdy || tapi_checksum_col(false);
        bdy := bdy || gen_procedures;
        bdy := bdy || nll || 'begin';
        bdy := bdy || nlt || 'null;';
        bdy := bdy || nl || 'end;' || nl;
        return bdy;
    end;

    procedure init_single_run
    (
        p_table_name in varchar2,
        p_owner      in varchar2 default user
    ) is
        function check_table(p_table_name in varchar2) return varchar2 is
            l_table_name obj_col;
        begin
            select t.table_name
              into l_table_name
              from all_tables t
             where t.owner = g_owner
                   and upper(t.table_name) = upper(sys.dbms_assert.simple_sql_name(p_table_name));
            return l_table_name;
        exception
            when no_data_found then
                raise_application_error(-20000, 'Table ' || p_table_name || ' does not exist!');
        end;
    begin
        if params.tapi_name.count = 0 then
            raise_application_error(-20000, 'Initialize generator first!');
        end if;
        if p_table_name is null then
            raise_application_error(-20000, 'No tablename given!');
        end if;
        g_owner      := upper(sys.dbms_assert.schema_name(p_owner));
        g_table_name := check_table(p_table_name);
        read_tab_cols();
        if params.concurrency_control.row_version_column is not null and include(p_tab_cols => g_cols, p_include_name => str_list(upper(params.concurrency_control.row_version_column))).count = 0 then
            raise_application_error(-20000,
                                    'Column ' || params.concurrency_control.row_version_column ||
                                    ' not contained in table.');
        end if;
        if params.use_result_cache and g_includes_lobs then
            raise_application_error(-20000, 'RESULT_CACHE is not allowed for tables containg LOB types.');
        end if;
        read_constraints();
        if g_pk_cols.count = 0 then
            raise_application_error(-20000,
                                    'Table ' || g_owner || '.' || g_table_name ||
                                    ' has no primary key or usable substitutes, skipping generating access package.');
        end if;
    end;

    function tapi_source
    (
        p_table_name  in varchar2,
        p_schema_name in varchar2 default user
    ) return clob is
    begin
        init_single_run(p_table_name, p_schema_name);
        return create_header || nll || create_body;
    end;

    procedure compile_tapi
    (
        p_table_name  in varchar2,
        p_schema_name in varchar2 default user
    ) is
        l_gen_info str := tab || '/* Generated by TAPIR PL/SQL Tapi-Generator on ' ||
                          to_char(sysdate, date_format_iso_8601) || ' */';
        start_time constant pls_integer := sys.dbms_utility.get_time;
    begin
        init_single_run(p_table_name, p_schema_name);
        if params.plsql_optimize_level is not null then
            execute immediate 'alter session set plsql_optimize_level = ' || params.plsql_optimize_level;
        end if;
    
        execute immediate create_header(l_gen_info);
        execute immediate create_body(l_gen_info);
        sys.dbms_output.put_line(tapi_name || ' [' ||
                                 mod(sys.dbms_utility.get_time - start_time + c_bignum, c_bignum) / 100 || ' sec]');
    end;

    procedure compile_tapis
    (
        p_owner     in varchar2 default user,
        p_name_like in varchar2 default null
    ) is
    begin
        for tab in (select t.owner,
                           t.table_name
                      from all_tables t
                     where t.owner = p_owner
                           and (p_name_like is null or t.table_name like upper('%' || p_name_like || '%'))
                     order by t.table_name) loop
            begin
                compile_tapi(tab.table_name, tab.owner);
            exception
                when others then
                    sys.dbms_output.put_line('Generating TAPI package failed for table: ' || tab.table_name || ' (' ||
                                             sqlerrm || ')');
            end;
        end loop;
    end;

    procedure create_ce_table
    (
        p_table_name     in varchar2,
        p_schema_name    in varchar2 default user,
        p_immutable      in boolean default false,
        p_retention_days in pls_integer default null
    ) is
        l_exists number;
        l_ce_tab str;
    begin
        if p_immutable and not (to_number(dbms_db_version.version || '.' || dbms_db_version.release) >= 19.11) then
            raise_application_error(-20000, 'Immutable tables require at least compatibility level 19.11.');
        end if;
    
        select count(*)
          into l_exists
          from all_tables t
         where t.owner = sys.dbms_assert.schema_name(p_schema_name)
               and upper(t.table_name) = upper(sys.dbms_assert.qualified_sql_name(p_table_name))
               and rownum = 1;
        if l_exists = 0 then
            l_ce_tab := 'create ' || case
                            when p_immutable then
                             'immutable '
                        end || 'table ' || p_schema_name || '.' || p_table_name || ' (';
            l_ce_tab := l_ce_tab || nltt || 'ce_id     raw(11) not null,';
            l_ce_tab := l_ce_tab || nltt || 'ce_time   timestamp not null,';
            l_ce_tab := l_ce_tab || nltt || 'ce_type   varchar2(100) not null,';
            l_ce_tab := l_ce_tab || nltt || 'ce_source varchar2(200) not null,';
            l_ce_tab := l_ce_tab || nltt || 'ce_data clob,';
            l_ce_tab := l_ce_tab || nlt || 'constraint ' || p_table_name || '_json check (ce_data is json),';
            l_ce_tab := l_ce_tab || nlt || 'constraint ' || p_table_name || '_pk primary key(ce_id) using index)';
            l_ce_tab := l_ce_tab || case
                            when p_immutable and p_retention_days is not null then
                             ' no drop until ' || p_retention_days || ' days idle' || nlt || ' no delete until ' ||
                             p_retention_days || ' days after insert'
                        end;
        
            execute immediate l_ce_tab;
            execute immediate 'alter table ' || p_table_name || ' disable constraint ' || p_table_name || '_json';
        end if;
    end;

    procedure create_ce_queue
    (
        p_queue_name  in varchar2,
        p_schema_name in varchar2 default user,
        p_event_type  in varchar2 default null
    ) is
        l_type  all_types.type_name%type;
        l_table all_tables.table_name%type;
    begin
        if not has_priv_for_sys_('DBMS_AQADM', p_schema_name) then
            raise_application_error(-20000, 'User ' || p_schema_name || ' is missing privileges to access DBMS_AQADM.');
        end if;
    
        begin
            select t.type_name
              into l_type
              from all_types t
             where t.owner = sys.dbms_assert.schema_name(p_schema_name)
                   and upper(t.type_name) = upper(nvl(p_event_type, type_cloud_event));
        exception
            when no_data_found then
                execute immediate 'create or replace type ' || p_schema_name || '.' ||
                                  nvl(p_event_type, type_cloud_event) || ' authid definer as object' || '(' ||
                                  'ce_id raw(11),' || nl || 'ce_time timestamp,' || nl || 'ce_type varchar2(100),' || nl ||
                                  'ce_source varchar2(200),' || nl || 'ce_data clob' || nl || ');';
        end;
    
        begin
            select t.table_name
              into l_table
              from all_tables t
             where t.owner = upper(p_schema_name)
                   and t.table_name = upper(sys.dbms_assert.simple_sql_name(p_queue_name) || ce_table_name_suffix);
        exception
            when no_data_found then
                execute immediate 'begin' || nl || 'sys.dbms_aqadm.create_queue_table(queue_table => ''' ||
                                  p_schema_name || '.'' || ' || nl || 'sys.dbms_assert.simple_sql_name(''' ||
                                  p_queue_name || ''') || ''' || ce_table_name_suffix || ''',' || nl ||
                                  'queue_payload_type => ''' || nvl(p_event_type, type_cloud_event) || ''',' || nl ||
                                  'sort_list => ''enq_time'',' || nl || 'multiple_consumers => true,' || nl ||
                                  'compatible => ''10.0'',' || nl || 'comment => ''cloudevents from ' ||
                                  upper($$plsql_unit) || ''');' || nl || 'end;';
                execute immediate 'begin' || nl || 'sys.dbms_aqadm.create_queue(queue_name => ''' || p_schema_name ||
                                  ''' || ''.'' || ''' || p_queue_name || ''',' || nl || 'queue_table => ''' ||
                                  p_schema_name || ''' || ''.'' || ''' || p_queue_name || ce_table_name_suffix || ''',' || nl ||
                                  'queue_type => 0,' || nl || 'max_retries => 2000000000,' || nl || 'retry_delay => 0,' || nl ||
                                  'dependency_tracking => false);' || nl || 'end;';
                execute immediate 'begin' || nl || 'sys.dbms_aqadm.start_queue(''' || p_queue_name || ''');' || nl ||
                                  'end;';
        end;
    end;

    procedure drop_ce_queue
    (
        p_queue_name  in varchar2,
        p_schema_name in varchar2 default user,
        p_drop_type   in boolean default false
    ) is
        l_type str;
    begin
        if not has_priv_for_sys_('DBMS_AQADM', p_schema_name) then
            raise_application_error(-20000, 'User ' || p_schema_name || ' is missing privileges to access DBMS_AQADM.');
        end if;
    
        if p_drop_type then
            begin
                select object_type
                  into l_type
                  from all_queue_tables
                 where owner = upper(p_schema_name)
                       and queue_table = upper(p_queue_name || '_tab');
            exception
                when no_data_found then
                    raise_application_error(-20000, 'Queue table does not exist.');
            end;
        end if;
        execute immediate 'begin' || nl || 'sys.dbms_aqadm.stop_queue(queue_name => sys.dbms_assert.schema_name(''' ||
                          p_schema_name || ''')' || ' || ''.'' || ' || 'sys.dbms_assert.simple_sql_name(''' ||
                          p_queue_name || '''), wait => false);' || nl || 'end;';
        execute immediate 'begin' || nl || 'sys.dbms_aqadm.drop_queue(''' || p_schema_name || ''' || ''.'' || ''' ||
                          p_queue_name || ''');' || nl || 'end;';
        execute immediate 'begin' || nl || 'sys.dbms_aqadm.drop_queue_table(''' || p_schema_name || ''' || ''.'' || ''' ||
                          p_queue_name || ce_table_name_suffix || ''');' || nl || 'end;';
        if p_drop_type then
            execute immediate 'drop type ' || l_type;
        end if;
    end;

    procedure init(p_params params_t) is
    begin
        priv_to_dbms_crypto := has_priv_for_sys_('DBMS_CRYPTO');
        priv_to_dbms_aqadm  := has_priv_for_sys_('DBMS_AQADM');
        params              := p_params;
    
        tab     := params.indent;
        nlt     := nl || tab;
        nltt    := nlt || tab;
        nlttt   := nltt || tab;
        nltttt  := nlttt || tab;
        nlttttt := nltttt || tab;
    
        params.proc_select                          := sys.dbms_assert.simple_sql_name(params.proc_select);
        params.proc_update                          := sys.dbms_assert.simple_sql_name(params.proc_update);
        params.proc_insert                          := sys.dbms_assert.simple_sql_name(params.proc_insert);
        params.bulk_proc.proc_insert_cur := case
                                                when params.bulk_proc.proc_insert_cur is not null then
                                                 sys.dbms_assert.simple_sql_name(params.bulk_proc.proc_insert_cur)
                                            end;
        params.proc_delete                          := sys.dbms_assert.simple_sql_name(params.proc_delete);
        params.proc_merge := case
                                 when params.proc_merge is not null then
                                  sys.dbms_assert.simple_sql_name(params.proc_merge)
                             end;
        params.proc_exists                          := sys.dbms_assert.simple_sql_name(params.proc_exists);
        params.proc_exists_and_select               := sys.dbms_assert.simple_sql_name(params.proc_exists_and_select);
        params.concurrency_control.proc_lock_record := case
                                                           when params.concurrency_control.proc_lock_record is not null then
                                                            sys.dbms_assert.simple_sql_name(params.concurrency_control.proc_lock_record)
                                                       end;
        params.proc_count := case
                                 when params.proc_count is not null then
                                  sys.dbms_assert.simple_sql_name(params.proc_count)
                             end;
        params.print_proc.proc_print := case
                                            when params.print_proc.proc_print is not null then
                                             sys.dbms_assert.simple_sql_name(params.print_proc.proc_print)
                                        end;
        params.proc_json_obj := case
                                    when params.proc_json_obj is not null then
                                     sys.dbms_assert.simple_sql_name(params.proc_json_obj)
                                end;
        params.proc_of_json := case
                                   when params.proc_of_json is not null then
                                    sys.dbms_assert.simple_sql_name(params.proc_of_json)
                               end;
        params.proc_checksum := case
                                    when params.proc_checksum is not null then
                                     sys.dbms_assert.simple_sql_name(params.proc_checksum)
                                end;
        params.proc_diff := case
                                when params.proc_diff is not null then
                                 sys.dbms_assert.simple_sql_name(params.proc_diff)
                            end;
        params.proc_pipe := case
                                when params.proc_pipe is not null then
                                 sys.dbms_assert.simple_sql_name(params.proc_pipe)
                            end;
        params.parameter_prefix := case
                                       when params.parameter_prefix is not null then
                                        sys.dbms_assert.simple_sql_name(params.parameter_prefix)
                                   end;
        params.log_exception_procedure              := lower(params.log_exception_procedure);
        if params.audit.user_exp is null then
            params.audit.col_created_by  := null;
            params.audit.col_modified_by := null;
        end if;
    
        if not params.double_quote_names then
            params.audit.col_created_by    := upper(params.audit.col_created_by);
            params.audit.col_modified_by   := upper(params.audit.col_modified_by);
            params.audit.col_created_date  := upper(params.audit.col_created_date);
            params.audit.col_modified_date := upper(params.audit.col_modified_date);
        end if;
    
        g_audit_cols          := str_list(params.audit.col_created_by,
                                          params.audit.col_modified_by,
                                          params.audit.col_created_date,
                                          params.audit.col_modified_date);
        g_audit_cols_created  := str_list(params.audit.col_created_by, params.audit.col_created_date);
        g_audit_cols_modified := str_list(params.audit.col_modified_by, params.audit.col_modified_date);
        g_audit_cols_user     := str_list(params.audit.col_created_by, params.audit.col_modified_by);
        g_audit_cols_date     := str_list(params.audit.col_created_date, params.audit.col_modified_date);
    
        with_cloud_events := false;
        if params.cloud_events.aq_queue_name is not null then
            declare
                l_priv all_queues.owner%type;
            begin
                select distinct nvl(p.grantee, q.owner)
                  into l_priv
                  from all_queues q
                  left outer join all_tab_privs p on q.owner = p.table_schema
                                                     and q.name = p.table_name
                                                    --and p.grantee = g_owner
                                                     and p.privilege = 'ENQUEUE'
                 where q.name = upper(params.cloud_events.aq_queue_name);
                with_cloud_events := with_cloud_events or l_priv is not null;
            exception
                when no_data_found then
                    raise_application_error(-20000,
                                            'Queue ''' || params.cloud_events.aq_queue_name ||
                                            ''' not found or no privlege to enqueue granted.');
            end;
        end if;
        if params.cloud_events.table_name is not null then
            declare
                l_priv all_tables.owner%type;
            begin
                select distinct nvl(p.grantee, t.owner)
                  into l_priv
                  from all_tables t
                  left outer join all_tab_privs p on t.owner = p.table_schema
                                                     and t.table_name = p.table_name
                                                     and p.privilege = 'INSERT'
                 where t.table_name = upper(params.cloud_events.table_name);
                with_cloud_events := with_cloud_events or l_priv is not null;
            exception
                when no_data_found then
                    raise_application_error(-20000,
                                            'Table ''' || params.cloud_events.table_name ||
                                            ''' not found or no privlege to insert granted.');
            end;
        end if;
    end;

begin
    null;
end;
/
