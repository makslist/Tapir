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
   c_type_rowid                          constant str := 'rowid';
   c_type_json_obj                       constant str := 'json_object_t';

   supported_types constant str_list := str_list(c_type_varchar2,
                                                 c_type_number,
                                                 c_type_date,
                                                 c_type_timestamp,
                                                 c_type_blob,
                                                 c_type_clob,
                                                 c_type_rowid);
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
   number_types    constant str_list := str_list(c_type_number, c_type_float, c_type_binary_float, c_type_binary_double);
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

   tab     str := '   ';
   nlt     str := nl || tab;
   nltt    str := nlt || tab;
   nlttt   str := nltt || tab;
   nltttt  str := nlttt || tab;
   nlttttt str := nltttt || tab;

   tag_col             constant str := '<col_lower>';
   tag_col_pad         constant str := '<col_rpad>';
   tag_type            constant str := '<col_type>';
   tag_param           constant str := '<col_param>';
   tag_sig_param       constant str := '<sig_param>';
   tag_not_null        constant str := '<not_nullable/>';
   tag_quote_1         constant str := '<col_quote>';
   tag_quote_2         constant str := '</col_quote>';
   tag_rt_def          constant str := '<col_rt_def>';
   tag_rt_1            constant str := '<col_rt>';
   tag_rt_2            constant str := '</col_rt>';
   tag_col_default_1   constant str := '<default>';
   tag_col_default_2   constant str := '</default>';
   tag_col_to_char_1   constant str := '<to_char>';
   tag_col_to_char_2   constant str := '</to_char>';
   tag_cust_default    constant str := '<custom_default>';
   tag_json_key        constant str := '<json_key>';
   tag_json_to_val_1   constant str := '<to_json_val>';
   tag_json_to_val_2   constant str := '</to_json_val>';
   tag_json_from_val_1 constant str := '<from_json_val>';
   tag_json_from_val_2 constant str := '</from_json_val>';

   type_name_suffix           constant str := '_t';
   type_rt_name               constant str := 'rt';
   occ_name_suffix            constant str := '_opt';
   type_rt_name_occ           constant str := type_rt_name || occ_name_suffix;
   type_rows_tab              constant str := 'rows_tab';
   type_ref_cursor            constant str := 'strong_ref_cursor';
   type_col_hash              constant str := 'checksum_t';
   type_cloud_event           constant str := 'cloud_event';
   ce_table_name_suffix       constant str := '_tab';
   cursor_prefix              constant str := 'cur_idx';
   cursor_suffix_pk           constant str := '_pk';
   proc_name_raise_if_null    constant str := 'raise_if_null';
   proc_name_pk_string        constant str := 'pk_str';
   param_name_row             constant str := 'rec';
   proc_name_emit_cloud_event constant str := 'emit_cloud_event';
   ex_forall_error            constant str := 'forall_error';
   ex_not_null_constraint     constant str := 'not_null_constraint';

   doc_sel            constant str := nlt || ' * Returns a single record from the table.';
   doc_sel_null_check constant str := nlt || ' * Checks for NULL values in the primary key columns.';

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
      return case when p_for_update then nlt || ' * Aquires a row level lock on this record.' end;
   end;

   function doc_sel_no_data_found return str is
   begin
      return case when params.select_return_null_when_no_data_found then nlt || ' * Returns NULL if no record was found.' else nlt || ' * Raises a no_data_found exception if no record was found.' end;
   end;

   function doc_sel_result_cache return str is
   begin
      return case when params.use_result_cache and not g_includes_lobs then nlt || ' * Uses the PL/SQL Function Result Cache when using an Enterprise Edition.' end;
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
         select c.column_name as name
               ,max(length(c.column_name)) over(partition by c.table_name) as max_length
               ,lower(c.data_type) as data_type
               ,c.data_default
               ,c.nullable
               ,substr(c.virtual_column, 1, 1) as virtual_column
               ,substr(c.identity_column, 1, 1) as identity_column
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
      for idx in 1 .. g_cols.count loop
         if lower(g_cols(idx).data_default) = 'null' then
            g_cols(idx).data_default := null;
         else
            g_cols(idx).data_default := regexp_replace(g_cols(idx).data_default,
                                                       '(^([[:space:]]|[[:cntrl:]])*|([[:space:]]|[[:cntrl:]])*$)');
         end if;
      
         if lower(g_cols(idx).data_type) member of lob_types then
            g_includes_lobs := true;
         end if;
      
         if lower(g_cols(idx).data_type) member of str_list(c_type_blob, c_type_raw) then
            g_includes_binaries := true;
         end if;
      end loop;
   end;

   function read_cons_cols(c_constraint_name in varchar2) return tab_cols_t is
      cursor c_ref_cursor is
         select c.column_name as name
               ,max(length(c.column_name)) over(partition by c.table_name) as max_length
               ,lower(c.data_type) as data_type
               ,c.data_default
               ,c.nullable
               ,substr(c.virtual_column, 1, 1) as virtual_column
               ,substr(c.identity_column, 1, 1) as identity_column
           from all_tab_cols c
           join (select *
                   from (select cc.owner
                               ,cc.table_name
                               ,cc.constraint_name
                               ,cc.column_name
                               ,cc.position
                           from all_cons_columns cc
                         union all
                         select ic.table_owner
                               ,ic.table_name
                               ,ic.index_name      as constraint_name
                               ,ic.column_name
                               ,ic.column_position as position
                           from all_ind_columns ic
                          where ic.index_name = c_constraint_name)
                  group by owner
                          ,table_name
                          ,constraint_name
                          ,column_name
                          ,position
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
         select min(c_name) keep(dense_rank first order by decode(c_type, 'P', 1, 'U', 2, 3)) as c_name
               ,min(c_type) keep(dense_rank first order by decode(c_type, 'P', 1, 'U', 2, 3)) as c_type
               ,lower(min(c_suffix) keep(dense_rank first order by decode(c_type, 'P', 1, 'U', 2, 3))) as c_suffix
           from (select c.constraint_name as c_name
                       ,c.constraint_type as c_type
                       ,case
                           when c.constraint_type = 'P' then
                            null
                           when c.constraint_name like 'SYS%' then
                            '_' || listagg(cc.column_name, '_') within group(order by cc.position)
                           else
                            '_' || canonicalize_name(c.table_name, c.constraint_name)
                        end as c_suffix
                       ,listagg(cc.column_name || cc.position) within group(order by cc.position) as key
                   from all_constraints c
                   join all_cons_columns cc on c.owner = cc.owner
                                           and c.constraint_name = cc.constraint_name
                  where c.owner = g_owner
                    and c.table_name = g_table_name
                    and c.constraint_type in ('P', 'U')
                  group by c.constraint_name
                          ,c.constraint_type
                          ,c.table_name
                 union all
                 select i.index_name as c_name
                       ,substr(i.uniqueness, 1, 1) as c_type
                       ,case
                           when i.index_name = 'P' then
                            null
                           when i.index_name like 'SYS%' then
                            '_' || listagg(ic.column_name, '_') within group(order by ic.column_position)
                           else
                            '_' || canonicalize_name(i.table_name, i.index_name)
                        end as c_suffix
                       ,listagg(ic.column_name || ic.column_position) within group(order by ic.column_position) as key
                   from all_indexes i
                   join all_ind_columns ic on i.owner = ic.index_owner
                                          and i.index_name = ic.index_name
                  where i.owner = g_owner
                    and i.table_owner = g_owner
                    and i.table_name = g_table_name
                    and i.index_type = 'NORMAL'
                  group by i.index_name
                          ,i.uniqueness
                          ,i.table_name) i
          group by key
          order by decode(c_type, 'P', 1, 'U', 2, 3)
                  ,case
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
      if g_cons.count > 0 and g_cons(1).c_type = 'P' then
         g_pk_cols := read_cons_cols(g_cons(1).c_name);
      else
         g_pk_cols := tab_cols_t();
      end if;
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
      for idx in 1 .. p_tab_cols.count loop
         if p_tab_cols(idx).virtual_column = 'N' and p_tab_cols(idx).identity_column = 'N' and
             not nvl(p_tab_cols(idx).col_name member of params.exclude_column_when_writing, false) then
            l_tab_cols.extend();
            l_tab_cols(l_tab_cols.last) := p_tab_cols(idx);
         end if;
      end loop;
      return l_tab_cols;
   end;

   function comparables(p_tab_cols in tab_cols_t default g_cols) return tab_cols_t is
      l_tab_cols tab_cols_t := tab_cols_t();
   begin
      for idx in 1 .. p_tab_cols.count loop
         if (p_tab_cols(idx).col_name member of scalar_types)
           -- or (params.audit_ignore_when_comparing and p_tab_cols(idx).name member of g_audit_cols)
            or p_tab_cols(idx).data_type member of lob_types then
            null;
         else
            l_tab_cols.extend();
            l_tab_cols(l_tab_cols.last) := p_tab_cols(idx);
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
      for idx in 1 .. p_tab_cols.count loop
         if p_tab_cols(idx).col_name member of p_exclude_name or p_tab_cols(idx).data_type member of p_exclude_type then
            null;
         else
            l_tab_cols.extend();
            l_tab_cols(l_tab_cols.last) := p_tab_cols(idx);
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
      for idx in 1 .. p_tab_cols.count loop
         if p_tab_cols(idx).col_name member of p_include_name or p_tab_cols(idx).data_type member of p_include_type then
            l_tab_cols.extend();
            l_tab_cols(l_tab_cols.last) := p_tab_cols(idx);
         end if;
      end loop;
      return l_tab_cols;
   end;

   function non_pk(p_tab_cols in tab_cols_t default g_cols) return tab_cols_t is
      function col_names(p_columns in tab_cols_t) return str_list is
         list str_list := str_list();
      begin
         for idx in 1 .. p_columns.count loop
            list.extend();
            list(list.last) := p_columns(idx).col_name;
         end loop;
         return list;
      end;
   begin
      return exclude(p_tab_cols, col_names(g_pk_cols));
   end;

   function non_audit(p_tab_cols in tab_cols_t default g_cols) return tab_cols_t is
      l_tab_cols tab_cols_t := tab_cols_t();
   begin
      for idx in 1 .. p_tab_cols.count loop
         if p_tab_cols(idx).col_name member of g_audit_cols then
            null;
         else
            l_tab_cols.extend();
            l_tab_cols(l_tab_cols.last) := p_tab_cols(idx);
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
      for idx in 1 .. p_cols.count loop
         l_list.extend();
         l_list(l_list.last) := p_mask;
         l_list(l_list.last) := replace(l_list(l_list.last),
                                        tag_type,
                                        table_name() || '.' || enquote(p_cols(idx).col_name) || '%type');
         l_list(l_list.last) := replace(l_list(l_list.last),
                                        tag_json_key,
                                        lower(p_cols(idx).col_name) || case
                                           when p_cols(idx).data_type member of str_list(c_type_clob, c_type_raw) then
                                            '_base64'
                                        end);
         l_list(l_list.last) := regexp_replace(l_list(l_list.last),
                                               tag_rt_1 || '([^<]*)' || tag_rt_2,
                                               '\1' || lower(p_cols(idx).col_name) || case
                                                  when lower(p_cols(idx).col_name) = lower(g_table_name) then
                                                   '_col'
                                               end);
         l_list(l_list.last) := replace(l_list(l_list.last),
                                        tag_sig_param,
                                        lower(params.parameter_prefix) ||
                                        lower(rpad(p_cols(idx).col_name, p_cols(idx).max_length, ' ')));
         l_list(l_list.last) := replace(l_list(l_list.last),
                                        tag_param,
                                        lower(params.parameter_prefix || p_cols(idx).col_name));
         l_list(l_list.last) := replace(l_list(l_list.last), tag_col, lower(p_cols(idx).col_name));
         l_list(l_list.last) := replace(l_list(l_list.last),
                                        tag_col_pad,
                                        lower(rpad(p_cols(idx).col_name, p_cols(idx).max_length, ' ')));
         l_list(l_list.last) := replace(l_list(l_list.last),
                                        tag_rt_def,
                                        lower(rpad(p_cols(idx).col_name || case
                                                       when lower(p_cols(idx).col_name) = lower(g_table_name) then
                                                        '_col'
                                                    end,
                                                   p_cols(idx).max_length,
                                                   ' ')));
         l_list(l_list.last) := regexp_replace(l_list(l_list.last),
                                               tag_not_null,
                                               case
                                                  when p_cols(idx).nullable = 'N' then
                                                   ' not null'
                                                  else
                                                   ''
                                               end);
         l_list(l_list.last) := regexp_replace(l_list(l_list.last),
                                               tag_col_default_1 || '(.*)' || tag_col_default_2,
                                               case
                                                  when p_cols(idx).data_default is not null then
                                                   'nvl(\1, ' || trim(p_cols(idx).data_default) || ')'
                                                  when params.column_default_expressions.exists(p_cols(idx).col_name) then
                                                   params.column_default_expressions(p_cols(idx).col_name)
                                                  when p_cols(idx).col_name = params.increase_row_version_column then
                                                   'nvl(\1, 0) + 1'
                                                  when p_cols(idx).col_name member of g_audit_cols_date then
                                                   case p_cols(idx).data_type
                                                      when c_type_date then
                                                       'sysdate'
                                                      when c_type_timestamp then
                                                       'systimestamp'
                                                   end
                                                  when p_cols(idx).col_name member of g_audit_cols_user then
                                                   'nvl(\1, ' || params.audit_user_exp || ')'
                                                  else
                                                   '\1'
                                               end);
         if instr(l_list(l_list.last), tag_cust_default) > 0 and
            not params.custom_default_expressions.exists(p_cols(idx).col_name) then
            raise_application_error(-20000,
                                    'Missing custom default expression for column ''' || p_cols(idx).col_name || '''');
         end if;
         l_list(l_list.last) := replace(l_list(l_list.last),
                                        tag_cust_default,
                                        case
                                           when params.custom_default_expressions.exists(p_cols(idx).col_name) then
                                            params.custom_default_expressions(p_cols(idx).col_name)
                                        end);
         l_list(l_list.last) := regexp_replace(l_list(l_list.last),
                                               tag_col_to_char_1 || '(.*)' || tag_col_to_char_2,
                                               case
                                                  when p_cols(idx).data_type member of number_types then
                                                   'to_char(\1' || case
                                                      when params.export_number_format is not null then
                                                       ', ''' || params.export_number_format || ''''
                                                   end || ')'
                                                  when p_cols(idx).data_type member of datetime_types then
                                                   'to_char(\1' || case
                                                      when params.export_date_format is not null then
                                                       ', ''' || params.export_date_format || ''''
                                                   end || ')'
                                                  when p_cols(idx).data_type member of binary_types then
                                                   'base64_encode(\1)'
                                                  when p_cols(idx).data_type member of str_list(c_type_clob, c_type_nclob) then
                                                   '\1'
                                                  when p_cols(idx).data_type = c_type_bool then
                                                   'case when \1 then ''true'' else ''false'' end'
                                                  when p_cols(idx).data_type = c_type_rowid then
                                                   'to_char(\1)'
                                                  when p_cols(idx).data_type member of char_types then
                                                   '\1'
                                               end);
         l_list(l_list.last) := regexp_replace(l_list(l_list.last),
                                               tag_json_from_val_1 || '([^<]*)' || tag_json_from_val_2,
                                               case
                                                  when p_cols(idx).data_type = c_type_number then
                                                   '\1.get_number(''' || lower(p_cols(idx).col_name) || ''')'
                                                  when p_cols(idx).data_type = c_type_date then
                                                   '\1.get_date(''' || lower(p_cols(idx).col_name) || ''')'
                                                  when p_cols(idx).data_type = c_type_timestamp then
                                                   '\1.get_timestamp(''' || lower(p_cols(idx).col_name) || ''')'
                                                  when p_cols(idx).data_type = c_type_bool then
                                                   '\1.get_boolean(''' || lower(p_cols(idx).col_name) || ''')'
                                                  when p_cols(idx).data_type = c_type_clob then
                                                   '\1.get_clob(''' || lower(p_cols(idx).col_name) || ''')'
                                                  when p_cols(idx).data_type = c_type_blob then
                                                   'base64_decode(\1.get_clob(''' || lower(p_cols(idx).col_name) || '_base64' || '''))'
                                                  else
                                                   '\1.get_string(''' || lower(p_cols(idx).col_name) || ''')'
                                               end);
         l_list(l_list.last) := regexp_replace(l_list(l_list.last),
                                               tag_quote_1 || '([^<]*)' || tag_quote_2,
                                               enquote('\1' || p_cols(idx).col_name));
         l_list(l_list.last) := regexp_replace(l_list(l_list.last),
                                               tag_json_to_val_1 || '([^<]*)' || tag_json_to_val_2,
                                               case
                                                  when p_cols(idx).data_type member of str_list(c_type_blob, c_type_raw) then
                                                   'base64_encode(\1)'
                                                  when p_cols(idx).data_type member of str_list(c_type_date, c_type_timestamp) then
                                                   'to_char(\1' || case
                                                      when params.export_date_format is not null then
                                                       ', ''' || params.export_date_format || ''''
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
      p_list      in str_list,
      p_delimiter in varchar2
   ) return clob is
      l_res clob;
   begin
      for idx in 1 .. p_list.count loop
         l_res := l_res || case
                     when idx > 1 then
                      p_delimiter
                  end || p_list(idx);
      end loop;
      return l_res;
   end;

   function str_join
   (
      p_str1  in varchar2,
      p_delim in varchar2,
      p_str2  in varchar2
   ) return str is
   begin
      return case when p_str2 is null then p_str1 when p_str1 is null then p_str2 when p_delim is null then p_str1 || p_str2 else p_str1 || p_delim || p_str2 end;
   end;

   function canonicalize_name
   (
      p_name_ref in varchar2,
      p_name     in varchar2
   ) return varchar2 is
      initials constant varchar2(30) := regexp_replace(p_name_ref, '([a-z])[a-z]*[^a-z]*', '\1', 1, 0, 'i');
   begin
      return coalesce(trim(regexp_substr(p_name, '(' || initials || '|' || p_name_ref || ')' || '_(.*)', 1, 1, 'i', 2)),
                      p_name);
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
      return regexp_replace(errmsg, '^(.*)$', params.logging_exception_procedure) || ';';
   end;

   function tapi_raise_if_null return str is
      prc_name constant str := 'procedure ' || proc_name_raise_if_null;
      p_val    constant str := params.parameter_prefix || 'val';
      p_param  constant str := params.parameter_prefix || 'param';
      sig      constant str := tab || prc_name || '(' || p_val || '       in varchar2' || nlt || indent(prc_name) || ',' ||
                               p_param || ' in varchar2' || ')';
      errmsg   constant str := '''Parameter '''''' || ' || p_param || ' || '''''' is null.''';
      bdy str;
   begin
      bdy := nll || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'if ' || p_val || ' is null';
      bdy := bdy || nltt || 'then';
      bdy := bdy || case
                when params.logging_exception_procedure is not null then
                 nlttt || log_exception(errmsg)
             end;
      bdy := bdy || nlttt || 'raise_application_error(-20000, ' || errmsg || ');';
      bdy := bdy || nltt || 'end if;';
      return bdy || nlt || 'end;';
   end;

   function tapi_base64_encode return str is
      p_val constant str := params.parameter_prefix || 'val';
      sig   constant str := tab || 'function base64_encode(' || p_val || ' in blob) return clob';
      bdy str;
   begin
      if not g_includes_binaries then
         return null;
      end if;
      bdy := nll || sig || ' is';
      bdy := bdy || nltt || 'c_step constant pls_integer := 8191 * 3;';
      bdy := bdy || nltt || 'l_b64 clob;';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'if ' || p_val || ' is null then';
      bdy := bdy || nlttt || 'return null;';
      bdy := bdy || nltt || 'end if;';
      bdy := bdy || nltt || 'for idx in 0 .. trunc((sys.dbms_lob.getlength(' || p_val || ') - 1) / c_step)';
      bdy := bdy || nltt || 'loop';
      bdy := bdy || nlttt || 'l_b64 := l_b64 ||';
      bdy := bdy || nlttt || '         ' || 'utl_raw.cast_to_varchar2(utl_encode.base64_encode(sys.dbms_lob.substr(' ||
             p_val || ', c_step, idx * c_step + 1)));';
      bdy := bdy || nltt || 'end loop;';
      bdy := bdy || nltt || 'return l_b64;';
      return bdy || nlt || 'end;';
   end;

   function tapi_base64_decode return str is
      p_val constant str := params.parameter_prefix || 'val';
      sig   constant str := tab || 'function base64_decode(' || p_val || ' in clob) return blob';
      bdy str;
   begin
      if not g_includes_binaries then
         return null;
      end if;
      bdy := nll || sig || ' is';
      bdy := bdy || nltt || 'c_step constant pls_integer := 8191 * 3;';
      bdy := bdy || nltt || 'temp  blob;';
      bdy := bdy || nltt || 'l_raw raw(32767);';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'if p_val is null then';
      bdy := bdy || nlttt || 'return null;';
      bdy := bdy || nltt || 'end if;';
      bdy := bdy || nltt || 'sys.dbms_lob.createtemporary(temp, false, dbms_lob.call);';
      bdy := bdy || nltt || 'for idx in 0 .. trunc((sys.dbms_lob.getlength(' || p_val || ') - 1) / c_step)';
      bdy := bdy || nltt || 'loop';
      bdy := bdy || nlttt || 'l_raw := sys.utl_raw.cast_to_raw(sys.dbms_lob.substr(p_val, c_step, idx * c_step + 1));';
      bdy := bdy || nlttt || 'sys.dbms_lob.append(temp, to_blob(sys.utl_encode.base64_decode(l_raw)));';
      bdy := bdy || nltt || 'end loop;';
      bdy := bdy || nltt || 'return temp;';
      return bdy || nlt || 'end;';
   end;

   function tapi_emit_cloud_event return str is
      p_name constant str := 'procedure ' || proc_name_emit_cloud_event;
      sig    constant str := tab || p_name || '(p_type   in varchar2' || nlt || indent(p_name) ||
                             ',p_source in varchar2' || nlt || indent(p_name) || ',p_data   in ' || c_type_json_obj || ')';
      bdy str;
   begin
      if not nvl(with_cloud_events, false) then
         return null;
      end if;
      bdy := nll || sig || ' is';
      bdy := bdy || nltt || 'l_ce_id integer;';
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
      if params.log_cloud_events.exists('tab_name') then
         bdy := bdy || nltt || 'insert into ' || params.log_cloud_events('tab_name');
         bdy := bdy || nlttt || '(ce_id';
         bdy := bdy || nlttt || ',ce_time';
         bdy := bdy || nlttt || ',ce_type';
         bdy := bdy || nlttt || ',ce_source';
         bdy := bdy || nlttt || ',ce_data)';
         bdy := bdy || nltt || 'values';
         bdy := bdy || nlttt || '(utl_raw.cast_from_number(l_ce_id)';
         bdy := bdy || nlttt || ',to_char(sys_extract_utc(systimestamp), ''' || gc_date_format_iso_8601 || ''')';
         bdy := bdy || nlttt || ',p_type';
         bdy := bdy || nlttt || ',p_source';
         bdy := bdy || nlttt || ',p_data.to_string);';
      end if;
   
      if priv_to_dbms_aqadm and params.log_cloud_events.exists('aq_queue_name') then
         bdy := bdy || nl;
         bdy := bdy || nltt || 'sys.dbms_aq.enqueue(queue_name         => ''' ||
                params.log_cloud_events('aq_queue_name') || '''';
         bdy := bdy || nlttt || ',enqueue_options    => sys.dbms_aq.enqueue_options_t()';
         bdy := bdy || nlttt || ',message_properties => sys.dbms_aq.message_properties_t(';
         bdy := bdy || nltttt || 'recipient_list => sys.dbms_aq.aq$_recipient_list_t(';
         bdy := bdy || nlttttt || '1 => sys.aq$_agent(''CONSUMER'', null, 0)))';
         bdy := bdy || nlttt || ',payload            => ' || type_cloud_event || '(' ||
                'utl_raw.cast_from_number(l_ce_id)' || ', sys_extract_utc(systimestamp)' || ', p_type' || ', p_source' ||
                ', p_data.to_string' || ')';
         bdy := bdy || nlttt || ',msgid              => l_message_id);';
      end if;
   
      bdy := bdy || nlt || 'end;';
      return bdy;
   end;

   function tapi_diff_recs(p_only_header in boolean) return str is
      doc       constant str := tab || '/**' || nlt ||
                                ' * Returns a JSON String containing all columns with different values.' || nlt ||
                                ' * For every contained column, the char representation of the values of both records are included.' || nlt ||
                                ' */';
      prc_name  constant str := 'function ' || params.proc_diff;
      param_old constant str := params.parameter_prefix || 'old';
      param_new constant str := params.parameter_prefix || 'new';
      sig       constant str := prc_name || '(' || param_old || ' in ' || type_rt_name || nlt || indent(prc_name) || ',' ||
                                param_new || ' in ' || type_rt_name || ') return ' || c_type_json_obj;
      ret_val   constant str := 'diff';
      bdy str;
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nltt || ret_val || ' ' || c_type_json_obj || ' := ' || c_type_json_obj || '();';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'if (' || str_join(stringf(g_pk_cols, col_rec(param_old) || ' is null'), ' or ') ||
             ') and (' || str_join(stringf(g_pk_cols, col_rec(param_new) || ' is null'), ' and ') || ')' || nltt ||
             'then' || nlttt || 'return ' || c_type_json_obj || '()' || ';' || nltt || 'end if;';
      bdy := bdy || nltt || str_join(stringf(g_pk_cols,
                                             ret_val || '.put(''' || tag_json_key || ''', nvl(' || col_rec(param_old) || ', ' ||
                                             col_rec(param_new) || '));'),
                                     nltt);
      bdy := bdy || nltt || 'if ' || str_join(stringf(g_pk_cols, col_rec(param_old) || ' is null'), ' or ') || ' then';
      bdy := bdy || nlttt || ret_val || '.put(''mode'', ''insert'');';
      bdy := bdy || nlttt || ret_val || '.put(''new'', json_obj(' || param_new || '));';
      bdy := bdy || nlttt || 'return ' || ret_val || ';';
      bdy := bdy || nltt || 'elsif ' || str_join(stringf(g_pk_cols, col_rec(param_new) || ' is null'), ' or ') ||
             ' then';
      bdy := bdy || nlttt || ret_val || '.put(''mode'', ''delete'');';
      bdy := bdy || nlttt || ret_val || '.put(''old'', json_obj(' || param_old || '));';
      bdy := bdy || nlttt || 'return ' || ret_val || ';';
      bdy := bdy || nltt || 'else';
      bdy := bdy || nlttt || 'declare';
      bdy := bdy || nltttt || 'jo_old   ' || c_type_json_obj || ' := json_obj(' || param_old || ');';
      bdy := bdy || nltttt || 'jo_new   ' || c_type_json_obj || ':= json_obj(' || param_new || ');';
      bdy := bdy || nltttt || 'l_keys   json_key_list := jo_new.get_keys;';
      bdy := bdy || nlttt || 'begin';
      bdy := bdy || nltttt || ret_val || '.put(''mode'', ''update'');';
      bdy := bdy || nltttt || ret_val || '.put(''new'', ' || 'jo_new' || ');';
      bdy := bdy || nltttt || ret_val || '.put(''old'', ' || 'jo_old' || ');';
      bdy := bdy || nltttt || 'l_keys := jo_new.get_keys;';
      bdy := bdy || nltttt || 'for idx in 1..l_keys.count loop';
      bdy := bdy || nlttttt || 'if (jo_old.get_string(l_keys(idx)) is null and jo_new.get_string(l_keys(idx)) is null)';
      bdy := bdy || nlttttt || tab || ' or jo_old.get_string(l_keys(idx)) = jo_new.get_string(l_keys(idx)) then';
      bdy := bdy || nlttttt || tab || 'jo_old.remove(l_keys(idx));';
      bdy := bdy || nlttttt || tab || 'jo_new.remove(l_keys(idx));';
      bdy := bdy || nlttttt || 'end if;';
      bdy := bdy || nltttt || 'end loop;';
      bdy := bdy || nltttt || 'if jo_new.get_size = 0 then';
      bdy := bdy || nlttttt || 'return json_object_t();';
      bdy := bdy || nltttt || 'else';
      bdy := bdy || nlttttt || 'diff.put(''mode'', ''update'');';
      bdy := bdy || nlttttt || 'diff.put(''old'', jo_old);';
      bdy := bdy || nlttttt || 'diff.put(''new'', jo_new);';
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
      sig      constant str := prc_name || '(' || param || ' in ' || c_type_json_obj || ', p_forward in boolean)';
      bdy clob;
   begin
      if p_only_header then
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
      bdy := bdy || nltttt || 'insert' || ' into ' || table_name() || ' t';
      bdy := bdy || nlttttt || '(' || str_join(stringf(changables, col_quote('t.')), nlttttt || ',') || ')';
      bdy := bdy || nltttt || 'values';
      bdy := bdy || nlttttt || '(' || str_join(stringf(changables, col_rec(l_rec)), nlttttt || ',') || ');';
      bdy := bdy || nlttt || 'when ' || '(p_forward and ' || 'l_mode = ''delete'')' || ' or (not p_forward and ' ||
             'l_mode = ''insert'')' || ' then';
      bdy := bdy || nltttt || 'delete ' || table_name() || ' t';
      bdy := bdy || nltttt || ' where ' ||
             str_join(stringf(g_pk_cols, col_quote('t.') || ' = ' || 'l_' || tag_col), nltttt || '   and ') || ';';
      bdy := bdy || nlttt || 'when ' || 'p_forward and ' || 'l_mode = ''update''' || ' then';
      bdy := bdy || nltttt || 'declare';
      bdy := bdy || nlttttt || str_join(stringf(non_pk(changables),
                                                'l_upd_' || tag_col_pad || ' varchar2(1) := ' || 'case when l_jo' ||
                                                '.has(''' || tag_col || ''') then ''1'' end;'),
                                        nlttttt);
      bdy := bdy || nltttt || 'begin';
      bdy := bdy || nlttttt || l_rec || ' := ' || params.proc_of_json || '(l_jo);';
      bdy := bdy || nlttttt || 'update ' || table_name() || ' t';
      bdy := bdy || nlttttt || '   set ' || str_join(stringf(non_pk(changables),
                                                             col_quote('t.') || ' = decode(' || 'l_upd_' || tag_col ||
                                                             ', 1, ' || col_rec(l_rec) || ', ' || col_quote('t.') || ')'),
                                                     nlttttt || '      ,');
      bdy := bdy || nlttttt || ' where ' ||
             str_join(stringf(g_pk_cols, col_quote('t.') || ' = ' || 'l_' || tag_col), nlttttt || '   and ') || ';';
      bdy := bdy || nltttt || 'end;';
      bdy := bdy || nltt || 'end case;';
      return bdy || nlt || 'end;';
   end;

   function tapi_redo(p_only_header in boolean) return str is
      doc      constant str := tab || '/**' || nlt ||
                               ' * Returns a JSON String containing all columns with different values.' || nlt ||
                               ' * For every contained column, the char representation of the values of both records are included.' || nlt ||
                               ' */';
      prc_name constant str := 'procedure ' || 'redo';
      param    constant str := params.parameter_prefix || 'diff';
      sig      constant str := prc_name || '(' || param || ' in ' || c_type_json_obj || ')';
      bdy str;
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'xxdo(' || param || ', true);';
      return bdy || nlt || 'end;';
   end;

   function tapi_undo(p_only_header in boolean) return str is
      doc str := tab || '/**' || nlt || ' * Returns a JSON String containing all columns with different values.' || nlt ||
                 ' * For every contained column, the char representation of the values of both records are included.' || nlt ||
                 ' */';
      prc_name constant str := 'procedure ' || 'undo';
      param    constant str := params.parameter_prefix || 'diff';
      sig      constant str := prc_name || '(' || param || ' in ' || c_type_json_obj || ')';
      bdy str;
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'xxdo(' || param || ', false);';
      return bdy || nlt || 'end;';
   end;

   function tapi_check_key_uk
   (
      p_cons        in tab_cols_t,
      p_only_header in boolean,
      p_suffix      in varchar2 default null
   ) return str is
      prc_name constant str := 'procedure check' || coalesce(p_suffix, cursor_suffix_pk);
      sig      constant str := tab || lower(prc_name) || '(' ||
                               str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type),
                                        nlt || indent(prc_name) || ',') || ')';
      bdy str;
   begin
      if p_only_header then
         return null;
      end if;
      bdy := nll || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || str_join(stringf(p_cons,
                                     nltt || proc_name_raise_if_null || '(' || tag_param || ', ''' || tag_col || ''');'),
                             '');
      return bdy || nlt || 'end;';
   end;

   function tapi_counts_uk
   (
      p_cons        in tab_cols_t,
      p_only_header in boolean,
      p_suffix      in varchar2 default null
   ) return str is
      doc      constant str := tab || '/**' || nlt || ' * Counts the rows.' || nlt || ' */';
      prc_name constant str := 'function ' || params.proc_count || p_suffix;
      sig constant str := lower(prc_name) || '(' ||
                          str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type || ' default null'),
                                   nlt || indent(prc_name) || ',') || ') return number' || case
                             when params.use_result_cache and not g_includes_lobs then
                              ' result_cache'
                          end;
      l_var    constant str := 'l_row_count';
      bdy str;
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
      bdy := bdy ||
             str_join(stringf(p_cons, col_quote('t.') || ' = ' || 'nvl(' || tag_param || ', ' || col_quote('t.') || ')'),
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
   ) return str is
      doc      constant str := tab || '/**' || doc_sel || doc_sel_lock(p_for_update) || doc_sel_result_cache ||
                               doc_sel_null_check || doc_sel_no_data_found || nlt || ' */';
      prc_name constant str := 'function ' || case
                                  when p_for_update then
                                   params.proc_lock_record
                                  else
                                   params.proc_select
                               end || p_suffix;
      sig constant str := lower(prc_name) || '(' ||
                          str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type), nlt || indent(prc_name) || ',') ||
                          ') return ' || type_rt_name || case
                             when params.use_result_cache and not g_includes_lobs and not p_for_update then
                              ' result_cache'
                          end;
      l_row    constant str := 'l_' || param_name_row;
      bdy str;
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nltt || l_row || ' ' || type_rt_name || ';';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'if ' || params.proc_exists_and_select || p_suffix || case
                when p_for_update then
                 '_lock'
             end || '(' || str_join(stringf(p_cons, tag_param), ', ') || ', ' || l_row || ')';
      bdy := bdy || nltt || 'then';
      bdy := bdy || nlttt || 'return ' || l_row || ';';
      bdy := bdy || nltt || 'else';
      bdy := bdy || nlttt || case
                when params.select_return_null_when_no_data_found then
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
   ) return str is
      doc       constant str := tab || '/**' || doc_sel || nlt ||
                                ' * Also captures the checksum of the record for later to perform an optimistic write lock check prior modifying the record in DB.' ||
                                doc_sel_result_cache || doc_sel_null_check || doc_sel_no_data_found || nlt || ' */';
      prc_name  constant str := 'function ' || params.proc_select || p_suffix || occ_name_suffix;
      sig       constant str := lower(prc_name) || '(' ||
                                str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type),
                                         nlt || indent(prc_name) || ',') || ') return ' || type_rt_name_occ;
      l_var_occ constant str := 'l_' || param_name_row || occ_name_suffix;
      cur_name  constant str := cursor_prefix || coalesce(p_suffix, cursor_suffix_pk) || occ_name_suffix;
      bdy str;
   begin
      if not priv_to_dbms_crypto or not params.create_occ_methods then
         return null;
      elsif p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nltt || l_var_occ || ' ' || type_rt_name_occ || ';';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || case
                when params.check_pk_values_before_select then
                 nltt ||
                 str_join(stringf(p_cons, proc_name_raise_if_null || '(' || tag_param || ', ''' || tag_col || ''');'), nltt) || nl
             end;
      bdy := bdy || nltt || 'open ' || cur_name || '(' || str_join(stringf(p_cons, tag_param), ', ') || ');';
      bdy := bdy || nltt || 'fetch ' || cur_name;
      bdy := bdy || nlttt || 'into ' || l_var_occ || ';';
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
   ) return str is
      doc constant str := tab || '/**' || nlt || ' * Returns true, if the row exists, false otherwise.' || nlt ||
                          ' * If the record exists, the function will return the record as an output parameter' || case
                             when p_for_update then
                              ' and aquires a write lock on this record'
                          end || doc_sel_result_cache || nlt || ' * Checks for null primary key columns ' || nlt || ' */';
      prc_name constant str := 'function ' || params.proc_exists_and_select || p_suffix || case
                                  when p_for_update then
                                   '_lock'
                               end;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := lower(prc_name) || '(' ||
                               str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type),
                                        nlt || indent(prc_name) || ',') || nlt || indent(prc_name) || ',' || param ||
                               ' out nocopy ' || type_rt_name || ') return boolean';
      l_exists constant str := 'l_found';
      cur_idx constant str := cursor_prefix || coalesce(p_suffix, cursor_suffix_pk) || case
                                 when p_for_update then
                                  '_lock'
                              end;
      bdy str;
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
                 str_join(stringf(p_cons, proc_name_raise_if_null || '(' || tag_param || ', ''' || tag_col || ''');'), nltt) || nl
             end;
      bdy := bdy || nltt || 'open ' || cur_idx || '(' || str_join(stringf(p_cons, tag_param), ', ') || ');';
      bdy := bdy || nltt || 'fetch ' || cur_idx;
      bdy := bdy || nlttt || 'into ' || param || ';';
      bdy := bdy || nltt || l_exists || ' := ' || cur_idx || '%found;';
      bdy := bdy || nltt || 'close ' || cur_idx || ';';
      bdy := bdy || nltt || 'return ' || l_exists || ';';
      return bdy || nlt || 'end;';
   end;

   function tapi_exists_rt(p_only_header in boolean) return str is
      doc      constant str := tab || '/**' || nlt || ' * Returns true, if the record exists, false otherwise.' || nlt ||
                               ' */';
      prc_name constant str := 'function ' || params.proc_exists;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := prc_name || '(' || param || ' in ' || type_rt_name || ') return boolean';
      bdy str;
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

   function tapi_log_exception_handler
   (
      p_param_name in varchar2,
      p_without_pk in boolean default false
   ) return str is
      bdy         str;
      exc_handler str;
   begin
      if (params.logging_exception_procedure is null or g_cons.count = 0) then
         return null;
      end if;
   
      bdy := bdy || nlt || 'exception' || nltt || 'when dup_val_on_index then';
      for idx in 1 .. g_cons.count loop
         if (p_without_pk and g_cons(idx).c_type = 'P') or g_cons(idx).c_type = 'N' then
            continue;
         end if;
      
         declare
            l_cons_cols tab_cols_t := read_cons_cols(g_cons(idx).c_name);
         begin
            exc_handler := exc_handler || nltttt || 'when instr(lower(sqlerrm), ''' || lower(g_cons(idx).c_name) ||
                           ''') > 0 then';
            exc_handler := exc_handler || nlttttt ||
                           log_exception('sqlerrm || '': ' || str_join(stringf(l_cons_cols,
                                                                               '"' || tag_col || '" = "'' || ' ||
                                                                               col_rec(p_param_name) || ' || ''"'''),
                                                                       ' || '', '));
            exc_handler := exc_handler || nlttttt ||
                           log_exception(params.proc_diff || '(' || params.proc_select || g_cons(idx).c_suffix || '(' ||
                                         str_join(stringf(l_cons_cols, col_rec(p_param_name)), ', ') || '), ' ||
                                         p_param_name || ')' || '.to_string');
         end;
      end loop;
   
      bdy := bdy || concat_if_not_null(nlttt || 'case', exc_handler, nlttt || 'end case;');
      bdy := bdy || nlttt || 'raise;';
      bdy := bdy || nltt || 'when not_null_constraint then';
      bdy := bdy || nlttt || log_exception('sqlerrm');
      return bdy || nlttt || 'raise;';
   end;

   function exceptions_forall
   (
      rows_var   in str,
      errors_var in str,
      ret_rows   in str
   ) return str is
      bdy str;
   begin
      bdy := bdy || nlt || 'exception';
      bdy := bdy || nltt || 'when forall_error';
      bdy := bdy || nltt || 'then';
      bdy := bdy || nlttt || 'for idx in 1 .. sql%bulk_exceptions.count';
      bdy := bdy || nlttt || 'loop';
      bdy := bdy || nltttt || errors_var || '.extend;';
      bdy := bdy || nltttt || errors_var || '(' || errors_var || '.last' || ') := ' || rows_var ||
             '(sql%bulk_exceptions(idx).error_index);';
      bdy := bdy || case
                when params.logging_exception_procedure is not null then
                 nltttt || log_exception('sqlerrm(-sql%bulk_exceptions(idx).error_code)')
             end;
      bdy := bdy || nlttt || 'end loop;';
      return bdy || nlttt || 'return ' || ret_rows || ';';
   end;

   function tapi_update_rt(p_only_header in boolean) return str is
      doc      constant str := tab || '/**' || nlt || ' * Updates the row with the modified values.' || nlt || ' */';
      prc_name constant str := 'procedure ' || params.proc_update;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := prc_name || '(' || param || ' in out nocopy ' || type_rt_name || ')';
      bdy str;
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt ||
             str_join(stringf(g_pk_cols,
                              proc_name_raise_if_null || '(' || col_rec(param) || ', ''' || tag_col || ''');'),
                      nltt) || nl;
      bdy := bdy || nltt || 'update ' || table_name() || ' t';
      bdy := bdy || nltt || '   set ' || str_join(stringf(exclude(changables, g_audit_cols_created),
                                                          col_quote('t.') || ' = ' || col_default(col_rec(param))),
                                                  nltt || '      ,');
      bdy := bdy || nltt || ' where ' ||
             str_join(str_join(stringf(g_pk_cols, col_quote('t.') || ' = ' || col_rec(param)), nltt || '   and '),
                      nltt || '   and    (',
                      str_join(stringf(non_pk(comparables), not_equal_exp(col_quote('t.'), col_rec(param))),
                               nltt || '        or '));
      bdy := bdy || concat_if_not_null(nltt || '        or ',
                                       str_join(stringf(include(g_cols, lob_types),
                                                        not_equal_exp(col_quote('t.'), col_rec(param), c_type_blob)),
                                                nltt || '        or '));
      bdy := bdy || ')';
      bdy := bdy || nltt || 'returning ' || str_join(stringf(g_cols, col_quote('t.')), ', ') || ' into ' || param || ';';
      if with_cloud_events then
         bdy := bdy || nltt || proc_name_emit_cloud_event || '(''update'', $$plsql_unit, ' || params.proc_json_obj || '(' ||
                param || '));';
      end if;
   
      bdy := bdy || tapi_log_exception_handler(param, true);
      return bdy || nlt || 'end;';
   end;

   function tapi_update_occ(p_only_header in boolean) return str is
      doc      constant str := tab || '/**' || nlt || ' * Updates the row with the modified values.' || nlt || ' */';
      prc_name constant str := 'procedure ' || params.proc_update || occ_name_suffix;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := prc_name || '(' || param || ' in out nocopy ' || type_rt_name_occ || ')';
      l_cur    constant str := cursor_prefix || '_' || 'pk' || occ_name_suffix;
      l_var    constant str := 'l_' || param_name_row;
      bdy str;
   begin
      if not priv_to_dbms_crypto or not params.create_occ_methods then
         return null;
      elsif p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nltt || l_var || ' ' || type_rt_name_occ || ';';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'open ' || l_cur || '(' || str_join(stringf(g_pk_cols, col_rec(param)), ', ') || ');';
      bdy := bdy || nltt || 'fetch ' || l_cur;
      bdy := bdy || nlttt || 'into ' || l_var || ';';
      bdy := bdy || nl;
      bdy := bdy || nltt || 'if ' || l_cur || '%notfound';
      bdy := bdy || nltt || 'then';
      bdy := bdy || nlttt || 'close ' || l_cur || ';';
      bdy := bdy || nlttt ||
             'raise_application_error(-20000, ''Update operation failed because the row is no longer in the database.'');';
      bdy := bdy || nltt || 'elsif ' || param || '.' || params.proc_checksum || ' != ' || l_var || '.' ||
             params.proc_checksum;
      bdy := bdy || nltt || 'then';
      bdy := bdy || nlttt || 'close ' || l_cur || ';';
      bdy := bdy || nlttt ||
             'raise_application_error(-20000, ''Current version of data in database has changed since last page refresh.'');';
      bdy := bdy || nltt || 'end if;' || nl;
      bdy := bdy || nltt || 'update ' || table_name() || ' t';
      bdy := bdy || nltt || '   set ' || str_join(stringf(exclude(non_pk(changables), g_audit_cols_created),
                                                          col_quote('t.') || ' = ' || col_rec(param)),
                                                  nltt || '      ,');
      bdy := bdy || nltt || ' where current of ' || l_cur || ';';
      --TODO
      /* bdy := bdy || nltt || 'returning ' || str_join(stringf(g_cols, col_quote('t.')), ', ')
      || ', ' || tapi_name || '.' || params.proc_checksum || '(' ||
      str_join(stringf(non_audit, col_quote('t.'))
              ,nltt || '       ' || indent(tapi_name || '.' || params.proc_checksum) || ',') || ')'
      || ' into ' || param || ';';*/
      bdy := bdy || nltt || 'close ' || l_cur || ';';
      return bdy || nlt || 'end;';
   end;

   function tapi_insert_rt_func(p_only_header in boolean) return str is
      doc      constant str := tab || '/**' || nlt || ' * Inserts a row into the table.' || nlt || ' */';
      prc_name constant str := 'function ' || params.proc_insert;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := prc_name || '(' || param || ' in ' || type_rt_name || ') return ' || type_rt_name;
      l_var    constant str := 'ret_val';
      bdy        str;
      pk_non_def tab_cols_t := tab_cols_t();
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nltt || l_var || ' ' || type_rt_name || ' := ' || param || ';';
      bdy := bdy || nlt || 'begin';
      for idx in 1 .. g_pk_cols.count loop
         if g_pk_cols(idx).data_default is null then
            pk_non_def.extend();
            pk_non_def(pk_non_def.last) := g_pk_cols(idx);
         end if;
      end loop;
   
      bdy := bdy || concat_if_not_null(nltt,
                                       str_join(stringf(pk_non_def,
                                                        proc_name_raise_if_null || '(' || col_rec(param) || ', ''' ||
                                                        tag_col || ''');'),
                                                nltt),
                                       nl);
      bdy := bdy || nltt || 'insert into ' || table_name() || ' t';
      bdy := bdy || nlttt || '(' || str_join(stringf(changables, col_quote('t.')), nlttt || ',') || ')';
      bdy := bdy || nltt || 'values';
      bdy := bdy || nlttt || '(' || str_join(stringf(changables, col_default(col_rec(param))), nlttt || ',') || ')';
      bdy := bdy || nltt || 'returning ' || str_join(stringf(g_cols, col_quote('t.')), ', ') || ' into ' || l_var || ';';
      if with_cloud_events then
         bdy := bdy || nltt || proc_name_emit_cloud_event || '(''insert'', $$plsql_unit, ' || params.proc_json_obj || '(' ||
                l_var || '));';
      end if;
   
      bdy := bdy || nltt || 'return ' || l_var || ';';
      bdy := bdy || tapi_log_exception_handler(param);
      return bdy || nlt || 'end;';
   end;

   function tapi_insert_rt_proc(p_only_header in boolean) return str is
      doc      constant str := tab || '/**' || nlt || ' * Inserts a row into the table.' || nlt || ' */';
      prc_name constant str := 'procedure ' || params.proc_insert;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := prc_name || '(' || param || ' in out nocopy ' || type_rt_name || ')';
      bdy str;
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || param || ' := ' || params.proc_insert || '(' || param || ');';
      return bdy || nlt || 'end;';
   end;

   function tapi_subtype_defaults(p_only_header in boolean) return str is
      doc      constant str := tab || '/**' || nlt || ' * Returns a record with defaults as defined.' || nlt || ' */';
      prc_name constant str := 'function ' || type_rt_name || '_defaults';
      l_var    constant str := 'l_' || param_name_row;
      sig str;
      bdy str;
   begin
      if params.custom_default_expressions.count = 0 then
         return null;
      end if;
   
      sig := prc_name || '(' ||
             str_join(stringf(non_audit(changables), tag_col_pad || ' ' || tag_type || ' default ' || tag_cust_default),
                      nlt || indent(prc_name) || ',') || ') return ' || type_rt_name;
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nltt || l_var || ' ' || type_rt_name || ';';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || str_join(stringf(non_audit(changables), col_rec(l_var) || ' := ' || tag_col || ';'), nltt);
      bdy := bdy || nltt || 'return ' || l_var || ';';
      return bdy || nlt || 'end;';
   end;

   function tapi_exists_select_rt(p_only_header in boolean) return str is
      doc      constant str := tab || '/**' || nlt || ' * Returns true, if the row exists, false otherwise.' || nlt ||
                               ' */';
      prc_name constant str := 'function ' || params.proc_exists_and_select;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := prc_name || '(' || param || ' in out nocopy ' || type_rt_name || ') return boolean';
      bdy str;
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nltt ||
             str_join(stringf(g_pk_cols, 'l_' || tag_col || ' constant ' || tag_type || ' := ' || col_rec(param) || ';'),
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
   ) return str is
      doc      constant str := tab || '/**' || doc_sel || doc_sel_lock(p_for_update) || doc_sel_result_cache ||
                               doc_sel_null_check || doc_sel_no_data_found || nlt || ' */';
      prc_name constant str := 'function ' || case
                                  when p_for_update then
                                   params.proc_lock_record
                                  else
                                   params.proc_select
                               end;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := prc_name || '(' || param || ' in ' || type_rt_name || ') return ' || type_rt_name;
      bdy str;
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'return ' || case
                when p_for_update then
                 params.proc_lock_record
                else
                 params.proc_select
             end || '(' || str_join(stringf(g_pk_cols, col_rec(param)), ', ') || ');';
      return bdy || nlt || 'end;';
   end;

   function tapi_select_rt_proc
   (
      p_only_header in boolean,
      p_for_update  in boolean default false
   ) return str is
      doc      constant str := tab || '/**' || doc_sel || doc_sel_lock(p_for_update) || doc_sel_result_cache ||
                               doc_sel_null_check || doc_sel_no_data_found || nlt || ' */';
      prc_name constant str := 'procedure ' || case
                                  when p_for_update then
                                   params.proc_lock_record
                                  else
                                   params.proc_select
                               end;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := prc_name || '(' || param || ' in out nocopy ' || type_rt_name || ')';
      bdy str;
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || param || ' := ' || case
                when p_for_update then
                 params.proc_lock_record
                else
                 params.proc_select
             end || '(' || str_join(stringf(g_pk_cols, col_rec(param)), ', ') || ');';
      return bdy || nlt || 'end;';
   end;

   function tapi_print_rt(p_only_header in boolean default false) return str is
      doc      constant str := tab || '/**' || nlt || ' * Prints out all fieldnames and values of the record.' || nlt ||
                               ' */';
      prc_name constant str := 'procedure ' || params.proc_print;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := prc_name || '(' || param || ' in ' || type_rt_name || ')';
      bdy str;
   begin
      if params.logging_exception_procedure is null then
         return null;
      elsif p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt ||
             str_join(stringf(g_cols, log_exception('''' || tag_col || ' = '' || ' || col_char(col_rec(param)))), nltt);
      return bdy || nlt || 'end;';
   end;

   function tapi_to_string_rt(p_only_header in boolean default false) return str is
      doc      constant str := tab || '/**' || nlt ||
                               ' * Returns a string representation of the concatenated primary key values.' || nlt ||
                               ' */';
      prc_name constant str := 'function ' || proc_name_pk_string;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := prc_name || '(' || param || ' in ' || type_rt_name || ') return varchar2';
      bdy str;
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'return ' ||
             str_join(stringf(g_pk_cols, col_char(col_rec(param))), ' || ''' || params.proc_pk_string_delim || ''' || ') || ';';
      return bdy || nlt || 'end;';
   end;

   function tapi_json_obj(p_only_header in boolean default false) return str is
      doc      constant str := tab || '/**' || nlt || ' * Returns a stringified JSON of the record.' || nlt || ' */';
      prc_name constant str := 'function ' || params.proc_json_obj;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := prc_name || '(' || param || ' in ' || type_rt_name || ') return ' || c_type_json_obj;
      bdy str;
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nltt || 'jo ' || c_type_json_obj || ' := ' || c_type_json_obj || ';';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || str_join(stringf(include(g_cols, supported_types),
                                             'jo.put(''' || tag_json_key || ''', ' || col_json_val(col_rec(param)) || ');'),
                                     nltt);
      bdy := bdy || nltt || 'return jo;';
      return bdy || nlt || 'end;';
   end;

   function tapi_json_import(p_only_header in boolean default false) return clob is
      doc      constant str := tab || '/**' || nlt || ' * Returns a record from the given JSON object.' || nlt ||
                               ' * Complex data like timestamps and binary data is deserialized to the native Oracle types.' || nlt ||
                               ' */';
      prc_name constant str := 'function ' || params.proc_of_json;
      param    constant str := params.parameter_prefix || 'json';
      sig      constant str := prc_name || '(' || param || ' in ' || c_type_json_obj || ') return ' || type_rt_name;
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
      bdy := bdy || nltt || str_join(stringf(include(g_cols, supported_types),
                                             col_rec(param_name_row) || ' := case' || nlttt || 'when (' || param ||
                                             '.has(''' || tag_json_key || ''') and not ' || param || '.get(''' ||
                                             tag_json_key || ''').is_null) then' || nltttt || tag_json_from_val_1 ||
                                             param || tag_json_from_val_2 || nlttt || 'end;'),
                                     nltt);
      bdy := bdy || nltt || 'return ' || param_name_row || ';';
      return bdy || nlt || 'end;';
   end;

   function tapi_json_arr(p_only_header in boolean default false) return str is
      doc      constant str := tab || '/**' || nlt || ' * Returns a stringified JSON of the record.' || nlt || ' */';
      prc_name constant str := 'function ' || 'json' || '_arr';
      param    constant str := params.parameter_prefix || 'rows';
      sig      constant str := prc_name || '(' || param || ' in ' || type_rows_tab || ') return json_array_t';
      bdy str;
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nltt || 'idx pls_integer := ' || param || '.first;';
      bdy := bdy || nltt || 'ja  json_array_t := json_array_t;';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'while idx is not null';
      bdy := bdy || nltt || 'loop';
      bdy := bdy || nlttt || 'ja.append(' || params.proc_json_obj || '(' || param || '(idx)));';
      bdy := bdy || nlttt || 'idx := ' || param || '.next(idx);';
      bdy := bdy || nltt || 'end loop;';
      bdy := bdy || nltt || 'return ja;';
      return bdy || nlt || 'end;';
   end;

   function tapi_merge_rt(p_only_header in boolean default false) return clob is
      doc      constant str := tab || '/**' || nlt ||
                               ' * Insert or updates the record on whether the record already exists.' || nlt || ' */';
      prc_name constant str := 'procedure ' || params.proc_merge;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := prc_name || '(' || param || ' in ' || type_rt_name || ')';
      bdy clob;
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'merge into ' || table_name() || ' x';
      bdy := bdy || nltt || 'using (select ';
      bdy := bdy || str_join(stringf(g_cols, col_rec(param) || ' as ' || tag_col), nltt || '             ,');
      bdy := bdy || nltt || '         from dual) y';
      bdy := bdy || nltt || 'on (' ||
             str_join(stringf(g_pk_cols, col_quote('x.') || ' = ' || col_quote('y.')), ' and ') || ')';
      bdy := bdy || nltt || 'when matched then';
      bdy := bdy || nlttt || 'update';
      bdy := bdy || nlttt || '   set ';
      bdy := bdy || str_join(stringf(exclude(non_pk(changables), g_audit_cols_created),
                                     col_quote('x.') || ' = ' || col_default(col_quote('y.'))),
                             nltttt || '   ,');
      bdy := bdy ||
             concat_if_not_null(nlttt || ' where ',
                                str_join(stringf(non_pk(comparables), not_equal_exp(col_quote('x.'), col_quote('y.'))),
                                         nltttt || ' or '));
      bdy := bdy || concat_if_not_null(nlttt || '    or ',
                                       str_join(stringf(include(g_cols, lob_types),
                                                        not_equal_exp(col_quote('x.'), col_quote('y.'), c_type_blob)),
                                                nltttt || ' or '));
      bdy := bdy || nltt || 'when not matched then';
      bdy := bdy || nlttt || 'insert';
      bdy := bdy || nltttt || '(' || str_join(stringf(changables, col_quote('x.')), nltttt || ',') || ')';
      bdy := bdy || nlttt || 'values';
      bdy := bdy || nltttt || '(' || str_join(stringf(changables, col_default(col_quote('y.'))), nltttt || ',') || ');';
      return bdy || nlt || 'end;';
   end;

   function tapi_exist_uk
   (
      p_cons        in tab_cols_t,
      p_only_header in boolean default false,
      p_suffix      in varchar2 default null
   ) return str is
      doc      constant str := tab || '/**' || nlt || ' * Returns true, if the record exists, false otherwise.' || nlt ||
                               ' */';
      prc_name constant str := 'function ' || params.proc_exists || p_suffix;
      sig constant str := lower(prc_name) || '(' ||
                          str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type || ' default null'),
                                   nlt || indent(prc_name) || ',') || ') return boolean' || case
                             when params.use_result_cache and not g_includes_lobs then
                              ' result_cache'
                          end;
      l_var    constant str := 'l_' || param_name_row;
      l_exists constant str := 'l_found';
      cur_idx  constant str := cursor_prefix || coalesce(p_suffix, cursor_suffix_pk);
      bdy str;
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
      bdy := bdy || nlttt || 'into ' || l_var || ';';
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
   ) return str is
      doc constant str := tab || '/**' || nlt || ' * Returns true, if the record exists, false otherwise.' || nlt ||
                          ' */';
      prc_name str;
      sig      str;
      bdy      str;
   begin
      if not params.boolean_pseudo_type.exists('true') or not params.boolean_pseudo_type.exists('false') then
         return null;
      end if;
      prc_name := 'function ' || params.proc_exists || p_suffix || '_' || lower(params.boolean_pseudo_type('true')) ||
                  lower(params.boolean_pseudo_type('false'));
      sig      := lower(prc_name) || '(' ||
                  str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type || ' default null'),
                           nlt || indent(prc_name) || ',') || ') return varchar2';
   
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'return case when ' || params.proc_exists || p_suffix || '(' ||
             str_join(stringf(p_cons, tag_param), ', ') || ') then ''' || params.boolean_pseudo_type('true') ||
             ''' else ''' || params.boolean_pseudo_type('false') || ''' end;';
      return bdy || nlt || 'end;';
   end;

   function tapi_delete_rt(p_only_header in boolean) return str is
      doc      constant str := tab || '/**' || nlt || ' * Deletes a row with the same primary key from the table.' || nlt ||
                               ' */';
      prc_name constant str := 'procedure ' || params.proc_delete;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := prc_name || '(' || param || ' in ' || type_rt_name || ')';
      bdy str;
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
   ) return str is
      doc      constant str := tab || '/**' || nlt || ' * Deletes a row with the same primary key from the table.' || nlt ||
                               ' */';
      prc_name constant str := 'procedure del' || p_suffix;
      sig      constant str := lower(prc_name) || '(' ||
                               str_join(stringf(p_cons, tag_sig_param || ' in ' || tag_type),
                                        nlt || indent(prc_name) || ',') || ')';
      bdy str;
   begin
      if p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'check' || coalesce(p_suffix, cursor_suffix_pk) || '(' ||
             str_join(stringf(p_cons, tag_param), ', ') || ');' || nl;
      bdy := bdy || nltt || 'delete ' || table_name() || ' t';
      bdy := bdy || nltt || ' where ' ||
             str_join(stringf(p_cons, col_quote('t.') || ' = ' || tag_param), nltt || '   and ') || ';';
      if params.raise_error_on_failed_update_delete then
         bdy := bdy || nltt || 'if sql%notfound';
         bdy := bdy || nltt || 'then';
         bdy := bdy || nlttt || 'raise no_data_found;';
         bdy := bdy || nltt || 'end if;';
      end if;
   
      return bdy || nlt || 'end;';
   end;

   function tapi_select_rows(p_only_header in boolean default false) return str is
      doc      constant str := tab || '/**' || nlt || ' * Returns the records captured by the ref cursor.' || nlt ||
                               ' * If more than the limited number of records exist, the functions has to be called as long as the return table is empty.' || nlt ||
                               ' * When the ref cursor doesn''t return any more rows, it is automacally closed.' || nlt ||
                               ' */';
      prc_name constant str := 'function ' || params.proc_select || '_rows';
      p_cursor constant str := params.parameter_prefix || 'ref_cursor';
      p_limit  constant str := params.parameter_prefix || 'bulk_limit';
      sig      constant str := tab || prc_name || '(' || p_cursor || ' in ' || type_ref_cursor || nlt ||
                               indent(prc_name) || ',' || p_limit || ' in pls_integer default ' ||
                               params.default_bulk_limit || ') return ' || type_rows_tab;
      l_rows   constant str := 'l_rows';
      bdy str;
   begin
      if p_only_header then
         return nll || doc || nl || sig || ';';
      end if;
   
      bdy := nll || sig || ' is';
      bdy := bdy || nltt || l_rows || ' ' || type_rows_tab || ';';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'if (' || p_cursor || '%isopen)' || nltt || 'then';
      bdy := bdy || nlttt || 'fetch ' || p_cursor || ' bulk collect';
      bdy := bdy || nltttt || 'into ' || l_rows || ' limit ' || p_limit || ';';
      bdy := bdy || nlttt || 'if (' || l_rows || '.count < ' || p_limit || ')';
      bdy := bdy || nlttt || 'then';
      bdy := bdy || nltttt || 'close ' || p_cursor || ';';
      bdy := bdy || nlttt || 'end if;';
      bdy := bdy || nlttt || 'return ' || l_rows || ';';
      bdy := bdy || nltt || 'else';
      bdy := bdy || nlttt || 'return ' || type_rows_tab || '();';
      bdy := bdy || nltt || 'end if;';
      return bdy || nlt || 'end;';
   end;

   function tapi_insert_rows(p_only_header in boolean) return str is
      prc_name constant str := 'procedure ' || params.proc_insert || '_rows';
      param    constant str := params.parameter_prefix || 'rows';
      sig      constant str := tab || prc_name || '(' || param || ' in out nocopy ' || type_rows_tab || ')';
      ret_tab  constant str := 'ret_tab';
      bdy str;
   begin
      if p_only_header then
         return nll || sig || ';';
      end if;
      bdy := nll || sig || ' is';
      bdy := bdy || nltt || ret_tab || ' ' || type_rows_tab || ';';
      bdy := bdy || nltt || 'errors' || ' ' || type_rows_tab || ';';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || ret_tab || ' := ' || params.proc_insert || '_rows(' || param || ', ' || 'errors' || ');';
      bdy := bdy || nltt || 'if errors.count > 0';
      bdy := bdy || nltt || 'then';
      bdy := bdy || nlttt || 'raise ' || ex_forall_error || ';';
      bdy := bdy || nltt || 'end if;';
      bdy := bdy || nltt || param || ' := ' || ret_tab || ';';
      return bdy || nlt || 'end;';
   end;

   function tapi_insert_rows_save_exc(p_only_header in boolean) return str is
      prc_name   constant str := 'function ' || params.proc_insert || '_rows';
      param      constant str := params.parameter_prefix || 'rows';
      errors_var constant str := params.parameter_prefix || 'errors';
      sig        constant str := tab || prc_name || '(' || param || ' in ' || type_rows_tab || nlt || indent(prc_name) || ',' ||
                                 errors_var || ' out nocopy ' || type_rows_tab || ') return ' || type_rows_tab;
      ret_rows   constant str := 'ret_tab';
      bdy str;
   begin
      if p_only_header then
         return nll || sig || ';';
      end if;
      bdy := nll || sig || ' is';
      bdy := bdy || nltt || ret_rows || ' ' || type_rows_tab || ' := ' || type_rows_tab || '();';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || errors_var || ' := ' || type_rows_tab || '();';
      bdy := bdy || nltt || 'forall i in indices of ' || param || ' save exceptions';
      bdy := bdy || nlttt || 'insert into ' || table_name() || ' t';
      bdy := bdy || nltttt || '(' || str_join(stringf(changables, col_quote('t.')), nltttt || ',') || ')';
      bdy := bdy || nlttt || 'values';
      bdy := bdy || nltttt || '(' || str_join(stringf(changables, col_default(col_rec(param || '(i)'))), nltttt || ',') || ')';
      bdy := bdy || nlttt || 'returning' || ' ' || str_join(stringf(g_cols, col_quote('t.')), ', ') ||
             ' bulk collect into ' || ret_rows || ';';
      bdy := bdy || nltt || 'return ' || ret_rows || ';';
      if with_cloud_events then
         bdy := bdy || nltt || 'for idx in 1 .. ' || ret_rows || '.count loop';
         bdy := bdy || nlttt || proc_name_emit_cloud_event || '(''insert'', $$plsql_unit, ' || 'json_obj' || '(' ||
                ret_rows || '(idx)));';
         bdy := bdy || nltt || 'end loop;';
         bdy := bdy || nltt || 'return ' || ret_rows || ';';
      end if;
   
      bdy := bdy || exceptions_forall(param, errors_var, ret_rows);
      return bdy || nlt || 'end;';
   end;

   function tapi_insert_cur(p_only_header in boolean default false) return str is
      prc_name constant str := 'procedure ' || params.proc_insert_cur;
      p_cursor constant str := params.parameter_prefix || 'ref_cursor';
      p_limit  constant str := params.parameter_prefix || 'bulk_limit';
      sig      constant str := tab || prc_name || '(' || p_cursor || ' in ' || type_ref_cursor || nlt ||
                               indent(prc_name) || ',' || p_limit || ' in pls_integer default ' ||
                               params.default_bulk_limit || ')';
      bdy str;
   begin
      if p_only_header then
         return nll || sig || ';';
      end if;
      bdy := nll || sig || ' is';
      bdy := bdy || nltt || 'l_tab ' || type_rows_tab || ';';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'loop';
      bdy := bdy || nlttt || 'l_tab := sel_rows(' || p_cursor || ', ' || p_limit || ');';
      bdy := bdy || nlttt || 'exit when l_tab.count = 0;';
      bdy := bdy || nl;
      bdy := bdy || nlttt || 'ins_rows(l_tab);';
      bdy := bdy || nltt || 'end loop;';
      return bdy || nlt || 'end;';
   end;

   function tapi_update_rows(p_only_header in boolean) return str is
      prc_name constant str := 'procedure ' || params.proc_update || '_rows';
      param    constant str := params.parameter_prefix || 'rows';
      sig      constant str := tab || prc_name || '(' || param || ' in out nocopy ' || type_rows_tab || ')';
      ret_tab  constant str := 'ret_tab';
      bdy str;
   begin
      if p_only_header then
         return nll || sig || ';';
      end if;
      bdy := nll || sig || ' is';
      bdy := bdy || nltt || ret_tab || ' ' || type_rows_tab || ';';
      bdy := bdy || nltt || 'errors' || ' ' || type_rows_tab || ';';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || ret_tab || ' := ' || params.proc_update || '_rows(' || param || ', ' || 'errors' || ');';
      bdy := bdy || nltt || 'if errors.count > 0';
      bdy := bdy || nltt || 'then';
      bdy := bdy || nlttt || 'raise ' || ex_forall_error || ';';
      bdy := bdy || nltt || 'end if;';
      bdy := bdy || nltt || param || ' := ' || ret_tab || ';';
      return bdy || nlt || 'end;';
   end;

   function tapi_update_rows_save_exc(p_only_header in boolean) return clob is
      prc_name   constant str := 'function ' || params.proc_update || '_rows';
      param      constant str := params.parameter_prefix || 'rows';
      errors_var constant str := params.parameter_prefix || 'errors';
      sig        constant str := tab || prc_name || '(' || param || ' in ' || type_rows_tab || nlt || indent(prc_name) || ',' ||
                                 errors_var || ' out nocopy ' || type_rows_tab || ') return ' || type_rows_tab;
      ret_rows   constant str := 'ret_tab';
      bdy clob;
   begin
      if p_only_header then
         return nll || sig || ';';
      end if;
      bdy := nll || sig || ' is';
      bdy := bdy || nltt || ret_rows || ' ' || type_rows_tab || ' := ' || type_rows_tab || '();';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || errors_var || ' := ' || type_rows_tab || '();';
      bdy := bdy || nltt || 'forall i in indices of ' || param || ' save exceptions';
      bdy := bdy || nlttt || 'update ' || table_name() || ' t';
      bdy := bdy || nlttt || '   set ' ||
             str_join(stringf(exclude(changables, g_audit_cols_created),
                              col_quote('t.') || ' = ' || col_default(col_rec(param || '(i)'))),
                      nltttt || '   ,');
      bdy := bdy || nlttt || ' where ';
      bdy := bdy ||
             str_join(stringf(g_pk_cols, col_quote('t.') || ' = ' || col_rec(param || '(i)')), nlttt || '   and ');
      bdy := bdy || concat_if_not_null(nlttt || '   and  (',
                                       str_join(stringf(non_pk(comparables),
                                                        not_equal_exp(col_quote('t.'), col_rec(param || '(i)'))),
                                                nlttt || '      or ') || ')');
      bdy := bdy || nlttt || 'returning' || ' ' || str_join(stringf(g_cols, col_quote), ', ') || ' bulk collect into ' ||
             ret_rows || ';';
      bdy := bdy || nltt || 'return ' || ret_rows || ';';
      bdy := bdy || exceptions_forall(param, errors_var, ret_rows);
      return bdy || nlt || 'end;';
   end;

   function tapi_pipe_rows(p_only_header in boolean default false) return str is
      prc_name constant str := 'function ' || params.proc_pipe || '_rows';
      param    constant str := params.parameter_prefix || 'ref_cursor';
      sig      constant str := tab || prc_name || '(' || param || ' in ' || type_ref_cursor || ') return ' ||
                               type_rows_tab || nltt || 'pipelined';
      l_var    constant str := 'l_' || param_name_row;
      bdy str;
   begin
      if params.proc_pipe is null then
         return null;
      elsif p_only_header then
         return nll || sig || ';';
      end if;
      bdy := nll || sig || ' is';
      bdy := bdy || nltt || l_var || ' ' || type_rt_name || ';';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'if not ' || param || '%isopen' || nltt || 'then';
      bdy := bdy || nlttt || 'return;';
      bdy := bdy || nltt || 'end if;';
      bdy := bdy || nltt || 'loop';
      bdy := bdy || nlttt || 'fetch ' || param;
      bdy := bdy || nltttt || 'into ' || l_var || ';';
      bdy := bdy || nlttt || 'exit when ' || param || '%notfound;';
      bdy := bdy || nlttt || 'pipe row(' || l_var || ');';
      bdy := bdy || nltt || 'end loop;';
      bdy := bdy || nltt || 'close ' || param || ';';
      bdy := bdy || nltt || 'return;';
      return bdy || nlt || 'end;';
   end;

   function tapi_checksum_col(p_only_header in boolean) return str is
      doc       constant str := tab || '/**' || nlt || ' * Returns a SHA512 hash of the concatenated values.' || nlt ||
                                ' */';
      prc_name  constant str := 'function ' || params.proc_checksum;
      sig       constant str := prc_name || '(' || str_join(stringf(non_audit, tag_sig_param || ' in ' || tag_type),
                                                            nlt || indent(prc_name) || ',') ||
                                ') return varchar2 deterministic';
      proc_hash constant str := 'sys.dbms_crypto.hash';
      bdy str;
   begin
      if not priv_to_dbms_crypto then
         return null;
      elsif p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'return ' || proc_hash || '(';
      bdy := bdy || str_join(stringf(non_audit, col_char(tag_param)), nltt || '     ' || indent(proc_hash) || '|| ');
      bdy := bdy || nltt || '       ' || indent(proc_hash) || ',' || 'sys.dbms_crypto.hash_sh512);';
      return bdy || nlt || 'end;';
   end;

   function tapi_checksum_rt(p_only_header in boolean) return str is
      doc      constant str := tab || '/**' || nlt || ' * Returns an SHA512 hash of the record.' || nlt || ' */';
      prc_name constant str := 'function ' || params.proc_checksum;
      param    constant str := params.parameter_prefix || param_name_row;
      sig      constant str := prc_name || '(' || param || ' in ' || type_rt_name || ') return varchar2';
      bdy str;
   begin
      if not priv_to_dbms_crypto then
         return null;
      elsif p_only_header then
         return nll || doc || nlt || sig || ';';
      end if;
   
      bdy := nll || tab || sig || ' is';
      bdy := bdy || nlt || 'begin';
      bdy := bdy || nltt || 'return ' || params.proc_checksum || '(' ||
             str_join(stringf(non_audit, col_rec(param)), nltt || '       ' || indent(params.proc_checksum) || ',') || ');';
      return bdy || nlt || 'end;';
   end;

   function tapi_subtypes return str is
      spc str;
   begin
      spc := nll || tab ||
             str_join(stringf(g_cols,
                              'subtype ' || tag_col || type_name_suffix || ' is ' || tag_type || tag_not_null || ';'),
                      nlt);
      return spc || nlt || 'subtype ' || type_col_hash || ' is varchar2(64);';
   end;

   function tapi_record_rt return clob is
      decl str := 'type ' || type_rt_name || ' is record';
      spc  clob;
   begin
      spc := nll || tab || decl || '(' ||
             str_join(stringf(g_cols, tag_rt_def || ' ' || tag_type), nlt || indent(decl) || ',') || ');';
   
      if priv_to_dbms_crypto and params.create_occ_methods then
         decl := 'type ' || type_rt_name || occ_name_suffix || ' is record';
         spc  := spc || nll || tab || decl || '(' ||
                 str_join(stringf(g_cols, tag_rt_def || ' ' || tag_type), nlt || indent(decl) || ',');
         spc  := spc || nlt || indent(decl) || ',' || 'checksum' || '    ' || type_col_hash;
         spc  := spc || ');';
      end if;
   
      return spc;
   end;

   function gen_procedures(gen_header in boolean default false) return clob is
      aquire_lock constant boolean := true;
      bdy clob;
   begin
      bdy := bdy || tapi_subtype_defaults(gen_header);
      bdy := bdy || tapi_checksum_rt(gen_header);
      bdy := bdy || tapi_diff_recs(gen_header);
      bdy := bdy || tapi_xxdo(gen_header);
      bdy := bdy || tapi_redo(gen_header);
      bdy := bdy || tapi_undo(gen_header);
      for idx in 1 .. g_cons.count loop
         if g_cons(idx).c_type = 'N' then
            continue;
         end if;
         declare
            l_cons_cols tab_cols_t := read_cons_cols(g_cons(idx).c_name);
         begin
            bdy := bdy || tapi_check_key_uk(l_cons_cols, gen_header, g_cons(idx).c_suffix);
            bdy := bdy || tapi_exists_select_uk(l_cons_cols, gen_header, g_cons(idx).c_suffix);
            bdy := bdy || tapi_exists_select_uk(l_cons_cols, gen_header, g_cons(idx).c_suffix, aquire_lock);
            bdy := bdy || tapi_select_uk(l_cons_cols, gen_header, g_cons(idx).c_suffix);
            bdy := bdy || tapi_select_uk(l_cons_cols, gen_header, g_cons(idx).c_suffix, aquire_lock);
            bdy := bdy || tapi_select_occ_uk(l_cons_cols, gen_header, g_cons(idx).c_suffix);
            bdy := bdy || tapi_exist_uk(l_cons_cols, gen_header, g_cons(idx).c_suffix);
            bdy := bdy || tapi_exist_uk_yn(l_cons_cols, gen_header, g_cons(idx).c_suffix);
            bdy := bdy || tapi_counts_uk(l_cons_cols, gen_header, g_cons(idx).c_suffix);
            bdy := bdy || tapi_delete_uk(l_cons_cols, gen_header, g_cons(idx).c_suffix);
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
      bdy := bdy || tapi_merge_rt(gen_header);
      bdy := bdy || tapi_select_rows(gen_header);
      bdy := bdy || tapi_insert_rows(gen_header);
      bdy := bdy || tapi_insert_rows_save_exc(gen_header);
      bdy := bdy || tapi_insert_cur(gen_header);
      bdy := bdy || tapi_update_rows(gen_header);
      bdy := bdy || tapi_update_rows_save_exc(gen_header);
      bdy := bdy || tapi_pipe_rows(gen_header);
      bdy := bdy || tapi_print_rt(gen_header);
      bdy := bdy || tapi_to_string_rt(gen_header);
      bdy := bdy || tapi_json_obj(gen_header);
      bdy := bdy || tapi_json_arr(gen_header);
      bdy := bdy || tapi_json_import(gen_header);
      return bdy;
   end;

   function tapi_cursor_indices(p_only_header in boolean) return str is
      bdy str;
   
      function cursor_idx
      (
         p_cols_tab    in tab_cols_t,
         p_suffix      in varchar2,
         p_for_update  in boolean,
         p_only_header in boolean default false
      ) return str is
         cur_name constant str := 'cursor ' || cursor_prefix || coalesce(p_suffix, cursor_suffix_pk) || case
                                     when p_for_update then
                                      '_lock'
                                  end;
         sig      constant str := nll || tab || lower(cur_name) || '(' ||
                                  str_join(stringf(p_cols_tab, tag_sig_param || ' ' || tag_type || ' default null'),
                                           nlt || indent(cur_name) || ',') || ') return ' || type_rt_name;
         spc str;
      begin
         if (p_for_update and params.proc_lock_record is null) then
            return null;
         elsif p_only_header then
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
                    nltt || '   for update' || case params.for_update_timeout
                       when -1 then
                        null
                       when 0 then
                        ' nowait'
                       else
                        ' wait ' || params.for_update_timeout
                    end
                end;
         return spc || ';';
      end;
   
      function cursor_idx_occ
      (
         p_cols_tab    in tab_cols_t,
         p_suffix      in varchar2,
         p_only_header in boolean default false
      ) return str is
         cur_name constant str := 'cursor ' || cursor_prefix || coalesce(p_suffix, cursor_suffix_pk) || occ_name_suffix;
         sig      constant str := nll || tab || lower(cur_name) || '(' ||
                                  str_join(stringf(p_cols_tab, tag_sig_param || ' ' || tag_type || ' default null'),
                                           nlt || indent(cur_name) || ',') || ') return ' || type_rt_name_occ;
         spc str;
      begin
         if not priv_to_dbms_crypto or not params.create_occ_methods then
            return null;
         elsif p_only_header then
            return sig || ';';
         end if;
      
         spc := sig || ' is';
         spc := spc || nltt || 'select t.*';
         spc := spc || nltt || '      ,' || tapi_name || '.' || params.proc_checksum || '(' ||
                str_join(stringf(non_audit, col_quote('t.')),
                         nltt || '       ' || indent(tapi_name || '.' || params.proc_checksum) || ',') || ')';
         spc := spc || nltt || '  from ' || table_name() || ' t';
         spc := spc || nltt || ' where ' ||
                str_join(stringf(p_cols_tab,
                                 col_quote('t.') || ' = nvl(' || tag_param || ', ' || col_quote('t.') || ')'),
                         nltt || '   and ');
         return spc || nltt || '   for update nowait;';
      end;
   begin
      for idx in 1 .. g_cons.count loop
         declare
            l_cons_cols tab_cols_t := read_cons_cols(g_cons(idx).c_name);
         begin
            bdy := bdy || cursor_idx(l_cons_cols, g_cons(idx).c_suffix, false, p_only_header);
            bdy := bdy || cursor_idx(l_cons_cols, g_cons(idx).c_suffix, true, p_only_header);
            if g_cons(idx).c_type member of str_list('P', 'U') then
               bdy := bdy || cursor_idx_occ(l_cons_cols, g_cons(idx).c_suffix, p_only_header);
            end if;
         end;
      end loop;
   
      return bdy;
   end;

   function create_header return clob is
      doc constant str := '/**' || nl || ' * Generated package for table ' || table_name() || nl || ' */';
      spc clob;
   begin
      spc := doc || nl || 'create or replace package ' || owner_name || '.' || tapi_name || ' authid definer is';
      spc := spc || tapi_subtypes();
      spc := spc || tapi_record_rt();
      spc := spc || nll || tab || 'type ' || type_rows_tab || '          is table of ' || type_rt_name || ';';
      spc := spc || nll || tab || 'type ' || type_ref_cursor || ' is ref cursor return ' || type_rt_name || ';';
      spc := spc || tapi_checksum_col(true);
      spc := spc || tapi_cursor_indices(true);
      spc := spc || gen_procedures(true);
      return spc || nll || 'end;';
   end;

   function create_body return clob is
      bdy clob;
   begin
      bdy := 'create or replace package body ' || owner_name || '.' || tapi_name || ' is';
      bdy := bdy || nl;
      bdy := bdy || nlt || ex_not_null_constraint || ' exception;';
      bdy := bdy || nlt || 'pragma exception_init(' || ex_not_null_constraint || ', -1400);';
      bdy := bdy || nlt || ex_forall_error || ' exception;';
      bdy := bdy || nlt || 'pragma exception_init (' || ex_forall_error || ', -24381);';
      bdy := bdy || tapi_cursor_indices(false);
      bdy := bdy || tapi_raise_if_null;
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
      p_table_name     in varchar2,
      p_owner          in varchar2 default user,
      p_raise_on_error boolean default false
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
      read_constraints();
      if g_pk_cols.count = 0 then
         if p_raise_on_error then
            raise_application_error(-20000,
                                    'Table ' || g_owner || '.' || g_table_name ||
                                    ' has no primary key or usable substitutes, skipping generation access package.');
         else
            sys.dbms_output.put_line('Table ' || g_owner || '.' || g_table_name ||
                                     ' has no primary key, skipping generation access package.');
            return;
         end if;
      end if;
   
      if params.log_cloud_events.exists('aq_queue_name') and params.log_cloud_events('aq_queue_name') is not null then
         declare
            l_priv all_queues.owner%type;
         begin
            select distinct nvl(p.grantee, q.owner)
              into l_priv
              from all_queues q
              left outer join all_tab_privs p on q.owner = p.table_schema
                                             and q.name = p.table_name
                                             and p.grantee = g_owner
                                             and p.privilege = 'ENQUEUE'
             where q.name = params.log_cloud_events('aq_queue_name')
               and nvl(p.grantee, q.owner) = g_owner;
            with_cloud_events := l_priv is not null;
         exception
            when no_data_found then
               raise_application_error(-20000,
                                       'Queue ''' || params.log_cloud_events('aq_queue_name') ||
                                       ''' not found or no privlege to enqueue granted.');
         end;
      end if;
      if params.log_cloud_events.exists('tab_name') and params.log_cloud_events('tab_name') is not null then
         declare
            l_priv all_tables.owner%type;
         begin
            select distinct nvl(p.grantee, t.owner)
              into l_priv
              from all_tables t
              left outer join all_tab_privs p on t.owner = p.table_schema
                                             and t.table_name = p.table_name
                                             and p.grantee = g_owner
                                             and p.privilege = 'INSERT'
             where t.table_name = params.log_cloud_events('tab_name')
               and nvl(p.grantee, t.owner) = g_owner;
            with_cloud_events := l_priv is not null;
         exception
            when no_data_found then
               raise_application_error(-20000,
                                       'Table ''' || params.log_cloud_events('tab_name') ||
                                       ''' not found or no privlege to insert granted.');
         end;
      end if;
   end;

   function tapi_source
   (
      p_table_name     in varchar2,
      p_schema_name    in varchar2 default user,
      p_raise_on_error boolean default true
   ) return clob is
   begin
      init_single_run(p_table_name, p_schema_name, p_raise_on_error);
      return create_header || nll || create_body;
   end;

   procedure compile_tapi
   (
      p_table_name     in varchar2,
      p_schema_name    in varchar2 default user,
      p_raise_on_error boolean default true
   ) is
      start_time constant pls_integer := sys.dbms_utility.get_time;
   begin
      init_single_run(p_table_name, p_schema_name, p_raise_on_error);
      if params.plsql_optimize_level is not null then
         execute immediate 'alter session set plsql_optimize_level = ' || params.plsql_optimize_level;
      end if;
   
      execute immediate create_header;
      execute immediate create_body;
      sys.dbms_output.put_line('Timing for package ''' || tapi_name || ''': ' ||
                               mod(sys.dbms_utility.get_time - start_time + c_bignum, c_bignum) || '/100s');
   end;

   procedure compile_tapis
   (
      p_owner     in varchar2 default user,
      p_name_like in varchar2 default null
   ) is
   begin
      for tab in (select t.owner
                        ,t.table_name
                    from all_tables t
                   where t.owner = p_owner
                     and (p_name_like is null or t.table_name like p_name_like)
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
      if p_immutable and not (dbms_db_version.version >= 19 and dbms_db_version.release >= 11) then
         raise_application_error(-20000, 'Immutable table require at least compatibility level 19.11.');
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
      p_schema_name in varchar2 default user
   ) is
      l_type all_types.type_name%type;
   begin
      if not has_priv_for_sys_('DBMS_AQADM', p_schema_name) then
         raise_application_error(-20000, 'User ' || p_schema_name || ' is missing privileges to access DBMS_AQADM.');
      end if;
   
      begin
         select t.type_name
           into l_type
           from all_types t
          where t.owner = sys.dbms_assert.schema_name(p_schema_name)
            and upper(t.type_name) = upper(type_cloud_event);
      exception
         when no_data_found then
            execute immediate 'create or replace type ' || p_schema_name || '.' || type_cloud_event ||
                              ' authid definer as object' || '(' || 'ce_id raw(11),' || nlt || 'ce_time timestamp,' || nlt ||
                              'ce_type varchar2(100),' || nlt || 'ce_source varchar2(200),' || nlt || 'ce_data clob' || nlt || ');';
      end;
   
      execute immediate 'begin' || nlt || 'sys.dbms_aqadm.create_queue_table(queue_table        => ''' || p_schema_name ||
                        '.'' || ' || nltt || 'sys.dbms_assert.simple_sql_name(''' || p_queue_name || ''') || ''' ||
                        ce_table_name_suffix || ''',' || nltt || 'queue_payload_type => ''' || type_cloud_event ||
                        ''',' || nltt || 'sort_list          => ''enq_time'',' || nltt || 'multiple_consumers => true,' || nltt ||
                        'compatible         => ''10.0'',' || nltt || 'comment            => ''cloudevents from ' ||
                        upper($$plsql_unit) || ''');' || nl || 'end;';
      execute immediate 'sys.dbms_aqadm.create_queue(queue_name          => ' || p_schema_name || '.' || p_queue_name || ',' || nl ||
                        'queue_table         => ' || p_schema_name || '.' || p_queue_name || ce_table_name_suffix || ',' || nl ||
                        'queue_type          => 0,' || nl || 'max_retries         => 2000000000,' || nl ||
                        'retry_delay         => 0,' || nl || 'dependency_tracking => false)';
      execute immediate 'sys.dbms_aqadm.start_queue(' || p_queue_name || ')';
   end;

   procedure drop_ce_queue
   (
      p_queue_name  in varchar2,
      p_schema_name in varchar2 default user,
      p_drop_type   in boolean default false
   ) is
   begin
      if not has_priv_for_sys_('DBMS_AQADM', p_schema_name) then
         raise_application_error(-20000, 'User ' || p_schema_name || ' is missing privileges to access DBMS_AQADM.');
      end if;
   
      execute immediate 'sys.dbms_aqadm.stop_queue(queue_name => sys.dbms_assert.schema_name(' || p_schema_name || ')' || '.' || nl ||
                        'sys.dbms_assert.simple_sql_name(' || p_queue_name || '),' || nl || 'wait       => false)';
      execute immediate 'sys.dbms_aqadm.drop_queue(' || p_schema_name || '.' || p_queue_name || ')';
      execute immediate 'sys.dbms_aqadm.drop_queue_table(' || p_schema_name || '.' || p_queue_name ||
                        ce_table_name_suffix || ')';
      if p_drop_type then
         execute immediate 'drop type ' || p_schema_name || '.' || type_cloud_event;
      end if;
   end;

   procedure init(p_params params_t) is
   begin
      priv_to_dbms_crypto                := has_priv_for_sys_('DBMS_CRYPTO');
      priv_to_dbms_aqadm                 := has_priv_for_sys_('DBMS_AQADM');
      params                             := p_params;
      params.proc_select                 := sys.dbms_assert.simple_sql_name(params.proc_select);
      params.proc_update                 := sys.dbms_assert.simple_sql_name(params.proc_update);
      params.proc_insert                 := sys.dbms_assert.simple_sql_name(params.proc_insert);
      params.proc_insert_cur             := sys.dbms_assert.simple_sql_name(params.proc_insert_cur);
      params.proc_delete                 := sys.dbms_assert.simple_sql_name(params.proc_delete);
      params.proc_merge                  := sys.dbms_assert.simple_sql_name(params.proc_merge);
      params.proc_exists                 := sys.dbms_assert.simple_sql_name(params.proc_exists);
      params.proc_exists_and_select      := sys.dbms_assert.simple_sql_name(params.proc_exists_and_select);
      params.proc_lock_record            := sys.dbms_assert.simple_sql_name(params.proc_lock_record);
      params.proc_count                  := sys.dbms_assert.simple_sql_name(params.proc_count);
      params.proc_print                  := sys.dbms_assert.simple_sql_name(params.proc_print);
      params.proc_json_obj               := sys.dbms_assert.simple_sql_name(params.proc_json_obj);
      params.proc_checksum               := sys.dbms_assert.simple_sql_name(params.proc_checksum);
      params.proc_diff                   := sys.dbms_assert.simple_sql_name(params.proc_diff);
      params.proc_pipe := case
                             when params.proc_pipe is not null then
                              sys.dbms_assert.simple_sql_name(params.proc_pipe)
                             else
                              null
                          end;
      params.parameter_prefix            := sys.dbms_assert.simple_sql_name(params.parameter_prefix);
      params.logging_exception_procedure := lower(params.logging_exception_procedure);
      if params.audit_user_exp is null then
         params.audit_col_created_by  := null;
         params.audit_col_modified_by := null;
      end if;
   
      if not params.double_quote_names then
         params.audit_col_created_by    := upper(params.audit_col_created_by);
         params.audit_col_modified_by   := upper(params.audit_col_modified_by);
         params.audit_col_created_date  := upper(params.audit_col_created_date);
         params.audit_col_modified_date := upper(params.audit_col_modified_date);
      end if;
   
      g_audit_cols          := str_list(params.audit_col_created_by,
                                        params.audit_col_modified_by,
                                        params.audit_col_created_date,
                                        params.audit_col_modified_date);
      g_audit_cols_created  := str_list(params.audit_col_created_by, params.audit_col_created_date);
      g_audit_cols_modified := str_list(params.audit_col_modified_by, params.audit_col_modified_date);
      g_audit_cols_user     := str_list(params.audit_col_created_by, params.audit_col_modified_by);
      g_audit_cols_date     := str_list(params.audit_col_created_date, params.audit_col_modified_date);
      if params.tap_size != length(tab) then
         tab     := rpad(' ', params.tap_size, ' ');
         nlt     := nl || tab;
         nltt    := nlt || tab;
         nlttt   := nltt || tab;
         nltttt  := nlttt || tab;
         nlttttt := nltttt || tab;
      end if;
   end;

begin
   null;
end;
/