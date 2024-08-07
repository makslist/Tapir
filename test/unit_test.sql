-- setup
create table tapi(
   col_id int generated always as identity,
   pk1 varchar2(3) not null,
   pk2 number not null,
   col1 varchar2(10),
   col2 timestamp,
   unique_col varchar2(20),
   created_at timestamp default systimestamp not null,
   number_1 number default 23,
   number_2 number default 42,
   number_add integer generated always as (number_1 + number_2) virtual,
   knz_relevant varchar2(1),
   bloby blob,
   creator varchar2(8),
   created date,
   modifier varchar2(8),
   modified date,
   constraint tapi_pk primary key(pk1, pk2) using index,
   unique (unique_col) using index,
   check (knz_relevant in ('1', '0'))
)
/

create index tapi_col1 on tapi (col1)
  nologging

/
/*
create unique index tapi_unique on tapi(unique_col)
  nologging

/*/

create index tapi_created_at on tapi(created_at)
  nologging

/

--------------------------------------------------------------------------------
-- create ce_table
begin
   tapir.create_ce_table('aq_ce');
end;
/

--------------------------------------------------------------------------------
-- create ce_table
begin
   tapir.create_ce_table('tapi$cloud_events');
end;
/

--------------------------------------------------------------------------------
-- create tapi
begin
   tapir.init(tapir.params_t(tapi_name                   => tapir.mapping('^(.*)$' => 'zz_\1'),
                             logging_exception_procedure => 'dbms_output.put_line(\1)',
                             audit_user_exp              => '''me''',
                             audit_col_created_by        => 'CREATOR',
                             audit_col_created_date      => 'CREATED',
                             audit_col_modified_by       => 'MODIFIER',
                             audit_col_modified_date     => 'MODIFIED',
                             double_quote_names          => false,
                             parameter_prefix            => 'p_',
                             proc_exists                 => 'exist',
                             proc_lock_record            => 'sel_lock',
                             for_update_timeout          => '15',
                             boolean_pseudo_type         => tapir.boolean_pseudo_10,
                             use_result_cache            => true,
                             create_occ_methods          => true,
                             log_cloud_events            => tapir.log_cloud_events('tapi$cloud_events', 'aq_ce'),
                             custom_default_expressions  => tapir.mapping('PK1'        => 'sys.dbms_random.string(''L'', round(sys.dbms_random.value(1, 4)))',
                                                                          'PK2'        => 'round(sys.dbms_random.value(1, 1024 * 1024))',
                                                                          'COL1'       => 'substr(sys_guid(), 1, 10)',
                                                                          'COL2'       => 'systimestamp + 10',
                                                                          'UNIQUE_COL' => 'substr(sys_guid(), 1, 20)',
                                                                          'CREATED_AT' => 'systimestamp - 100',
                                                                          'NUMBER_1'   => 'round(sys.dbms_random.value(1, 1024 * 1024))',
                                                                          'NUMBER_2'   => 'round(sys.dbms_random.value(1, 1024 * 1024))',
                                                                          'BLOBY'      => 'utl_raw.cast_to_raw(''blob'')',
                                                                          'KNZ_RELEVANT' => '1')));
   tapir.compile_tapi(p_owner => user, p_table_name => 'TAPI');
end;
/

--------------------------------------------------------------------------------
-- exists_sel (return false, if non existing)
declare
   l_pk1    zz_tapi.pk1_t := 'a';
   l_pk2    zz_tapi.pk2_t := 1;
   l_tapi   zz_tapi.rt := zz_tapi.ins(
      p_pk1 => l_pk1,
      p_pk2 => l_pk2
   );
   l_exists boolean;
begin
   l_exists := zz_tapi.exists_sel('B', l_pk2, l_tapi);
   tapir.assert(not l_exists, 'found non-existing row.');
   rollback;
end;
/

-- exists_sel (return true, if existing)
declare
   l_pk1    zz_tapi.pk1_t := 'a';
   l_pk2    zz_tapi.pk2_t := 1;
   l_tapi   zz_tapi.rt := zz_tapi.ins(
      p_pk1 => l_pk1,
      p_pk2 => l_pk2
   );
   l_exists boolean;
begin
   l_exists := zz_tapi.exists_sel(l_pk1, l_pk2, l_tapi);
   tapir.assert(l_exists, 'existing row was not found.');
   rollback;
end;
/

-- exists_sel (return found record, if existing)
declare
   l_pk1    zz_tapi.pk1_t := 'a';
   l_pk2    zz_tapi.pk2_t := 1;
   l_tapi   zz_tapi.rt := zz_tapi.ins(
      p_pk1 => l_pk1,
      p_pk2 => l_pk2
   );
   l_exists boolean;
begin
   l_tapi   := null;
   l_exists := zz_tapi.exists_sel(l_pk1, l_pk2, l_tapi);
   tapir.assert(l_tapi.pk1 = l_pk1
   and l_tapi.pk2 = l_pk2, 'haven''t found row.');
   rollback;
end;
/

--------------------------------------------------------------------------------
-- select
declare
   l_pk1      zz_tapi.pk1_t := 'a';
   l_pk2      zz_tapi.pk2_t := 2;
   l_tapi     tapi%rowtype := zz_tapi.rt(
      pk1 => l_pk1,
      pk2 => l_pk2
   );
   l_tapi_sel tapi%rowtype;
begin
   begin
      zz_tapi.sel(l_tapi);
      raise_application_error( - 20999, 'select  doesn''t escalate exception');
   exception
      when no_data_found then
         null;
   end;

   insert into tapi (
      pk1,
      pk2
   ) values (
      l_pk1,
      l_pk2
   );
   zz_tapi.sel(l_tapi);
   tapir.assert(l_tapi_sel.pk1 != null, 'select failed.');
   l_tapi_sel := zz_tapi.sel(l_pk1, l_pk2);
   tapir.assert(l_tapi_sel.pk1 != null, 'select pk-single failed.');
   rollback;
exception
   when others then
      rollback;
      raise;
end;
/

--------------------------------------------------------------------------------
-- exists
declare
   l_pk1      zz_tapi.pk1_t := 'a';
   l_pk2      zz_tapi.pk2_t := 2;
   l_col1     zz_tapi.col1_t := 'test';
   l_tapi_ref constant tapi%rowtype := zz_tapi.rt(
      pk1 => l_pk1,
      pk2 => l_pk2
   );
   l_exists   boolean;
begin
   l_exists := zz_tapi.exist(l_tapi_ref);
   tapir.assert(not l_exists, 'exist found non-existing row.');
   insert into tapi (
      pk1,
      pk2,
      col1
   ) values (
      l_pk1,
      l_pk2,
      l_col1
   );
   l_exists := zz_tapi.exist(l_tapi_ref);
   tapir.assert(l_exists, 'exist hasn''t found row.');
   l_exists := zz_tapi.exist(l_pk1, l_pk2);
   tapir.assert(l_exists, 'exist hasn''t found row.');
   rollback;
exception
   when others then
      rollback;
      raise;
end;
/

--------------------------------------------------------------------------------
-- diff_two_records
declare
   l_pk1_ref         zz_tapi.pk1_t := 'AaA';
   l_pk2_ref         zz_tapi.pk2_t := 1;
   l_pk1_comp        zz_tapi.pk1_t := 'bBb';
   l_pk2_comp        zz_tapi.pk2_t := '999';
   l_first           zz_tapi.rt := zz_tapi.rt(
      pk1     => l_pk1_ref,
      pk2     => l_pk2_ref,
      creator => 'first'
   );
   l_tapi_2          zz_tapi.rt := zz_tapi.rt(
      pk1     => l_pk1_comp,
      pk2     => l_pk2_comp,
      creator => 'second'
   );
   l_tapi_only_audit zz_tapi.rt := l_first;
   l_diff            json_object_t;
begin
   l_diff := zz_tapi.diff(l_first, l_first);
   dbms_output.put_line(l_diff.to_string);
   tapir.assert(l_diff.get_size = 0, 'diff shows diff when identical');
   l_diff := zz_tapi.diff(l_first, l_tapi_2);
   tapir.assert(l_diff.get_object('new').get_string('creator') = 'second', 'diff column is missing');
   tapir.assert(l_diff.get_object('new').get_string('pk1') = l_pk1_comp, 'diff column is missing');
   tapir.assert(l_diff.get_object('old').get_string('pk2') = l_pk2_ref, 'diff column is missing');
   tapir.assert(l_diff.get_object('new').get_string('pk2') = l_pk2_comp, 'diff column is missing');
   rollback;
end;
/

--------------------------------------------------------------------------------
-- raise_if_pk_is_null
declare
   l_tapi tapi%rowtype;
begin
   l_tapi := zz_tapi.sel(1, null);
   rollback;
exception
   when others then
      rollback;
      if not sqlcode = - 20000 then
         raise;
      end if;
end;
/

--------------------------------------------------------------------------------
-- counts
declare
   l_pk1   zz_tapi.pk1_t := 'a';
   l_count number;
begin
   l_count := zz_tapi.counts('foo', 999);
   tapir.assert(l_count = 0, 'counts found ' || l_count || ' rows.');
   insert into tapi (
      pk1,
      pk2
   ) values (
      l_pk1,
      1
   );
   insert into tapi (
      pk1,
      pk2
   ) values (
      l_pk1,
      2
   );
   insert into tapi (
      pk1,
      pk2
   ) values (
      'b',
      1
   );
   l_count := zz_tapi.counts(l_pk1, 1);
   tapir.assert(l_count = 1, 'counts found ' || l_count || ' rows.');
   l_count := zz_tapi.counts(l_pk1, null);
   tapir.assert(l_count = 2, 'counts found ' || l_count || ' rows.');
   l_count := zz_tapi.counts(null, null);
   tapir.assert(l_count = 3, 'counts found ' || l_count || ' rows.');
   rollback;
exception
   when others then
      rollback;
      raise;
end;
/

--------------------------------------------------------------------------------
-- update
declare
   l_pk1      zz_tapi.pk1_t := 'a';
   l_pk2      zz_tapi.pk2_t := 1;
   l_col1     zz_tapi.col1_t := 'test';
   l_tapi     tapi%rowtype := zz_tapi.rt(
      pk1 => l_pk1,
      pk2 => l_pk2
   );
   l_tapi_sel tapi%rowtype;
begin
   insert into tapi (
      pk1,
      pk2,
      col1
   ) values (
      l_pk1,
      l_pk2,
      l_col1
   );
   zz_tapi.sel(l_tapi);
   l_tapi.col1 := l_col1 || 'zzz';
   zz_tapi.upd(l_tapi);
   tapir.assert(l_tapi.col1 != l_col1, 'update column failed.');
   rollback;
exception
   when others then
      rollback;
      raise;
end;
/

--------------------------------------------------------------------------------
-- delete
declare
   l_pk1  zz_tapi.pk1_t := 'a';
   l_pk2  zz_tapi.pk2_t := 1;
   l_col1 zz_tapi.col1_t := 'test';
   l_tapi tapi%rowtype := zz_tapi.rt(
      pk1 => l_pk1,
      pk2 => l_pk2
   );
begin
   insert into tapi (
      pk1,
      pk2,
      col1
   ) values (
      l_pk1,
      l_pk2,
      l_col1
   );
   l_tapi      := zz_tapi.sel(p_pk1 => l_pk1, p_pk2 => l_pk2);
   l_tapi.col1 := l_col1 || 'zzz';
   zz_tapi.upd(l_tapi);
   tapir.assert(l_tapi.col1 != l_col1, 'update column failed.');
   rollback;
exception
   when others then
      rollback;
      raise;
end;
/

--------------------------------------------------------------------------------
-- merge
declare
   l_pk1      zz_tapi.pk1_t := 'a';
   l_pk2      zz_tapi.pk2_t := 1;
   l_col1_old zz_tapi.col1_t := 'old';
   l_col1_new zz_tapi.col1_t := 'new';
   l_tapi     tapi%rowtype;
begin
   zz_tapi.upsert(zz_tapi.rt(
      pk1  => l_pk1,
      pk2  => l_pk2,
      col1 => l_col1_old
   ));
   l_tapi      := zz_tapi.sel(l_pk1, l_pk2);
   tapir.assert(l_tapi.col1 = l_col1_old, 'upsert(insert) failed.');
   l_tapi.col1 := l_col1_new;
   zz_tapi.upsert(l_tapi);
   l_tapi      := zz_tapi.sel(l_pk1, l_pk2);
   tapir.assert(l_tapi.col1 = l_col1_new, 'upsert(update) failed.');
   rollback;
exception
   when others then
      rollback;
      raise;
end;
/

--------------------------------------------------------------------------------
-- sel_rows
declare
   l_pk1      zz_tapi.pk1_t := 'a';
   l_tab      zz_tapi.rows_tab;
   l_count    number;
   c_tapi_pk1 zz_tapi.strong_ref_cursor;
begin
   insert into tapi (
      pk1,
      pk2
   ) values (
      l_pk1,
      1
   );
   insert into tapi (
      pk1,
      pk2
   ) values (
      l_pk1,
      2
   );
   insert into tapi (
      pk1,
      pk2
   ) values (
      'b',
      1
   );
   open c_tapi_pk1 for
      select t. *
      from tapi t
      where t.pk1 = l_pk1;
   l_tab := zz_tapi.sel_rows(c_tapi_pk1);
   tapir.assert(l_tab.count = 2, 'sel_rows selected ' || l_tab.count || ' rows.');
   tapir.assert(not c_tapi_pk1%isopen, 'sel_rows cursor is still open.');
   l_tab := zz_tapi.sel_rows(c_tapi_pk1);
   tapir.assert(l_tab.count = 0, 'sel_rows selected ' || l_tab.count || ' rows.');
   open c_tapi_pk1 for
      select t. *
      from tapi t
      where t.pk1 = l_pk1;
   l_tab := zz_tapi.sel_rows(c_tapi_pk1, 1);
   tapir.assert(l_tab.count = 1, 'sel_rows selected ' || l_tab.count || ' rows.');
   tapir.assert(c_tapi_pk1%isopen, 'sel_rows cursor is closed.');
   l_tab := zz_tapi.sel_rows(c_tapi_pk1, 1);
   tapir.assert(l_tab.count = 1, 'sel_rows selected ' || l_tab.count || ' rows.');
   tapir.assert(c_tapi_pk1%isopen, 'sel_rows cursor is closed.');
 
   -- cursor is still open, because it is not known eforehand if more rows will be fetched
   l_tab := zz_tapi.sel_rows(c_tapi_pk1, 1);
   tapir.assert(l_tab.count = 0, 'sel_rows selected ' || l_tab.count || ' rows.');
   tapir.assert(not c_tapi_pk1%isopen, 'sel_rows cursor is closed.');
   rollback;
exception
   when others then
      rollback;
      raise;
end;
/

--------------------------------------------------------------------------------
-- upd_rows
declare
   l_pk1      zz_tapi.pk1_t := 'a';
   l_col1     zz_tapi.col1_t := 'zzz';
   l_tab      zz_tapi.rows_tab;
   row_cursor zz_tapi.strong_ref_cursor;
begin
   l_tab := zz_tapi.rows_tab(zz_tapi.rt(pk1 => l_pk1, pk2 => 1), zz_tapi.rt(pk1 => l_pk1, pk2 => 2));
   zz_tapi.ins_rows(l_tab);
   open row_cursor for
      select *
      from tapi t
      where t.pk1 = l_pk1;
   l_tab := zz_tapi.sel_rows(row_cursor);
   for i in 1..l_tab.count loop
      l_tab(i).col1 := l_col1;
   end loop;

   zz_tapi.upd_rows(l_tab);
   tapir.assert(sql%rowcount = 2, 'upd_rows updated ' || sql%rowcount || ' rows.');
   rollback;
exception
   when others then
      rollback;
      raise;
end;
/

--------------------------------------------------------------------------------
-- index_cursor
declare
   l_pk1   zz_tapi.pk1_t := 'a';
   l_col1  zz_tapi.col1_t := 'zzz';
   l_tab   zz_tapi.rows_tab := zz_tapi.rows_tab(zz_tapi.rt(
      pk1  => l_pk1,
      pk2  => 1,
      col1 => l_col1
   ), zz_tapi.rt(
      pk1 => l_pk1,
      pk2 => 2
   ));
   l_count number := 0;
begin
   zz_tapi.ins_rows(l_tab);
   for rec in zz_tapi.cur_idx_col1(l_col1) loop
      l_count := l_count + 1;
   end loop;

   tapir.assert(l_count = 1, 'index_cursor found ' || l_count || ' rows.');
   rollback;
exception
   when others then
      rollback;
      raise;
end;
/

--------------------------------------------------------------------------------
-- to_json
declare
   l_pk1  zz_tapi.pk1_t := 'a';
   l_pk2  zz_tapi.pk2_t := 1;
   l_col1 zz_tapi.col1_t := 'json';
   l_tapi tapi%rowtype := zz_tapi.rt(
      pk1  => l_pk1,
      pk2  => l_pk2,
      col1 => l_col1
   );
   json   json_object_t;
begin
   json := zz_tapi.json_obj(l_tapi);
   tapir.assert(instr(json.to_string, l_col1) > 0, 'json output failed.');
   rollback;
exception
   when others then
      rollback;
      raise;
end;
/

--------------------------------------------------------------------------------

drop package zz_tapi

/

drop table tapi$ce

/

drop table tapi

/
