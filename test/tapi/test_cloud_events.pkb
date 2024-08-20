create or replace package body test_cloud_events is

   function count_objects(p_name_like in varchar2) return pls_integer is
      l_count pls_integer;
   begin
      select count(*)
        into l_count
        from all_objects o
       where o.owner = user
         and upper(o.object_name) like upper('%' || p_name_like || '%');
      return l_count;
   end;

   procedure test_create_ce_table is
      l_ce_tab_name varchar2(100) := 'CE_TABLE';
   begin
      tapir.create_ce_table(p_table_name => l_ce_tab_name);
      --ut.expect(count_objects(l_ce_tab_name)).to_equal(1);
   
      execute immediate 'drop table ' || l_ce_tab_name;
   end;

   procedure test_create_ce_queue is
      l_queue_name varchar2(100) := 'CE_QUEUE';
   begin
      tapir.create_ce_queue(p_queue_name => l_queue_name, p_schema_name => user);
      -- ut.expect(count_objects('cloud_event')).to_equal(1);
      -- ut.expect(count_objects(l_queue_name || '_tab')).to_equal(1);
      -- ut.expect(count_objects(l_queue_name)).to_equal(1);
   
      tapir.drop_ce_queue(p_queue_name => l_queue_name);
   end;

   procedure test_drop_ce_queue is
      l_queue_name varchar2(100) := 'CE_QUEUE';
   begin
      tapir.create_ce_queue(p_queue_name => l_queue_name, p_schema_name => user);
      -- ut.expect(count_objects(l_queue_name)).to_equal(1);
      tapir.drop_ce_queue(p_queue_name => l_queue_name);
      --  ut.expect(count_objects('cloud_event')).to_equal(0);
      -- ut.expect(count_objects(l_queue_name)).to_equal(0);
      -- ut.expect(count_objects(l_queue_name || '_tab')).to_equal(0);
   end;

end;
/
