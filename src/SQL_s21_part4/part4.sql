create or replace procedure delete_table()
as $$
declare
  name text;
begin
  for name in (select table_name from information_schema.tables where table_schema = current_schema()
  and table_name like 'tablename%')
  loop
    execute 'DROP TABLE IF EXISTS ' || quote_ident(name) || ' CASCADE';
  end loop;
end
$$ language plpgsql;

create or replace procedure find_functions(out functions_count integer)
as $$
declare
  function_name text;
  function_param text;
  user_oid oid;
begin
  user_oid := (select oid from pg_catalog.pg_authid where rolname = current_user);
  functions_count := 0;
  for function_name, function_param in (select proname, pg_catalog.pg_get_function_arguments(pg_proc.oid) from pg_catalog.pg_proc
   where proowner = user_oid and proretset = false and pronargs > 0 order by 1)
    loop
      functions_count := functions_count + 1;
      raise notice '%:Function_name: %, Parameters: %', functions_count, function_name, function_param;
    end loop;
end
$$ language plpgsql;

create or replace procedure delete_sql_dml_triggers(out deleted_triggers integer)
as $$
declare
  trig_name text;
  table_name text;
begin
  deleted_triggers := 0;
  for trig_name, table_name in (select trigger_name, event_object_table from information_schema.triggers
   where trigger_schema = current_schema())
  loop
    deleted_triggers := deleted_triggers + 1;
    execute 'drop trigger if exists ' || trig_name || ' on ' || quote_ident(table_name) || ' cascade';
  end loop;
end
$$ language plpgsql;

create or replace procedure find_sql_objects(ref refcursor, containe text)
as $$
begin
  open ref for (
    select routine_name, routine_type from information_schema.routines as routines
    where routines.routine_schema not in ('pg_catalog', 'information_schema')
    and routines.routine_definition ilike ('%' || containe || '%')
  );
end;
$$ language plpgsql;


-- Testspace


-- 1 Задание
call delete_table();

-- 2 Задание
do $$
declare
  function_count integer;
begin
  call find_functions(function_count);
  raise notice 'Procedure find % functions', function_count;
end $$;

-- 3 Задание
do $$
declare
  trigger_count integer;
begin
  call delete_sql_dml_triggers(trigger_count);
  raise notice 'Deleted % SQL DML triggers', trigger_count;
end $$;

-- 4 Задание
begin;
  call find_sql_objects('ref', '');
  fetch all in "ref";
end;
