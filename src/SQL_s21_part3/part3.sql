create or replace function GetReadableTransferredPoints()
returns table (Peer1 varchar, Peer2 varchar, PointsAmount int)
as
$$
begin
  return query
  select tp1.checkingpeer as peer1,
    tp1.checkedpeer as peer2,
    tp1.pointsamount - coalesce(tp2.pointsamount, 0)
    as pointsamount
  from transferredpoints tp1
  left join transferredpoints tp2 on tp1.checkedpeer = tp2.checkingpeer
  and tp1.checkingpeer = tp2.checkedpeer
  order by pointsamount desc;
end;
$$ language plpgsql;

create or replace function GetSuccessTask()
  returns table (
    Peer varchar,
    Task varchar,
    XP int
  )
  language plpgsql
as
$$
  begin
    return query
    select ch.peer as Peer, ch.task as Task, x.xpamount as XP from checks ch
    join p2p p on ch.id = p.checkid
    join verter v on ch.id = v.checkid
    join xp x on ch.id = x.checkid
    where p.status = 'Success' and v.status = 'Success';
   end;
$$;

create or replace function GetGeeks(day date)
  returns table (Peer1 varchar)
  language plpgsql
as
$$
begin
  return query
    select  Peer from timetracking
    where date = day and state <> 2
    group by peer
    having count(distinct state) = 1;
end;
$$;

create or replace function CalculatePoints()
  returns table (Peer varchar, PointsChange int)
  language plpgsql
as
$$
  begin
    return query
    select tr1.checkingpeer as Peer,
    tr1.pointsamount - tr2.pointsamount as PointsChange
    from transferredpoints tr1
    join transferredpoints tr2
    on tr2.checkedpeer = tr1.checkingpeer
    order by PointsChange desc;
  end;
$$;

create or replace function CalculatePointsForGeeks()
  returns table (Peer varchar, PointsChange int)
  language plpgsql
as
$$
  begin
    return query
    select peer1, pointsamount as PointsChange
    from GetReadableTransferredPoints();
  end;
$$;

create or replace procedure FindMaxCheckedTask()
language plpgsql
AS $$
  begin
    create materialized view count_checked_task_mv as
    select date, task, count(task) as count
    from checks
    group by task, date;
    create materialized view max_visits_mv as
    select date, task, row_number()
    over (partition by date order by count desc) as row_num
    from count_checked_task_mv;
    perform date, task from max_visits_mv where row_num = 1;
  end;
$$;

create or replace procedure FindAllPeersForFinalBlock(task_name_pp varchar)
language plpgsql
as $$
  begin
    select p.nickname, c.date, c.task from verter vr
    join checks c on c.id = vr.checkid
    join peers p on p.nickname = c.peer
    where vr.Status = 'Success' and
    c.task like case
      when task_name_pp like 'CPP' then 'CPP5%'
      when task_name_pp like 'C' then 'C8'
      when task_name_pp like 'DO' then 'DO6'
      when task_name_pp like 'A' then 'A8'
      when task_name_pp like 'SQL' then 'SQL3'
    end;
  end;
$$;

create or replace procedure FindMaxCheckedTask()
language plpgsql
as $$
  begin
    create materialized view count_recommended_peer_mv as
    select peer, recommendedpeer, count(recommendedpeer) as count from recommendations
    where peer in (select peer2 from friends
      join peers p on p.nickname = friends.peer1
      where peer1 = p.nickname)
    group by peer, recommendedpeer;

    create materialized view matched_peer_mv as
    select peer, recommendedpeer, row_number()
    over (partition by recommendedpeer order by count desc) as row_num
    from count_recommended_peer_mv;

    select peer, recommendedpeer from matched_peer_mv where row_num = 1;
  end;
$$;

create or replace procedure PercentPeerSuccessPassedBlock(
    block1 varchar,
    block2 varchar
)
language plpgsql
as $$
  declare
    start_block1 int;
    start_block2 int;
    start_both_block int;
    not_start_empty_block int;
  BEGIN
    start_block1 :=
      (select count(counts) from
        (select peer, task, count(distinct task) as counts
        from checks
        group by task, peer) as tb1
       where tb1.task = block1
        group by counts) * 100/ (select count(*)from peers);
    start_block2 :=
      (select count(counts) from
        (select peer, task, count(task) as counts from checks
        group by task, peer) as tb1
      where tb1.task = block2
      group by counts) * 100/ (select count(*) from peers);
    start_both_block :=
      (select count(counts) from
        (select peer, task, count(task) as counts from checks
        group by task, peer) as tb1
      where tb1.task in (block1, block2)
      group by counts) * 100/ (select count(*) from peers);
    not_start_empty_block :=
      (select
        count(counts) * 100/ (select count(*) from peers)
      from
        (select peer, task, count(task) as counts from checks
        group by task, peer) as tb1
      where tb1.task not in (block1, block2)
      group by counts);
  end;
$$;

create or replace function CountPassedPeer(state status)
  returns int
language plpgsql
as $$
  declare
    final_state status;
  begin
    if state = 'Success' then
      final_state := state;
    else
      final_state := 'Failure';
    end if;

    return (select count(ch.peer) as birthday from checks ch
      join peers p on p.nickname = ch.peer
      join p2p p2p2 on ch.id = p2p2.checkid
      join verter v on ch.id = v.checkid
      where ch.date = p.birthday
      and p2p2.status = 'Success'
      and v.status = 'Success');
  end;
$$;

create or replace procedure FindMaxCheckedTask()
language plpgsql
as $$
  declare
    count_peer int := (select count(*) from peers);
    count_success_passed_peer int := CountPassedPeer('Success');
    count_failure_passed_peer int := CountPassedPeer('Failure');
  begin
    create table my_temp_table (
      SuccessfulChecks int,
      UnsuccessfulChecks int);
    insert into my_temp_table (SuccessfulChecks, UnsuccessfulChecks) values
      (count_success_passed_peer * 100 / count_peer,
      count_failure_passed_peer * 100 / count_peer);
    select * from my_temp_table;
  end;
$$;

CREATE OR REPLACE PROCEDURE GetPeerPassedTask(
    task_1 varchar,
    task_2 varchar,
    task_3 varchar
)
language plpgsql
AS $$
begin
    select peer from checks
    where task in (task_1, task_2) and task <> task_3;
  end;
$$;

create or replace function RecursiveCountTaskInBlock()
  returns table(task varchar, prevcount int)
language plpgsql
as $$
begin
  with recursive prevcnt as (
    select title, 1 as prevcount
    from tasks
    where parenttask is null
    union all
    select tasks.title, prevcnt.prevcount + 1
    from tasks
    join prevcnt on tasks.parenttask = prevcnt.title
    )
    select title, prevcount
    from prevcnt
    order by title;
  end;
$$;

create or replace procedure LuckyDaysForChecks(
    n_count int
)
language plpgsql
as $$
  begin
    create materialized view list_all_peers as (
      select ch.peer, ch.task, ch.date, p.status, row_number()
      over (partition by p.time order by p.status) as row_number
      from checks ch
      join p2p p on ch.id = p.checkid
      where status <> 'Start'
    );
    select date from list_all_peers where row_number = n_count and status = 'Success';
  end;
$$;

create or replace function FindThePeerWithTheHighestAmountOfXP()
  returns table (PeerName varchar, XP bigint)
language plpgsql
as $$
  begin
    create materialized view names_and_xp as (
      select peers.nickname, c.task, x.xpamount
      from peers
      join checks c on peers.nickname = c.peer
      join xp x on c.id = x.checkid
      join p2p p on c.id = p.checkid
      join verter v on c.id = v.checkid
      where p.status = 'Success' and v.status = 'Success');

    create materialized view sum_xp_by_names as (
      select sum(xpamount) as xpamount, nickname
      from names_and_xp
      group by nickname);

    return query (select s.nickname, xpamount from sum_xp_by_names s
      where xpamount = (select max(xpamount) from sum_xp_by_names));
  end;
$$;

create or replace procedure PunctualPeers(
    time_vis time,
    n_count int
)
as $$
  begin
    perform peer from (select peer, count(peer) as count
    from timetracking where time < time_vis and state = 1
    group by peer) as foo
    where count >= n_count;
  end;
$$ language plpgsql;

-- This function need's for test upper procedure
create or replace function PunctualPeersFun1(
    time_vis time,
    n_count int
)
returns table (Peer1 varchar)
as $$
  begin
    return query
    select peer from (select peer, count(peer) as count
    from timetracking where time < time_vis and state = 1
    group by peer) as foo
    where count >= n_count;
  end;
$$ language plpgsql;

create or replace procedure PunctualPeers(
    m_count int,
    n_count int
)
language plpgsql
as $$
  begin
    select truant.peer from
    (select peer, count(peer) from timetracking
    where Date between current_date - n_count
    and current_date
    and state = 1
    group by peer) as truant
    where truant.count >= m_count;
  end;
$$;

-- This function need's for test upper procedure
create or replace function PunctualPeersFun2(
    m_count int,
    n_count int
)
returns table (Peer1 varchar)
as $$
  begin
    return query
    select truant.peer from
    (select peer, count(peer) from timetracking
    where Date between current_date - n_count
    and current_date
    and state = 1
    group by peer) as truant
    where truant.count >= m_count;
  end;
$$ language plpgsql;

create or replace function get_month_name(month_num numeric) returns text
as $$
  declare
    month_name text;
  begin
    case month_num
      when 1 then month_name := 'january';
      when 2 then month_name := 'february';
      when 3 then month_name := 'march';
      when 4 then month_name := 'april';
      when 5 then month_name := 'may';
      when 6 then month_name := 'june';
      when 7 then month_name := 'july';
      when 8 then month_name := 'august';
      when 9 then month_name := 'september';
      when 10 then month_name := 'october';
      when 11 then month_name := 'november';
      when 12 then month_name := 'december';
      else month_name := 'invalid month number';
    end case;
    return month_name;
  end;
$$ language plpgsql;

select timetracking.peer, extract(month from timetracking.date) as entry_month, timetracking.time
from timetracking
join peers on timetracking.peer = peers.nickname
where extract(month from timetracking.date) = extract(month from peers.birthday)
and timetracking.state = 1;

create or replace function DetermiteForEachMonthThePercentageOfEarlyEntries()
returns table (Month text, EarlyEntries bigint)
language plpgsql
as $$
  begin
    drop materialized view if exists table_nickname_entry_month_time_in_birthmonth;
    create materialized view table_nickname_entry_month_time_in_birthmonth as (
      select timetracking.peer, extract(month from timetracking.date) as entry_month, timetracking.time
      from timetracking
      join peers on timetracking.peer = peers.nickname
      where extract(month from timetracking.date) = extract(month from peers.birthday)
      and timetracking.state = 1
    );

    return query
    select get_month_name(tmp.entry_month) as month, (tmp2.count * 100 / tmp.count) as early_entries_percent
    from (
      select entry_month, count(*) as count
      from table_nickname_entry_month_time_in_birthmonth
      group by entry_month
    ) as tmp
    join (
      select entry_month, count(*) as count, (count(*) * 100 / (select count(*)
      from table_nickname_entry_month_time_in_birthmonth where time <= '12:00:00')) as percent
      from table_nickname_entry_month_time_in_birthmonth where time <= '12:00:00'
      group by entry_month
    ) as tmp2 on tmp2.entry_month = tmp.entry_month;
  end;
$$;


-- Testspace

select (select count(counts) from
  (select peer, task, count(task) as counts from checks
  group by task, peer) as tb1
where tb1.task = 'Task2'
group by counts) * 100/ (select count(*) from peers);

select * from  DetermiteForEachMonthThePercentageOfEarlyEntries();

-- begin;
-- call proc_change_points('ref');
-- fetch all in "ref";
-- end;

select * from PunctualPeersFun1('19:00:00'::time, 1);

select * from PunctualPeersFun2(1, 100);

call PunctualPeers('19:00:00'::time, 1);

call PunctualPeers('19:00:00'::time, 1);
