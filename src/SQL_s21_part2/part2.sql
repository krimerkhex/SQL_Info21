create or replace procedure CheckP2P(
    checked varchar,
    checkers varchar,
    taskName varchar,
    status Status,
    timecurrent date
)
language plpgsql
as
    $$
  declare
    lastCheckId int;
    currentTime time;
  begin
    if status = 'Start' then
      insert into Checks (Peer, Task, Date)
      values (checked, taskName, current_date);
      select LASTVAL() into lastCheckId;
    else
      select checks.id into lastCheckId
      from p2p
      join checks on p2p.checkid = checks.id
      where p2p.checkingpeer = checkers
        and checks.peer = checked
        and checks.task = taskName
      order by checks.id
      limit 1;
    end if;

    currentTime := time '00:00:00' + timecurrent;
    insert into P2P (CheckId, CheckingPeer, Status, Time)
    values (lastCheckId, checkers, status, currentTime);
  end;
$$;

create or replace procedure CheckVerter(
    nickname varchar,
    taskName varchar,
    state status,
    timecurrent time
)
language plpgsql
as
    $$
    declare
        lastCheckID int;
  begin
    lastCheckID := (select c.id from P2P
      join Checks c on c.id = p2p.checkid
      where p2p.checkingpeer = nickname and c.task = taskName
      order by P2P.Time desc
      limit 1);
    insert into Verter(checkid, status, Time)
    values (lastCheckID, state, timecurrent);
  end;
$$;

create or replace function ValidateP2P()
  returns trigger
language plpgsql
as
    $$
  begin
    if new.status = 'Start' then
      update TransferredPoints tr
      set PointsAmount = 1
      where new.checkingpeer = tr.checkingpeer;
    end if;
    return new;
  end;
$$;

create trigger AfterInsertP2P after insert on p2p
for each row execute procedure ValidateP2P();

create or replace function ValidateXP()
  returns trigger
language plpgsql
as
    $$
  declare
    maxXP int;
    status status;
  begin
    maxXP = (select t.MaxXP from xp
      join checks c on c.id = xp.checkid
      join tasks t on c.task = t.title
      limit 1);
    status = (select v.status from xp
      join checks c2 on c2.id = xp.checkid
      join verter v on c2.id = v.checkid
      limit 1);
    if (new.xpamount <= maxXP and status = 'Success') then
      return new;
    else
      return null;
    end if;
  end;
$$;

create trigger BeforeInsertXP before insert on xp
for each row Execute procedure ValidateXP();


-- Testspace


call CheckP2P('grandpat', 'jerlenem', 'CPP3_SmartCalc_v2.0', 'Start', current_date);
SELECT * FROM checks;
SELECT * FROM p2p;
call CheckVerter('jerlenem', 'CPP3_SmartCalc_v2.0', 'Success', '10:00:00');
insert into P2P(CheckId, CheckingPeer, status, time) values(2, 'jerlenem', 'Start', '14:00:00');
insert into XP (CheckId, XPAmount) values (2, 3);
