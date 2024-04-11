create or replace procedure Import(tableName varchar, filePath varchar, delimiter varchar)
as
    $$
  begin
    execute format('COPY %s FROM ''%s'' DELIMITER ''%s'' CSV HEADER;',
    tableName, filePath, delimiter);
  end;
$$ language plpgsql;

CALL Import('peers', '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/peers.csv', ',');
CALL  Import('tasks',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/tasks.csv', ',');
CALL  Import('checks',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/checks.csv', ',');
CALL  Import('p2p',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/p2p.csv', ',');
CALL  Import('verter',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/verter.csv', ',');
CALL  Import('TransferredPoints',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/transferred_points.csv', ',');
CALL  Import('Friends',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/friends.csv', ',');
CALL  Import('Recommendations',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/recommendations.csv', ',');
CALL  Import('XP',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/xp.csv', ',');
CALL  Import('TimeTracking',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/time_tracking.csv', ',');
