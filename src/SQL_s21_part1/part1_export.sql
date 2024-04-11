create or replace procedure Export(tableName varchar, filePath varchar, delimiter varchar)
as $$
  begin
    execute format('copy %s TO ''%s'' DELIMITER ''%s'' CSV HEADER;',
    tableName, filePath, delimiter);
  end;
$$ language plpgsql;

create EXTENSION if not exists pgcrypto;

CALL Export('peers', '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/checks.csvpeers.csv', ',');
CALL  Export('tasks',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/checks.csvtasks.csv', ',');
CALL  Export('checks',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/checks.csvchecks.csv', ',');
CALL  Export('p2p',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/checks.csvp2p.csv', ',');
CALL  Export('verter',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/checks.csvverter.csv', ',');
CALL  Export('TransferredPoints',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/checks.csvtransferred_points.csv', ',');
CALL  Export('Friends',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/checks.csvfriends.csv', ',');
CALL  Export('Recommendations',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/checks.csvrecommendations.csv', ',');
CALL  Export('XP',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/checks.csvxp.csv', ',');
CALL  Export('TimeTracking',  '/opt/goinfre/jerlenem/SQL2_Info21_v1.0-1/src/SQL_s21_part1/data/checks.csvtime_tracking.csv', ',');


