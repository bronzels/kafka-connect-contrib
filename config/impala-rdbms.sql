CREATE EXTERNAL TABLE insct_copytrading_t_trades
STORED AS KUDU
TBLPROPERTIES (
  'kudu.table_name' = 'presto::dw_v_0_0_1_20191108_1435.insct_copytrading_t_trades'
);
CREATE EXTERNAL TABLE insct_account_user_accounts
STORED AS KUDU
TBLPROPERTIES (
  'kudu.table_name' = 'presto::dw_v_0_0_1_20191108_1435.insct_account_user_accounts'
);
CREATE EXTERNAL TABLE insct_copytrading_t_users
STORED AS KUDU
TBLPROPERTIES (
  'kudu.table_name' = 'presto::dw_v_0_0_1_20191108_1435.insct_copytrading_t_users'
);
CREATE EXTERNAL TABLE insct_copytrading_t_followorder
STORED AS KUDU
TBLPROPERTIES (
  'kudu.table_name' = 'presto::dw_v_0_0_1_20191108_1435.insct_copytrading_t_followorder'
);

USE dw_v_0_0_1_20191108_1435;

DELETE FROM insct_copytrading_t_trades;
DELETE FROM insct_copytrading_t_users;
DELETE FROM insct_copytrading_t_followorder;

SELECT COUNT(1) FROM insct_copytrading_t_trades;
SELECT COUNT(1) FROM insct_copytrading_t_users;
SELECT COUNT(1) FROM insct_copytrading_t_followorder;

SELECT * FROM insct_copytrading_t_trades LIMIT 5;
SELECT * FROM insct_copytrading_t_users LIMIT 5;
SELECT * FROM insct_copytrading_t_followorder LIMIT 5;

insct_copytrading_t_trades
2019-11-08 18:47:12,2109304
2019-11-08 18:49:07,3235956
(3235956-2109304)/115=9796.973913043 record/s