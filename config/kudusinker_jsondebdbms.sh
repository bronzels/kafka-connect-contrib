#ansible -i /etc/ansible/hosts-hadoop slave -m shell -a"nohup ~/confluent/bin/connect-distributed ~/confluent/config/connect-distributed-dwsync-json.properties 2> ~/confluent/logs/connect-distributed-dwsync_src_stderr.log > ~/confluent/logs/connect-distributed-dwsync_src_stdout.log &"
mystart_connector_distributed.sh -dwsync-json
ansible -i /etc/ansible/hosts-hadoop slave -m shell -a"netstat -nlap|grep 8384"
mykill_connector_distributed.sh -dwsync-json

tail -f ~/confluent/logs/connect-distributed-dwsync-json_stdout.log

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" beta-hbase02:8384/connectors/ -d '{ "name": "deb-mysql-ctins-connector", "config": { "connector.class": "io.debezium.connector.mysql.MySqlConnector", "tasks.max": "3", "database.hostname": "10.0.0.46", "database.port": "3333", "database.user": "liuxiangbin", "database.password": "m1njooUE04vc", "database.server.name": "debmysqlinsct", "database.whitelist": "copytrading,account", "table.whitelist": "copytrading.t_trades,copytrading.t_users,account.user_accounts,copytrading.t_followorder", "database.history.kafka.bootstrap.servers": "beta-hbase02:9092,beta-hbase03:9092,beta-hbase04:9092", "database.history.kafka.topic": "beta-dbhistory.beta-debezium.kudu", "database.history.store.only.monitored.tables.ddl":"true", "database.history.skip.unparseable.ddl":"true",
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": "debmysql([^.]+)\\.([^.]+)\\.([^.]+)",
        "transforms.route.replacement": "$1_$2_$3"
} }'
#"database.whitelist": "copytrading,account", "table.whitelist": "copytrading.t_trades,copytrading.t_users,account.user_accounts,copytrading.t_followorder",
curl -H "Accept:application/json" beta-hbase02:8384/connectors/
curl -H "Accept:application/json" beta-hbase02:8384/connectors/deb-mysql-ctins-connector
curl -i -X DELETE -H "Accept:application/json" -H "Content-Type:application/json" beta-hbase02:8384/connectors/deb-mysql-ctins-connector


nohup ~/confluent/bin/connect-distributed-rd ~/confluent/config/connect-distributed-KuduJsonDebDBMS.properties 2> ~/confluent/logs/connect-distributed-dwsync-nativeapikudu_stderr.log > ~/confluent/logs/connect-distributed-KuduJsonDebDBMS_stdout.log &
nohup ~/confluent/bin/connect-distributed ~/confluent/config/connect-distributed-KuduJsonDebDBMS.properties 2> ~/confluent/logs/connect-distributed-dwsync-nativeapikudu_stderr.log > ~/confluent/logs/connect-distributed-KuduJsonDebDBMS_stdout.log &
ansible -i /etc/ansible/hosts-hadoop slave -m copy -a"src=~/confluent/config/connect-distributed-KuduJsonDebDBMS.properties dest=~/confluent/config"

~/confluent/bin/connect-distributed-rd ~/confluent/config/connect-distributed-KuduJsonDebDBMS.properties 2> ~/confluent/logs/connect-distributed-KuduJsonDebDBMS_stderr.log > ~/confluent/logs/connect-distributed-KuduJsonDebDBMS_stdout.log
netstat -nlap|grep 8584
tail -f ~/confluent/logs/connect-distributed-KuduJsonDebDBMS_stdout.log

#curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" beta-hbase02:8584/connectors/ -d '{
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8584/connectors/ -d '{
    "name": "debjson-mysql-ctins-kudu-nativeapi-sink-connector",
    "config": {
        "connector.class": "at.bronzels.kafka.connect.kudu.KuduSinkConnector",
        "tasks.max": "3",
        "topics": "insct_copytrading_t_trades",
        "connection.uri": "beta-hbase01:7051",
        "change.data.capture.handler": "at.bronzels.kafka.connect.kudu.cdc.debezium.rdbms.mysql.MysqlHandler",
        "max.num.retries": "3",
        "retries.defer.timeout": "5000",
        "delete.on.null.values": "false",
        "writemodel.strategy": "at.bronzels.kafka.connect.kudu.writemodel.strategy.ReplaceOneTimestampStrategy",
        "max.batch.size": "2000",
        "rate.limiting.timeout": "10",
        "rate.limiting.every.n": "10",
        "src.fieldname.with.uppercase": "true",
        "kudu.presto.catalog": "presto",
        "kudu.database": "dw_v_0_0_1_20191108_1435"
    }
}'
#        "topics": "insct_copytrading_t_followorder,insct_copytrading_t_trades,insct_copytrading_t_users",
#        "topics": "insct_account_user_accounts,insct_copytrading_t_followorder,insct_copytrading_t_trades,insct_copytrading_t_users",
ps -ef|grep connect-distributed
curl -H "Accept:application/json" localhost:8584/connectors/
curl -H "Accept:application/json" localhost:8584/connectors/debjson-mysql-ctins-kudu-nativeapi-sink-connector
curl -i -X DELETE -H "Accept:application/json" -H "Content-Type:application/json" localhost:8584/connectors/debjson-mysql-ctins-kudu-nativeapi-sink-connector

mystart_connector-distributed.sh -KuduJsonDebDBMS
ansible -i /etc/ansible/hosts-hadoop slave -m shell -a"netstat -nlap|grep 8584"
mykill_connector_distributed.sh -KuduJsonDebDBMS
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" beta-hbase02:8584/connectors/ -d '{
    "name": "debjson-mysql-ctins-kudu-nativeapi-sink-connector",
    "config": {
        "connector.class": "at.bronzels.kafka.connect.kudu.KuduSinkConnector",
        "tasks.max": "3",
        "topics": "insct_copytrading_t_followorder,insct_copytrading_t_trades,insct_copytrading_t_users",
        "connection.uri": "beta-hbase01:7051",
        "change.data.capture.handler": "at.bronzels.kafka.connect.kudu.cdc.debezium.rdbms.mysql.MysqlHandler",
        "max.num.retries": "3",
        "retries.defer.timeout": "5000",
        "delete.on.null.values": "false",
        "writemodel.strategy": "at.bronzels.kafka.connect.kudu.writemodel.strategy.ReplaceOneTimestampStrategy",
        "max.batch.size": "2000",
        "rate.limiting.timeout": "10",
        "rate.limiting.every.n": "10",
        "src.fieldname.with.uppercase": "true",
        "kudu.presto.catalog": "presto",
        "kudu.database": "dw_v_0_0_1_20191108_1435"
    }
}'
# no uppercase name for mg_result_xxx, default is false:       "src.fieldname.with.uppercase": "false",
#        "topics": "insct_account_user_accounts,insct_copytrading_t_followorder,insct_copytrading_t_trades,insct_copytrading_t_users",
ansible -i /etc/ansible/hosts-hadoop slave -m shell -a"ps -ef|grep connect-distributed"
curl -H "Accept:application/json" beta-hbase02:8584/connectors/
curl -H "Accept:application/json" beta-hbase02:8584/connectors/debjson-mysql-ctins-kudu-nativeapi-sink-connector
curl -i -X DELETE -H "Accept:application/json" -H "Content-Type:application/json" beta-hbase02:8584/connectors/debjson-mysql-ctins-kudu-nativeapi-sink-connector

