wget -c https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/0.9.5.Final/debezium-connector-mongodb-0.9.5.Final-plugin.tar.gz
tar xzvf debezium-connector-mongodb-0.9.5.Final-plugin.tar.gz
rm -rf debezium-connector-mongodb-0.9.5.Final-plugin.tar.gz
cd ~/confluent/share/java
ln -s /var/lib/hadoop-hdfs/debezium-connector-mongodb debezium-connector-mongodb
ansible -i /etc/ansible/hosts-hadoop slave -m copy -a"src=/var/lib/hadoop-hdfs/debezium-connector-mongodb dest=/var/lib/hadoop-hdfs"
ansible -i /etc/ansible/hosts-hadoop slave -m shell -a"ln -s /var/lib/hadoop-hdfs/debezium-connector-mongodb /var/lib/hadoop-hdfs/confluent/share/java/debezium-connector-mongodb"

schema-registry-start -daemon ~/confluent/etc/schema-registry/schema-registry.properties
netstat -nlap|grep 8981
tail -f ~/confluent/logs/schema-registry.log

ansible -i /etc/ansible/hosts-hadoop slave -m copy -a"src=~/confluent/config/connect-distributed-dwsync.properties dest=~/confluent/config"
mystart_connector-distributed.sh -dwsync
ansible -i /etc/ansible/hosts-hadoop slave -m shell -a"netstat -nlap|grep 8484"
curl -H "Accept:application/json" beta-hbase02:8484/connectors/
mykill_connector_distributed.sh -dwsync

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" beta-hbase02:8484/connectors/ -d '{
    "name": "deb-mongodb-insbd-connector",
    "config":
        {
            "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
            "snapshot.mode": "initial",
            "mongodb.hosts": "dds-wz9e222b78bedd041.mongodb.rds.aliyuncs.com:3717",
            "mongodb.user": "root",
            "mongodb.password": "rootRoot!@#",
            "mongodb.name": "debmginsbd",
            "transforms": "route",
            "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.route.regex": "debmg([^.]+)\\.([^.]+)\\.([^.]+)",
            "transforms.route.replacement": "$1_datastatistic_$3",
            "database.whitelist": "datastatistic_str_2_0_3",
            "collection.whitelist": "datastatistic_str_2_0_3.mg_result_all,datastatistic_str_2_0_3.mg_result_day,datastatistic_str_2_0_3.mg_result_symall",
            "database.history.kafka.bootstrap.servers": "beta-hbase02:9092,beta-hbase03:9092,beta-hbase04:9092",
            "database.history.kafka.topic": "debmginsbd-dbhistory.debmginsbd-debezium"
        }
    }'
curl -H "Accept:application/json" beta-hbase02:8484/connectors/deb-mongodb-insbd-connector
curl -i -X DELETE -H "Accept:application/json" -H "Content-Type:application/json" beta-hbase02:8484/connectors/deb-mongodb-insbd-connector

nohup ~/confluent/bin/connect-distributed-rd ~/confluent/config/connect-distributed-KuduAvroDebMongoDB.properties 2> ~/confluent/logs/connect-distributed-KuduAvroDebMongoDB_stderr.log > ~/confluent/logs/connect-distributed-KuduAvroDebMongoDB_stdout.log &
ps -ef|grep connect-distributed
netstat -nlap|grep 8684
tail -f ~/confluent/logs/connect-distributed-KuduAvroDebMongoDB_stdout.log

curl -H "Accept:application/json" localhost:8684/connectors/

#curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" beta-hbase02:8584/connectors/ -d '{
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8684/connectors/ -d '{
    "name": "debjson-mongodb-ctbd-kudu-nativeapi-sink-connector",
    "config": {
        "connector.class": "at.grahsl.kafka.connect.kudu.KuduSinkConnector",
        "tasks.max": "3",
        "topics": "insbd_datastatistic_mg_result_all",
        "connection.uri": "beta-hbase01:7051/dw_v_0_0_1_20191111_1535",
        "change.data.capture.handler": "at.grahsl.kafka.connect.kudu.cdc.debezium.mongodb.MongoDbHandler",
        "max.num.retries": "3",
        "retries.defer.timeout": "5000",
        "delete.on.null.values": "false",
        "writemodel.strategy": "at.bronzels.kafka.connect.kudu.writemodel.strategy.ReplaceOneTimestampStrategy",
        "max.batch.size": "2000",
        "rate.limiting.timeout": "10",
        "rate.limiting.every.n": "10",
        "kudu.presto.catalog": "presto",
        "kudu.database": "dw_v_0_0_1_20191111_1535"
    }
}'
# no uppercase name for mg_result_xxx, default is false:       "src.fieldname.with.uppercase": "false",
#insbd_datastatistic_mg_result_all,insbd_datastatistic_mg_result_day,insbd_datastatistic_mg_result_symall
#        "topics": "insct_account_user_accounts,insct_copytrading_t_followorder,insct_copytrading_t_trades,insct_copytrading_t_users",

curl -H "Accept:application/json" localhost:8684/connectors/debjson-mongodb-ctbd-kudu-nativeapi-sink-connector
curl -i -X DELETE -H "Accept:application/json" -H "Content-Type:application/json" localhost:8684/connectors/debjson-mongodb-ctbd-kudu-nativeapi-sink-connector

ansible -i /etc/ansible/hosts-hadoop slave -m copy -a"src=~/confluent/config/connect-distributed-KuduAvroDebMongoDB.properties dest=~/confluent/config"

mystart_connector_distributed.sh -KuduAvroDebMongoDB
ansible -i /etc/ansible/hosts-hadoop slave -m shell -a"netstat -nlap|grep 8684"
curl -H "Accept:application/json" beta-hbase02:8684/connectors/
mykill_connector_distributed.sh -KuduAvroDebMongoDB

curl -H "Accept:application/json" beta-hbase02:8684/connectors/
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" beta-hbase02:8684/connectors/ -d '{
    "name": "debjson-mongodb-ctbd-kudu-nativeapi-sink-connector",
    "config": {
        "connector.class": "at.grahsl.kafka.connect.kudu.KuduSinkConnector",
        "tasks.max": "3",
        "topics": "insbd_datastatistic_mg_result_all",
        "connection.uri": "beta-hbase01:7051/dw_v_0_0_1_20191111_1535",
        "change.data.capture.handler": "at.grahsl.kafka.connect.kudu.cdc.debezium.mongodb.MongoDbHandler",
        "max.num.retries": "3",
        "retries.defer.timeout": "5000",
        "delete.on.null.values": "false",
        "writemodel.strategy": "at.bronzels.kafka.connect.kudu.writemodel.strategy.ReplaceOneTimestampStrategy",
        "max.batch.size": "2000",
        "rate.limiting.timeout": "10",
        "rate.limiting.every.n": "10",
        "kudu.presto.catalog": "presto",
        "kudu.database": "dw_v_0_0_1_20191108_1435"
    }
}'
# no uppercase name for mg_result_xxx, default is false:       "src.fieldname.with.uppercase": "false",
curl -H "Accept:application/json" beta-hbase02:8684/connectors/debjson-mongodb-ctbd-kudu-nativeapi-sink-connector
curl -i -X DELETE -H "Accept:application/json" -H "Content-Type:application/json" beta-hbase02:8684/connectors/debjson-mongodb-ctbd-kudu-nativeapi-sink-connector
