nohup confluent/bin/connect-distributed-rd confluent/config/connect-distributed-bson2kudu.properties 2> confluent/logs/connect-distributed_bson2kudu_stderr.log > confluent/logs/connect-distributed_bson2kudu_stdout.log &

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8984/connectors/ -d '{
    "name": "lxb-bsondoc-tsdb-all-kudu-nativeapi-sink-connector10",
    "config": {
        "connector.class": "at.bronzels.kafka.connect.kudu.KuduSinkConnector",
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": "([^.]+)result([^.]+)",
        "transforms.route.replacement": "result$2",
        "tasks.max": "3",
        "topics": "2_2_1_0__result_all,2_2_1_0__result_all_bizid,2_2_1_0__result_symall,2_2_1_0__result_symall_bizid",
        "connection.uri": "beta-hbase01:7052",
        "max.num.retries": "3",
        "retries.defer.timeout": "5000",
        "delete.on.null.values": "false",
        "writemodel.strategy": "at.bronzels.kafka.connect.kudu.writemodel.strategy.ReplaceOneTimestampStrategy",
        "max.batch.size": "2000",
        "rate.limiting.timeout": "10",
        "rate.limiting.every.n": "10",
        "src.fieldname.with.uppercase": "false",
        "kudu.presto.catalog": "dw_v_0_0_1_20191223_1830",
        "kudu.database": "tsdb"
    }
}'

INSERT INTO result_all_swp SELECT brokerid,login,CAST(bizid AS BIGINT),seriests,deposit,withdraw,money_followed_close_actual,money_cf,point_cf,standardlots_cf,deal_cf,money_close,deal_close,point_close,standardlots_close,money_cs,point_cs,standardlots_cs,deal_cs,_dwsyncts FROM result_all;
INSERT INTO result_all_bizid_swp SELECT brokerid,login,CAST(bizid AS BIGINT),seriests,deposit,withdraw,money_followed_close_actual,money_cf,point_cf,standardlots_cf,deal_cf,money_close,deal_close,point_close,standardlots_close,money_cs,point_cs,standardlots_cs,deal_cs,_dwsyncts  FROM result_all_bizid ;
INSERT INTO result_symall_bizid_swp SELECT brokerid,login,standardsymbol,CAST(bizid AS BIGINT),seriests,money_profit_short_close,_dwsyncts FROM result_symall_bizid;
INSERT INTO result_symall_swp SELECT brokerid,login,standardsymbol,seriests,CAST(bizid AS BIGINT),money_profit_short_close,_dwsyncts FROM result_symall;

curl -H "Accept:application/json" beta-hbase01:8984/connectors/
curl -H "Accept:application/json" beta-hbase01:8984/connectors/lxb-bsondoc-tsdb-all-kudu-nativeapi-sink-connector10
curl -H "Accept:application/json" beta-hbase01:8984/connectors/lxb-bsondoc-tsdb-all-kudu-nativeapi-sink-connector10/status
curl -i -X DELETE -H "Accept:application/json" -H "Content-Type:application/json" localhost:8984/connectors/lxb-bsondoc-tsdb-all-kudu-nativeapi-sink-connector10

CREATE TABLE kudu.bd.datastatistic_tsdb_lxb_result_all_bizid (
    brokerid integer WITH ( primary_key = true ),
    login varchar WITH ( primary_key = true ),
    bizid bigint WITH ( primary_key = true ),
    seriests bigint WITH ( nullable = true ),
    deal_cf bigint WITH ( nullable = true ),
    deal_close bigint WITH ( nullable = true ),
    deal_cs bigint WITH ( nullable = true ),
    deposit double WITH ( nullable = true ),
    money_cf double WITH ( nullable = true ),
    money_close double WITH ( nullable = true ),
    money_cs double WITH ( nullable = true ),
    money_followed_close_actual double WITH ( nullable = true ),
    point_cf double WITH ( nullable = true ),
    point_close double WITH ( nullable = true ),
    point_cs double WITH ( nullable = true ),
    standardlots_cf double WITH ( nullable = true ),
    standardlots_close double WITH ( nullable = true ),
    standardlots_cs double WITH ( nullable = true ),
    withdraw double WITH ( nullable = true ),
    _dwsyncts bigint WITH ( nullable = true )
 )
 WITH (
    number_of_replicas = 3,
    partition_by_hash_buckets = 15,
    partition_by_hash_columns = ARRAY['brokerid','login','bizid'],
    partition_by_range_columns = ARRAY['brokerid','login','bizid'],
    range_partitions = '[{"lower":null,"upper":null}]'
 );

CREATE TABLE kudu.bd.datastatistic_tsdb_lxb_result_all (
    brokerid integer WITH ( primary_key = true ),
    login varchar WITH ( primary_key = true ),
    seriests bigint WITH ( primary_key = true ),
    bizid bigint WITH ( nullable = true ),
    deal_cf bigint WITH ( nullable = true ),
    deal_close bigint WITH ( nullable = true ),
b4str有 流处理无    deal_cs bigint WITH ( nullable = true ),
b4str有 流处理无   deposit double WITH ( nullable = true ),
    money_cf double WITH ( nullable = true ),
    money_close double WITH ( nullable = true ),
    money_cs double WITH ( nullable = true ),
    money_followed_close_actual double WITH ( nullable = true ),
    point_cf double WITH ( nullable = true ),
    point_close double WITH ( nullable = true ),
    point_cs double WITH ( nullable = true ),
    standardlots_cf double WITH ( nullable = true ),
    standardlots_close double WITH ( nullable = true ),
    standardlots_cs double WITH ( nullable = true ),
b4str有 流处理无    withdraw double WITH ( nullable = true ),
    _dwsyncts bigint WITH ( nullable = true )
 )
 WITH (
    number_of_replicas = 3,
    partition_by_hash_buckets = 15,
    partition_by_hash_columns = ARRAY['brokerid','login','seriests'],
    partition_by_range_columns = ARRAY['brokerid','login','seriests'],
    range_partitions = '[{"lower":null,"upper":null}]'
 );

