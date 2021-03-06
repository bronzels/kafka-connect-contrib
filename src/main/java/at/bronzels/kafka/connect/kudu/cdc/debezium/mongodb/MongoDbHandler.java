/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package at.bronzels.kafka.connect.kudu.cdc.debezium.mongodb;

import at.bronzels.libcdcdw.OperationType;
import at.bronzels.kafka.connect.kudu.KuduSinkConnectorConfig;
import at.bronzels.kafka.connect.kudu.cdc.CdcOperation;
import at.bronzels.kafka.connect.kudu.cdc.debezium.DebeziumCdcHandler;
import at.bronzels.libcdcdw.kudu.pool.MyKudu;
import at.grahsl.kafka.connect.converter.SinkDocument;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MongoDbHandler extends DebeziumCdcHandler {

    public static final String JSON_ID_FIELD_PATH = "id";

    private static Logger logger = LoggerFactory.getLogger(MongoDbHandler.class);

    public MongoDbHandler(KuduSinkConnectorConfig config) {
        super(config);
        final Map<OperationType, CdcOperation> operations = new HashMap<>();
        operations.put(OperationType.CREATE,new MongoDbInsert());
        operations.put(OperationType.READ,new MongoDbInsert());
        operations.put(OperationType.UPDATE,new MongoDbUpdate());
        operations.put(OperationType.DELETE,new MongoDbDelete());
        registerOperations(operations);
    }

    public MongoDbHandler(KuduSinkConnectorConfig config,
                          Map<OperationType, CdcOperation> operations) {
        super(config);
        registerOperations(operations);
    }

    @Override
    public Optional<Collection<Operation>> handle(SinkDocument doc, MyKudu mykudu, Schema valueSchema) {

        BsonDocument keyDoc = doc.getKeyDoc().orElseThrow(
                () -> new DataException("error: key document must not be missing for CDC mode")
        );

        BsonDocument valueDoc = doc.getValueDoc()
                                    .orElseGet(BsonDocument::new);

        if(keyDoc.containsKey(JSON_ID_FIELD_PATH)
                && valueDoc.isEmpty()) {
            logger.debug("skipping debezium tombstone event for kafka topic compaction");
            return Optional.empty();
        }

        logger.debug("key: "+keyDoc.toString());
        logger.debug("value: "+valueDoc.toString());

        Collection<Operation> opCollection = getCdcOperation(valueDoc).perform(doc, mykudu, isSrcFieldNameWTUpperCase, valueSchema);
        if(opCollection != null)
            return Optional.of(opCollection);
        else
            return Optional.empty();
    }

}
