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

package at.bronzels.kafka.connect.kudu.cdc.debezium.rdbms;

import at.bronzels.libcdcdw.OperationType;
import at.grahsl.kafka.connect.converter.SinkDocument;
import at.bronzels.kafka.connect.kudu.KuduSinkConnectorConfig;
import at.bronzels.kafka.connect.kudu.cdc.CdcOperation;
import at.bronzels.kafka.connect.kudu.cdc.debezium.DebeziumCdcHandler;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.bson.BsonDocument;
import org.bson.BsonInvalidOperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RdbmsHandler extends DebeziumCdcHandler {

    public static final String JSON_DOC_BEFORE_FIELD = "before";
    public static final String JSON_DOC_AFTER_FIELD = "after";

    private static Logger logger = LoggerFactory.getLogger(RdbmsHandler.class);

    public RdbmsHandler(KuduSinkConnectorConfig config) {
        super(config);
        final Map<OperationType, CdcOperation> operations = new HashMap<>();
        operations.put(OperationType.CREATE,new RdbmsInsert());
        operations.put(OperationType.READ,new RdbmsInsert());
        operations.put(OperationType.UPDATE,new RdbmsUpdate());
        operations.put(OperationType.DELETE,new RdbmsDelete());
        registerOperations(operations);
    }

    public RdbmsHandler(KuduSinkConnectorConfig config,
                        Map<OperationType, CdcOperation> operations) {
        super(config);
        registerOperations(operations);
    }

    @Override
    public Optional<Collection<Operation>> handle(SinkDocument doc, KuduTable collection, KuduClient kuduClient, Schema valueSchema) {

        BsonDocument keyDoc = doc.getKeyDoc().orElseGet(BsonDocument::new);

        BsonDocument valueDoc = doc.getValueDoc().orElseGet(BsonDocument::new);

        if (valueDoc.isEmpty())  {
            logger.debug("skipping debezium tombstone event for kafka topic compaction");
            return Optional.empty();
        }

        return Optional.of(getCdcOperation(valueDoc)
                            .perform(new SinkDocument(keyDoc,valueDoc), collection, kuduClient, isSrcFieldNameWTUpperCase, valueSchema));
    }

    protected static BsonDocument generateDeleteFilterDoc(BsonDocument keyDoc, BsonDocument valueDoc, OperationType opType) {
        if (keyDoc.keySet().isEmpty()) {
            //update or delete: no PK info in keyDoc -> take everything in 'before' field
            try {
                BsonDocument filter = valueDoc.getDocument(JSON_DOC_BEFORE_FIELD);
                if (filter.isEmpty())
                    throw new BsonInvalidOperationException("value doc before field is empty");
                return filter;
            } catch(BsonInvalidOperationException exc) {
                throw new DataException("error: value doc 'before' field is empty or has invalid type" +
                        " for update/delete operation which seems severely wrong -> defensive actions taken!",exc);
            }
        }
        //build filter document composed of all PK columns
        BsonDocument pk = new BsonDocument();
        for (String f : keyDoc.keySet()) {
            pk.put(f,keyDoc.get(f));
        }
        return pk;
    }

    protected static BsonDocument generateUpsertOrReplaceDoc(BsonDocument valueDoc) {

        if (!valueDoc.containsKey(JSON_DOC_AFTER_FIELD)
                || valueDoc.get(JSON_DOC_AFTER_FIELD).isNull()
                || !valueDoc.get(JSON_DOC_AFTER_FIELD).isDocument()
                || valueDoc.getDocument(JSON_DOC_AFTER_FIELD).isEmpty()) {
            throw new DataException("error: valueDoc must contain non-empty 'after' field" +
                    " of type document for insert/update operation");
        }

        BsonDocument afterDoc = valueDoc.getDocument(JSON_DOC_AFTER_FIELD);

        return afterDoc;
    }

}
