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
import at.bronzels.kafka.connect.kudu.cdc.CdcOperation;
import at.bronzels.libcdcdw.kudu.pool.MyKudu;
import at.bronzels.libcdcdwstr.flink.global.MyParameterTool;
import at.bronzels.libcdcdwstr.flink.util.MyKuduTypeValue;
import at.grahsl.kafka.connect.converter.SinkDocument;
import at.bronzels.libcdcdw.kudu.KuduOperation;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class MongoDbInsert implements CdcOperation {
    private static Logger logger = LoggerFactory.getLogger(MongoDbInsert.class);

    public static final String JSON_DOC_FIELD_PATH = "after";

    @Override
    public Collection<Operation> perform(SinkDocument doc, MyKudu mykudu, boolean isSrcFieldNameWTUpperCase, Schema valueSchema) {

        BsonDocument valueDoc = doc.getValueDoc().orElseThrow(
                () -> new DataException("error: value doc must not be missing for insert operation")
        );

        BsonDocument insertDoc = BsonDocument.parse(
                valueDoc.get(JSON_DOC_FIELD_PATH).asString().getValue()
        );

        Map<String, Type> newCol2TypeMap = MyKuduTypeValue.getBsonCol2Add(mykudu.getName2TypeMap(), insertDoc, isSrcFieldNameWTUpperCase);
        if(newCol2TypeMap.size() > 0){
            logger.info("col name to add : {}, type: {}", newCol2TypeMap.keySet(), newCol2TypeMap.values());
            mykudu.addColumns(newCol2TypeMap);
        }

        Operation operation = KuduOperation.getOperation(OperationType.CREATE, mykudu.getKuduTable(), insertDoc, isSrcFieldNameWTUpperCase);
        return operation != null ? Collections.singleton(operation) : null;
    }

}
