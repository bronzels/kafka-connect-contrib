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
import at.bronzels.libcdcdw.kudu.pool.MyKudu;
import at.bronzels.libcdcdwstr.flink.util.MyKuduTypeValue;
import at.grahsl.kafka.connect.converter.SinkDocument;
import at.bronzels.kafka.connect.kudu.cdc.CdcOperation;
import at.bronzels.libcdcdw.kudu.KuduOperation;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kudu.Type;
import org.apache.kudu.client.Operation;

import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class RdbmsInsert implements CdcOperation {

    private static Logger logger = LoggerFactory.getLogger(RdbmsInsert.class);

    @Override
    public Collection<Operation> perform(SinkDocument doc, MyKudu myKudu, boolean isSrcFieldNameWTUpperCase, Schema valueSchema) {

        BsonDocument valueDoc = doc.getValueDoc().orElseThrow(
                () -> new DataException("error: value doc must not be missing for insert operation")
        );

        try {
            BsonDocument insertDoc = RdbmsHandler.generateUpsertOrReplaceDoc(valueDoc);

            /*Map<String, Type> newCol2TypeMap = MyKuduTypeValue.getBsonCol2Add(myKudu.getName2TypeMap(), insertDoc, isSrcFieldNameWTUpperCase);
            if(newCol2TypeMap.size() > 0){
                logger.info("col name to add : {}, type: {}", newCol2TypeMap.keySet(), newCol2TypeMap.values());
                myKudu.addColumns(newCol2TypeMap);
            }*/
            Operation operation = KuduOperation.getOperation(OperationType.CREATE, myKudu.getKuduTable(), insertDoc, isSrcFieldNameWTUpperCase);
            return operation != null ? Collections.singleton(operation) : null;
        } catch (Exception exc) {
            throw new DataException(exc);
        }

    }

}
