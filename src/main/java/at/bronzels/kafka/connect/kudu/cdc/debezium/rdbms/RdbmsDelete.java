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
import at.bronzels.kafka.connect.kudu.cdc.CdcOperation;
import at.bronzels.libcdcdw.kudu.KuduOperation;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;

import org.bson.BsonDocument;

import java.util.Collection;
import java.util.Collections;

public class RdbmsDelete implements CdcOperation {

    @Override
    public Collection<Operation> perform(SinkDocument doc, KuduTable collection, KuduClient kuduClient, boolean isSrcFieldNameWTUpperCase, Schema valueSchema) {

        BsonDocument keyDoc = doc.getKeyDoc().orElseThrow(
                () -> new DataException("error: key doc must not be missing for delete operation")
        );

        BsonDocument valueDoc = doc.getValueDoc().orElseThrow(
                () -> new DataException("error: value doc must not be missing for delete operation")
        );

        try {
            BsonDocument filterDoc = RdbmsHandler.generateDeleteFilterDoc(keyDoc, valueDoc, OperationType.DELETE);
            Operation operation = KuduOperation.getOperation(OperationType.DELETE, collection, filterDoc, isSrcFieldNameWTUpperCase);
            return operation != null ? Collections.singleton(operation) : null;
        } catch(Exception exc) {
            throw new DataException(exc);
        }

    }

}
