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
import at.bronzels.libcdcdw.kudu.pool.MyKudu;
import at.bronzels.libcdcdw.util.MyKuduTypeValue;
import at.grahsl.kafka.connect.converter.SinkDocument;
import at.bronzels.kafka.connect.kudu.cdc.CdcOperation;
import at.bronzels.libcdcdw.util.MyBson;
import at.bronzels.libcdcdw.kudu.KuduOperation;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MongoDbUpdate implements CdcOperation {
    private static Logger logger = LoggerFactory.getLogger(MongoDbUpdate.class);

    String methodSet = "$set";
    String methodUnset = "$unset";

    public static final String JSON_DOC_FIELD_PATH = "patch";

    @Override
    public Collection<Operation> perform(SinkDocument doc, MyKudu myKudu, boolean isSrcFieldNameWTUpperCase, Schema valueSchema) {
        //patch contains idempotent change only to update original document with
        BsonDocument keyDoc = doc.getKeyDoc().orElseThrow(
                () -> new DataException("error: key doc must not be missing for update operation")
        );

        BsonDocument valueDoc = doc.getValueDoc().orElseThrow(
                () -> new DataException("error: value doc must not be missing for update operation")
        );

        BsonDocument updateDoc = BsonDocument.parse(
                valueDoc.getString(JSON_DOC_FIELD_PATH).getValue()
        );
        Set<String> keySet = updateDoc.keySet();

        if (keySet.contains(methodSet))
            updateDoc = updateDoc.get(methodSet).asDocument();
        else if (keySet.contains(methodUnset)) {
            Set<String> keys2Unset = updateDoc.get(methodUnset).asDocument().keySet();
            updateDoc = new BsonDocument();
            for (String key : keys2Unset)
                updateDoc.put(key, new BsonNull());
        }
            //throw new RuntimeException(String.format("unrecognized format, keyDoc:%s, updateDoc:%s", keyDoc, updateDoc));

        List<Operation> opList = new ArrayList<>();

        Map<String, Type> newCol2TypeMap = MyKuduTypeValue.getBsonCol2Add(myKudu.getName2TypeMap(), updateDoc, isSrcFieldNameWTUpperCase);
        if(newCol2TypeMap.size() > 0){
            logger.info("col name to add : {}, type: {}", newCol2TypeMap.keySet(), newCol2TypeMap.values());
            myKudu.addColumns(newCol2TypeMap);
        }

        //patch contains full new document for replacement
        if (updateDoc.containsKey(at.bronzels.libcdcdw.Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME)) {

            BsonDocument filterDoc =
                    new BsonDocument(at.bronzels.libcdcdw.Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME,
                            updateDoc.get(at.bronzels.libcdcdw.Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME));
            Operation opDelete = KuduOperation.getOperation(OperationType.DELETE, myKudu.getKuduTable(), filterDoc, isSrcFieldNameWTUpperCase);
            if (opDelete != null)
                opList.add(opDelete);

            Operation opInsert = KuduOperation.getOperation(OperationType.CREATE, myKudu.getKuduTable(), updateDoc, isSrcFieldNameWTUpperCase);
            if (opInsert != null)
                opList.add(opInsert);

            return opList;
        }

        BsonDocument filterDoc = BsonDocument.parse(
                "{" + at.bronzels.libcdcdw.Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME +
                        ":" + keyDoc.getString(MongoDbHandler.JSON_ID_FIELD_PATH)
                        .getValue() + "}"
        );

        BsonDocument merged = MyBson.getMerged(filterDoc, updateDoc);
        Operation opUpdate = KuduOperation.getOperation(OperationType.UPDATE, myKudu.getKuduTable(), merged, isSrcFieldNameWTUpperCase);
        if (opUpdate != null)
            opList.add(opUpdate);

        return opList.isEmpty() ? null : opList;
    }

}
