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

package at.bronzels.kafka.connect.mongodb.cdc.debezium.mongodb;

import at.grahsl.kafka.connect.converter.SinkDocument;
import at.bronzels.kafka.connect.mongodb.cdc.CdcOperation;
import com.mongodb.DBCollection;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

public class MongoDbUpdate implements CdcOperation {

    public static final String JSON_DOC_FIELD_PATH = "patch";

    private static final UpdateOptions UPDATE_OPTIONS =
            new UpdateOptions().upsert(true);

    @Override
    public WriteModel<BsonDocument> perform(SinkDocument doc) {

        BsonDocument valueDoc = doc.getValueDoc().orElseThrow(
                () -> new DataException("error: value doc must not be missing for update operation")
        );

        try {

            BsonDocument updateDoc = BsonDocument.parse(
                    valueDoc.getString(JSON_DOC_FIELD_PATH).getValue()
            );

            if(updateDoc.containsKey("$v"))
                updateDoc.remove("$v");
            //patch contains full new document for replacement
            if(updateDoc.containsKey(at.bronzels.libcdcdw.Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME)) {
                BsonDocument filterDoc =
                        new BsonDocument(at.bronzels.libcdcdw.Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME,
                                updateDoc.get(at.bronzels.libcdcdw.Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME));
                return new ReplaceOneModel<>(filterDoc, updateDoc, UPDATE_OPTIONS);
            }

            //patch contains idempotent change only to update original document with
            BsonDocument keyDoc = doc.getKeyDoc().orElseThrow(
                    () -> new DataException("error: key doc must not be missing for update operation")
            );

            BsonDocument filterDoc = BsonDocument.parse(
                            "{"+at.bronzels.libcdcdw.Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME+
                                    ":"+keyDoc.getString(MongoDbHandler.JSON_ID_FIELD_PATH)
                                    .getValue()+"}"
            );

            return new UpdateOneModel<>(filterDoc, updateDoc);

        } catch (DataException exc) {
            exc.printStackTrace();
            throw exc;
        }
        catch (Exception exc) {
            exc.printStackTrace();
            throw new DataException(exc.getMessage(),exc);
        }

    }

}
