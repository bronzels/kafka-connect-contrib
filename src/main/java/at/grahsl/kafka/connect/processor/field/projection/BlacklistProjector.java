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

package at.grahsl.kafka.connect.processor.field.projection;

import at.bronzels.kafka.connect.SinkConnectorConfig;

import at.bronzels.kafka.connect.DBCollection;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public abstract class BlacklistProjector extends FieldProjector {

    public BlacklistProjector(SinkConnectorConfig config, String collection) {
        this(config,config.getValueProjectionList(collection),collection);
    }

    public BlacklistProjector(SinkConnectorConfig config,
                              Set<String> fields, String collection) {
        super(config,collection);
        this.fields = fields;
    }

    @Override
    protected void doProjection(String field, BsonDocument doc) {

        if(!field.contains(SUB_FIELD_DOT_SEPARATOR)) {

            if(field.equals(SINGLE_WILDCARD)
                    || field.equals(DOUBLE_WILDCARD)) {
                handleWildcard(field,"",doc);
                return;
            }

            //NOTE: never try to remove the _id field
            if(!field.equals(at.bronzels.libcdcdw.Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME))
                doc.remove(field);

            return;
        }

        int dotIdx = field.indexOf(SUB_FIELD_DOT_SEPARATOR);
        String firstPart = field.substring(0,dotIdx);
        String otherParts = field.length() >= dotIdx
                                ? field.substring(dotIdx+1) : "";

        if(firstPart.equals(SINGLE_WILDCARD)
            || firstPart.equals(DOUBLE_WILDCARD)) {
            handleWildcard(firstPart,otherParts,doc);
            return;
        }

        BsonValue value = doc.get(firstPart);
        if(value != null) {
            if(value.isDocument()) {
                doProjection(otherParts, (BsonDocument)value);
            }
            if(value.isArray()) {
                BsonArray values = (BsonArray)value;
                for(BsonValue v : values.getValues()) {
                    if(v != null && v.isDocument()) {
                        doProjection(otherParts,(BsonDocument)v);
                    }
                }
            }
        }

    }

    private void handleWildcard(String firstPart, String otherParts, BsonDocument doc) {
        Iterator<Map.Entry<String, BsonValue>> iter = doc.entrySet().iterator();
        while(iter.hasNext()) {
            Map.Entry<String, BsonValue> entry = iter.next();
            BsonValue value = entry.getValue();

            //NOTE: never try to remove the _id field
            if(entry.getKey().equals(at.bronzels.libcdcdw.Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME))
                continue;

            if(firstPart.equals(DOUBLE_WILDCARD)) {
                iter.remove();
            }

            if(firstPart.equals(SINGLE_WILDCARD)) {
                if(!value.isDocument()) {
                    iter.remove();
                } else {
                    if(!otherParts.isEmpty()) {
                        doProjection(otherParts, (BsonDocument)value);
                    }
                }
            }
        }
    }

}
