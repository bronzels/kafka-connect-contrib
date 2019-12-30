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

package at.grahsl.kafka.connect.converter.types.sink.bson.logical;

import at.grahsl.kafka.connect.converter.SinkFieldConverter;
import org.bson.BsonInt64;
import org.bson.BsonValue;

public class DebDateFieldConverter extends SinkFieldConverter {

    public DebDateFieldConverter() {
        super(io.debezium.time.Date.builder().schema());
    }

    @Override
    public BsonValue toBson(Object data) {
        if (data != null)
            return new BsonInt64(Integer.parseInt(data.toString()));
        else
            return null;
    }
}
