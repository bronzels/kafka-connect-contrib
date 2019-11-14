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

package at.grahsl.kafka.connect.processor;

import at.bronzels.kafka.connect.SinkConnectorConfig;

import at.grahsl.kafka.connect.converter.SinkDocument;
import at.bronzels.kafka.connect.mongodb.id.strategy.IdStrategy;
import at.bronzels.kafka.connect.DBCollection;
import org.apache.kafka.connect.sink.SinkRecord;

public class DocumentIdAdder extends PostProcessor {

    protected final IdStrategy idStrategy;

    public DocumentIdAdder(SinkConnectorConfig config, String collection) {
        this(config,config.getIdStrategy(collection),collection);
    }

    public DocumentIdAdder(SinkConnectorConfig config, IdStrategy idStrategy, String collection) {
        super(config,collection);
        this.idStrategy = idStrategy;
    }

    @Override
    public void process(SinkDocument doc, SinkRecord orig) {
        if(idStrategy != null) {
            doc.getValueDoc().ifPresent(vd ->
                    vd.append(at.bronzels.libcdcdw.Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME, idStrategy.generateId(doc,orig))
            );
            getNext().ifPresent(pp -> pp.process(doc, orig));
        }
    }

}
