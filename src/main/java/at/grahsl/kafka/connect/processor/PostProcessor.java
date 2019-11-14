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
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Optional;

public abstract class PostProcessor {

    private final SinkConnectorConfig config;
    private Optional<PostProcessor> next = Optional.empty();
    private final String collection;

    public PostProcessor(SinkConnectorConfig config, String collection) {
        this.config = config;
        this.collection = collection;
    }

    public PostProcessor chain(PostProcessor next) {
        // intentionally throws NPE here if someone
        // tries to be 'smart' by chaining with null
        this.next = Optional.of(next);
        return this.next.get();
    }

    public abstract void process(SinkDocument doc, SinkRecord orig);

    public SinkConnectorConfig getConfig() {
        return this.config;
    }

    public Optional<PostProcessor> getNext() {
        return this.next;
    }

    public String getCollection() {
        return this.collection;
    }

}
