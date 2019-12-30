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

package at.bronzels.kafka.connect.kudu.cdc;

import at.bronzels.libcdcdw.kudu.pool.MyKudu;
import at.grahsl.kafka.connect.converter.SinkDocument;

import org.apache.kafka.connect.data.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;

import java.util.Collection;

public interface CdcOperation {

    Collection<Operation> perform(SinkDocument doc, MyKudu myKudu, boolean isSrcFieldNameWTUpperCase, Schema valueSchema);

}
