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

package at.bronzels.kafka.connect.kudu;

import at.bronzels.kafka.connect.DBCollection;
import at.bronzels.libcdcdw.Constants;
import at.grahsl.kafka.connect.SinkRecordBatches;
import at.grahsl.kafka.connect.VersionUtil;
import at.grahsl.kafka.connect.converter.SinkConverter;
import at.grahsl.kafka.connect.processor.PostProcessor;
import at.grahsl.kafka.connect.converter.SinkDocument;
import at.bronzels.kafka.connect.kudu.cdc.CdcHandler;
import at.bronzels.kafka.connect.kudu.writemodel.strategy.WriteModelStrategy;

import io.vavr.Tuple2;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kudu.client.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KuduSinkTask extends SinkTask {

    private static Logger LOGGER = LoggerFactory.getLogger(KuduSinkTask.class);

    private KuduSinkConnectorConfig sinkConfig;
    private KuduClient kuduClient;
    private KuduSession kuduSession;
    private String prestoCatalog;
    private String database;
    private int remainingRetries;
    private int deferRetryMs;

    private Map<String, PostProcessor> processorChains;
    private Map<String, CdcHandler> cdcHandlers;
    private Map<String, WriteModelStrategy> writeModelStrategies;
    private Map<String, KuduSinkConnectorConfig.RateLimitSettings> rateLimitSettings;

    private Map<String, WriteModelStrategy> deleteOneModelDefaultStrategies;

    private Map<String, KuduTable> cachedCollections = new HashMap<>();

    private SinkConverter sinkConverter = new SinkConverter();

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("starting Kudu sink task");

        if(!props.containsKey(KuduSinkConnectorConfig.COLLECTIONS_CONF)) {
            props.put(KuduSinkConnectorConfig.COLLECTIONS_CONF, props.get(DBCollection.CONFLUENT_COMMON_CONFIG_ITEM_TOPICS));
        }
        sinkConfig = new KuduSinkConnectorConfig(props);

        String host = sinkConfig.buildClientURI();
        prestoCatalog = sinkConfig.getKuduPrestoCatalog();
        database = sinkConfig.getKuduDatabase();
        kuduClient = new KuduClient.KuduClientBuilder(host).defaultAdminOperationTimeoutMs(600000).build();
        kuduSession = kuduClient.newSession();
        kuduSession.setTimeoutMillis(60000);
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        kuduSession.setMutationBufferSpace(10000);

        remainingRetries = sinkConfig.getInt(
                KuduSinkConnectorConfig.MAX_NUM_RETRIES_CONF);
        deferRetryMs = sinkConfig.getInt(
                KuduSinkConnectorConfig.RETRIES_DEFER_TIMEOUT_CONF);

        processorChains = sinkConfig.buildPostProcessorChains();
        cdcHandlers = sinkConfig.getCdcHandlers();

        writeModelStrategies = sinkConfig.getWriteModelStrategies();
        rateLimitSettings = sinkConfig.getRateLimitSettings();
        deleteOneModelDefaultStrategies = sinkConfig.getDeleteOneModelDefaultStrategies();

    }

    static private String getCollectionName(KuduTable collection) {
        return StringUtils.substringAfterLast(collection.getName(), KuduSinkConnectorConfig.NAMESPACE_SEPARATOR);
    }

    @Override
    public void put(Collection<SinkRecord> records) {

        if (records.isEmpty()) {
            LOGGER.debug("no sink records to process for current poll operation");
            return;
        }

        Map<String, SinkRecordBatches> batchMapping = createSinkRecordBatchesPerTopic(records);

        batchMapping.forEach((namespace, batches) -> {

            String collection = StringUtils.substringAfter(namespace,
                    KuduSinkConnectorConfig.NAMESPACE_SEPARATOR);
                    //DBCollection.KUDU_TABLE_NAME_SCHEMA_PREFIX_SEP);

            batches.getBufferedBatches().forEach(batch -> {
                        processSinkRecords(cachedCollections.get(namespace), batch);
                        KuduSinkConnectorConfig.RateLimitSettings rls =
                                rateLimitSettings.getOrDefault(collection,
                                        rateLimitSettings.get(KuduSinkConnectorConfig.TOPIC_AGNOSTIC_KEY_NAME));
                        if (rls.isTriggered()) {
                            LOGGER.debug("rate limit settings triggering {}ms defer timeout"
                                            + " after processing {} further batches for collection {}",
                                    rls.getTimeoutMs(), rls.getEveryN(), collection);
                            try {
                                Thread.sleep(rls.getTimeoutMs());
                            } catch (InterruptedException e) {
                                LOGGER.error(e.getMessage());
                            }
                        }
                    }
            );
        });

    }

    private void processSinkRecords(KuduTable collection, List<SinkRecord> batch) {
        String collectionName = getCollectionName(collection);
        List<? extends Collection<Operation>> docsToWrite =
                sinkConfig.isUsingCdcHandler(collectionName)
                        ? buildWriteModelCDC(batch, collection, collectionName)
                        : buildWriteModel(batch, collection, collectionName);
        try {
            if (!docsToWrite.isEmpty()) {
                LOGGER.debug("bulk writing {} document(s) into collection [{}]",
                        docsToWrite.size(), collection.getName());
                for(Collection<Operation> opCols: docsToWrite) {
                    for(Operation op: opCols) {
                        kuduSession.apply(op);
                    }
                }
                List<OperationResponse> resp = kuduSession.flush();
                LOGGER.debug("kudu flush result: " + resp);
            }
        } catch (KuduException kuduexc) {
            /*if (kuduexc instanceof KuduException) {
            } else*/ {
                LOGGER.error("error on kudu operation", kuduexc);
                LOGGER.error("writing {} document(s) into collection [{}] failed -> remaining retries ({})",
                        docsToWrite.size(), collection.getName(), remainingRetries);
            }
            if (remainingRetries-- <= 0) {
                throw new ConnectException("failed to write mongodb documents"
                        + " despite retrying -> GIVING UP! :( :( :(", kuduexc);
            }
            LOGGER.debug("deferring retry operation for {}ms", deferRetryMs);
            context.timeout(deferRetryMs);
            throw new RetriableException(kuduexc.getMessage(), kuduexc);
        }
    }

    Map<String, SinkRecordBatches> createSinkRecordBatchesPerTopic(Collection<SinkRecord> records) {
        LOGGER.debug("number of sink records to process: {}", records.size());

        Map<String, SinkRecordBatches> batchMapping = new HashMap<>();
        LOGGER.debug("buffering sink records into grouped topic batches");
        records.forEach(r -> {
            String collection = sinkConfig.getString(KuduSinkConnectorConfig.COLLECTION_CONF, r.topic());
            if (collection.isEmpty()) {
                LOGGER.debug("no explicit collection name mapping found for topic {} "
                        + "and default collection name was empty ", r.topic());
                LOGGER.debug("using topic name {} as collection name", r.topic());
                collection = r.topic();
            }
            String namespace = database + KuduSinkConnectorConfig.NAMESPACE_SEPARATOR + collection;
            //String namespace = database + DBCollection.KUDU_TABLE_NAME_SCHEMA_PREFIX_SEP + collection;
            if(!cachedCollections.containsKey(namespace)) {
                try {
                    KuduTable kuduTable = kuduClient.openTable(prestoCatalog + Constants.KUDU_TABLE_NAME_AFTER_CATALOG_SEP + namespace);
                    cachedCollections.put(namespace, kuduTable);
                } catch (KuduException e) {
                    e.printStackTrace();
                }
            }

            SinkRecordBatches batches = batchMapping.get(namespace);

            if (batches == null) {
                int maxBatchSize = sinkConfig.getInt(KuduSinkConnectorConfig.MAX_BATCH_SIZE, collection);
                LOGGER.debug("batch size for collection {} is at most {} record(s)", collection, maxBatchSize);
                batches = new SinkRecordBatches(maxBatchSize, records.size());
                batchMapping.put(namespace, batches);
            }
            batches.buffer(r);
        });
        return batchMapping;
    }

    List<? extends Collection<Operation>>
    buildWriteModel(Collection<SinkRecord> records, KuduTable collection, String collectionName) {
        List<Collection<Operation>> docsToWrite = new ArrayList<>(records.size());
        LOGGER.debug("building write model for {} record(s)", records.size());
        records.forEach(record -> {
                    SinkDocument doc = sinkConverter.convert(record);
                    processorChains.getOrDefault(collectionName,
                            processorChains.get(KuduSinkConnectorConfig.TOPIC_AGNOSTIC_KEY_NAME))
                            .process(doc, record);
                    if (doc.getValueDoc().isPresent()) {
                        docsToWrite.add(Collections.singleton(writeModelStrategies.getOrDefault(
                                collectionName, writeModelStrategies.get(KuduSinkConnectorConfig.TOPIC_AGNOSTIC_KEY_NAME)
                                ).createWriteModel(doc, collection, record.valueSchema())
                        ));
                    } else {
                        if (doc.getKeyDoc().isPresent()
                                && sinkConfig.isDeleteOnNullValues(record.topic())) {
                            docsToWrite.add(Collections.singleton(deleteOneModelDefaultStrategies.getOrDefault(collectionName,
                                    deleteOneModelDefaultStrategies.get(KuduSinkConnectorConfig.TOPIC_AGNOSTIC_KEY_NAME))
                                    .createWriteModel(doc, collection, record.valueSchema())
                            ));
                        } else {
                            LOGGER.error("skipping sink record " + record + "for which neither key doc nor value doc were present");
                        }
                    }
                }
        );

        return docsToWrite;
    }

    List<? extends Collection<Operation>>
    buildWriteModelCDC(Collection<SinkRecord> records, KuduTable collection, String collectionName) {
        LOGGER.debug("building CDC write model for {} record(s) into collection {}", records.size(), collection.getName());
        return records.stream()
                .map(sinkRecord -> new Tuple2<SinkDocument, Schema>(sinkConverter.convert(sinkRecord), sinkRecord.valueSchema()))
                .map(tuple2 ->
                    cdcHandlers.get(collectionName).handle(tuple2._1, collection, kuduClient, tuple2._2)
                )
                //.filter(Objects::nonNull)
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toList());

    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        //NOTE: flush is not used for now...
    }

    @Override
    public void stop() {
        LOGGER.info("stopping Kudu sink task");
        try {
            if(kuduSession != null)
                kuduSession.close();
            if(kuduClient != null)
                kuduClient.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

}
