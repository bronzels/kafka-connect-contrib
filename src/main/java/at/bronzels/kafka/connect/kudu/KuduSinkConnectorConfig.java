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

import at.bronzels.kafka.connect.SinkConnectorConfig;
import at.bronzels.kafka.connect.mongodb.id.strategy.IdStrategy;
import at.bronzels.kafka.connect.kudu.cdc.CdcHandler;
import at.bronzels.kafka.connect.kudu.cdc.debezium.mongodb.MongoDbHandler;
import at.bronzels.kafka.connect.kudu.cdc.debezium.rdbms.RdbmsHandler;
import at.bronzels.kafka.connect.kudu.cdc.debezium.rdbms.mysql.MysqlHandler;
import at.bronzels.kafka.connect.kudu.cdc.debezium.rdbms.postgres.PostgresHandler;
import at.bronzels.kafka.connect.kudu.writemodel.strategy.DeleteOneDefaultStrategy;
import at.bronzels.kafka.connect.kudu.writemodel.strategy.WriteModelStrategy;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class KuduSinkConnectorConfig extends SinkConnectorConfig {
    public static final String CONNECTION_URI_DEFAULT = "kuduhost:kuduport";
    public static final String DIST_LOCK_PREFIX = "metabase";

    public static final String REDIS_URL_DEFAULT = "redishost:redisport";
    public static final String REDIS_URL_CONF = "redis.url";
    protected static final String REDIS_URL_DOC = "redis url as host:port, used in dist lock when new field not in schema is detected and table altering to be done";

    public static final String KUDU_PRESTO_CATALOG_DEFAULT = "presto";
    public static final String KUDU_PRESTO_CATALOG_CONF = "kudu.presto.catalog";
    protected static final String KUDU_PRESTO_CATALOG_DOC = "kudu catalog name at the beginning of kudu table name, used in presto schema/database emulation";

    public static final String KUDU_DATABASE_DEFAULT = "default";
    public static final String KUDU_DATABASE_CONF = "kudu.database";
    protected static final String KUDU_DATABASE_DOC = "kudu database name between catalog and table names";

    public KuduSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public KuduSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef() {

            private <T> Validator<T> ensureValid(String name, Consumer<T> consumer) {
                return new Validator<>(name, consumer);
            }

            class Validator<T> {

                private final String name;
                private final Consumer<T> consumer;

                Validator(String name, Consumer<T> consumer) {
                    this.name = name;
                    this.consumer = consumer;
                }

                private Validator<T> unless(boolean condition) {
                    return condition ? new Validator<T>(name, (T t) -> {}) : this;
                }

                private void accept(T obj) {
                    this.consumer.accept(obj);
                }
            }

            @Override
            public Map<String, ConfigValue> validateAll(Map<String, String> props) {
                Map<String, ConfigValue> result = super.validateAll(props);
                KuduSinkConnectorConfig config = new KuduSinkConnectorConfig(props);
                Stream.of(
                    ensureValid(CONNECTION_URI_CONF, KuduSinkConnectorConfig::buildClientURI),
                    ensureValid(KEY_PROJECTION_TYPE_CONF,
                            (KuduSinkConnectorConfig cfg) -> cfg.getKeyProjectionList("")),
                    ensureValid(VALUE_PROJECTION_TYPE_CONF,
                            (KuduSinkConnectorConfig cfg) -> cfg.getValueProjectionList("")),
                    ensureValid(FIELD_RENAMER_MAPPING,
                            (KuduSinkConnectorConfig cfg) -> cfg.parseRenameFieldnameMappings("")),
                    ensureValid(FIELD_RENAMER_REGEXP,
                            (KuduSinkConnectorConfig cfg) -> cfg.parseRenameRegExpSettings("")),
                    ensureValid(POST_PROCESSOR_CHAIN,
                            (KuduSinkConnectorConfig cfg) -> cfg.buildPostProcessorChain("")),
                    ensureValid(CHANGE_DATA_CAPTURE_HANDLER,
                            (KuduSinkConnectorConfig cfg) -> cfg.getCdcHandler(""))
                        .unless(config.getString(CHANGE_DATA_CAPTURE_HANDLER).isEmpty())
                ).forEach(validator -> {
                    try {
                        validator.accept(config);
                    } catch (Exception ex) {
                        result.get(validator.name).addErrorMessage(ex.getMessage());
                    }
                });
                return result;
            }
        }
                .define(REDIS_URL_CONF, Type.STRING, REDIS_URL_DEFAULT, Importance.MEDIUM, REDIS_URL_DOC)
                .define(SRC_FIELDNAME_WITH_UPPERCASE_CONF, Type.BOOLEAN, SRC_FIELDNAME_WITH_UPPERCASE_DEFAULT, Importance.MEDIUM, SRC_FIELDNAME_WITH_UPPERCASE_DOC)
                .define(KUDU_PRESTO_CATALOG_CONF, Type.STRING, KUDU_PRESTO_CATALOG_DEFAULT, Importance.MEDIUM, KUDU_PRESTO_CATALOG_DOC)
                .define(KUDU_DATABASE_CONF, Type.STRING, KUDU_DATABASE_DEFAULT, Importance.MEDIUM, KUDU_DATABASE_DOC)
                .define(CONNECTION_URI_CONF, Type.STRING, CONNECTION_URI_DEFAULT, Importance.HIGH, CONNECTION_URI_DOC)
                .define(COLLECTIONS_CONF, Type.STRING, COLLECTIONS_DEFAULT, Importance.MEDIUM, COLLECTIONS_DOC)
                .define(COLLECTION_CONF, Type.STRING, COLLECTION_DEFAULT, Importance.HIGH, COLLECTION_DOC)
                .define(MAX_NUM_RETRIES_CONF, Type.INT, MAX_NUM_RETRIES_DEFAULT, ConfigDef.Range.atLeast(0), Importance.MEDIUM, MAX_NUM_RETRIES_DOC)
                .define(RETRIES_DEFER_TIMEOUT_CONF, Type.INT, RETRIES_DEFER_TIMEOUT_DEFAULT, ConfigDef.Range.atLeast(0), Importance.MEDIUM, RETRIES_DEFER_TIMEOUT_DOC)
                .define(VALUE_PROJECTION_TYPE_CONF, Type.STRING, VALUE_PROJECTION_TYPE_DEFAULT, EnumValidator.in(FieldProjectionTypes.values()), Importance.LOW, VALUE_PROJECTION_TYPE_DOC)
                .define(VALUE_PROJECTION_LIST_CONF, Type.STRING, VALUE_PROJECTION_LIST_DEFAULT, Importance.LOW, VALUE_PROJECTION_LIST_DOC)
                .define(KEY_PROJECTION_TYPE_CONF, Type.STRING, KEY_PROJECTION_TYPE_DEFAULT, EnumValidator.in(FieldProjectionTypes.values()), Importance.LOW, KEY_PROJECTION_TYPE_DOC)
                .define(KEY_PROJECTION_LIST_CONF, Type.STRING, KEY_PROJECTION_LIST_DEFAULT, Importance.LOW, KEY_PROJECTION_LIST_DOC)
                .define(FIELD_RENAMER_MAPPING, Type.STRING, FIELD_RENAMER_MAPPING_DEFAULT, Importance.LOW, FIELD_RENAMER_MAPPING_DOC)
                .define(FIELD_RENAMER_REGEXP, Type.STRING, FIELD_RENAMER_REGEXP_DEFAULT, Importance.LOW, FIELD_RENAMER_REGEXP_DOC)
                .define(POST_PROCESSOR_CHAIN, Type.STRING, POST_PROCESSOR_CHAIN_DEFAULT, emptyString().or(matching(FULLY_QUALIFIED_CLASS_NAME_LIST)), Importance.LOW, POST_PROCESSOR_CHAIN_DOC)
                .define(CHANGE_DATA_CAPTURE_HANDLER, Type.STRING, CHANGE_DATA_CAPTURE_HANDLER_DEFAULT, emptyString().or(matching(FULLY_QUALIFIED_CLASS_NAME)), Importance.LOW, CHANGE_DATA_CAPTURE_HANDLER_DOC)
                .define(DELETE_ON_NULL_VALUES, Type.BOOLEAN, DELETE_ON_NULL_VALUES_DEFAULT, Importance.MEDIUM, DELETE_ON_NULL_VALUES_DOC)
                .define(WRITEMODEL_STRATEGY, Type.STRING, MONGODB_WRITEMODEL_STRATEGY_DEFAULT, Importance.LOW, MONGODB_WRITEMODEL_STRATEGY_DOC)
                .define(MAX_BATCH_SIZE, Type.INT, MAX_BATCH_SIZE_DEFAULT, ConfigDef.Range.atLeast(0), Importance.MEDIUM, MAX_BATCH_SIZE_DOC)
                .define(RATE_LIMITING_TIMEOUT, Type.INT, RATE_LIMITING_TIMEOUT_DEFAULT, ConfigDef.Range.atLeast(0), Importance.LOW, RATE_LIMITING_TIMEOUT_DOC)
                .define(RATE_LIMITING_EVERY_N, Type.INT, RATE_LIMITING_EVERY_N_DEFAULT, ConfigDef.Range.atLeast(0), Importance.LOW, RATE_LIMITING_EVERY_N_DOC)
                ;
    }

    public String getRedisUrl() {
        return getString(REDIS_URL_CONF);
    }
    public String getKuduPrestoCatalog() {
        return getString(KUDU_PRESTO_CATALOG_CONF);
    }

    public String getKuduDatabase() {
        return getString(KUDU_DATABASE_CONF);
    }

    public String buildClientURI() {
        return getString(CONNECTION_URI_CONF);
    }

    @Deprecated
    public WriteModelStrategy getWriteModelStrategy() {
        return getWriteModelStrategy("");
    }

    public WriteModelStrategy getWriteModelStrategy(String collection) {
        String strategyClassName = getString(WRITEMODEL_STRATEGY,collection);
        try {
            return (WriteModelStrategy) Class.forName(strategyClassName)
                    .getConstructor(boolean.class).newInstance(isSrcFieldNameWithUppercase());
        } catch (ReflectiveOperationException e) {
            throw new ConfigException(e.getMessage(),e);
        } catch (ClassCastException e) {
            throw new ConfigException("error: specified class "+ strategyClassName
                    + " violates the contract since it doesn't implement " +
                    WriteModelStrategy.class);
        }
    }

    public Map<String, WriteModelStrategy> getWriteModelStrategies() {

        Map<String, WriteModelStrategy> writeModelStrategies = new HashMap<>();

        writeModelStrategies.put(TOPIC_AGNOSTIC_KEY_NAME,getWriteModelStrategy(""));

        splitAndTrimAndRemoveConfigListEntries(getString(COLLECTIONS_CONF))
                .forEach(collection -> writeModelStrategies.put(collection,getWriteModelStrategy(collection)));

        return writeModelStrategies;

    }

    public WriteModelStrategy getDeleteOneModelDefaultStrategy(String collection) {
        return new DeleteOneDefaultStrategy(isSrcFieldNameWithUppercase());
    }

    public Map<String, WriteModelStrategy> getDeleteOneModelDefaultStrategies() {

        Map<String, WriteModelStrategy> deleteModelStrategies = new HashMap<>();

        if(isDeleteOnNullValues("")) {
            deleteModelStrategies.put(TOPIC_AGNOSTIC_KEY_NAME, getDeleteOneModelDefaultStrategy(""));
        }

        splitAndTrimAndRemoveConfigListEntries(getString(COLLECTIONS_CONF))
                .forEach(collection -> {
                    if(isDeleteOnNullValues(collection)) {
                        deleteModelStrategies.put(collection, getDeleteOneModelDefaultStrategy(collection));
                    }
                });

        return deleteModelStrategies;
    }

    public static Set<String> getPredefinedCdcHandlerClassNames() {
        Set<String> cdcHandlers = new HashSet<>();
        cdcHandlers.add(MongoDbHandler.class.getName());
        cdcHandlers.add(RdbmsHandler.class.getName());
        cdcHandlers.add(MysqlHandler.class.getName());
        cdcHandlers.add(PostgresHandler.class.getName());
        return cdcHandlers;
    }

    @Deprecated
    public CdcHandler getCdcHandler() {
        return getCdcHandler("");
    }

    public CdcHandler getCdcHandler(String collection) {
        Set<String> predefinedCdcHandler = getPredefinedCdcHandlerClassNames();

        String cdcHandler = getString(CHANGE_DATA_CAPTURE_HANDLER,collection);

        if(cdcHandler.isEmpty()) {
            return null;
        }

        if(!predefinedCdcHandler.contains(cdcHandler)) {
            throw new ConfigException("error: unknown cdc handler "+cdcHandler);
        }

        try {
            return (CdcHandler) Class.forName(cdcHandler)
                    .getConstructor(KuduSinkConnectorConfig.class)
                    .newInstance(this);
        } catch (ReflectiveOperationException e) {
            throw new ConfigException(e.getMessage(),e);
        } catch (ClassCastException e) {
            throw new ConfigException("error: specified class "+ cdcHandler
                    + " violates the contract since it doesn't implement " +
                    CdcHandler.class);
        }

    }

    public Map<String, CdcHandler> getCdcHandlers() {

        Map<String, CdcHandler> cdcHandlers = new HashMap<>();

        splitAndTrimAndRemoveConfigListEntries(getString(COLLECTIONS_CONF))
                .forEach(collection -> {
                            CdcHandler candidate = cdcHandlers.put(collection,getCdcHandler(collection));
                            if(candidate != null) {
                                cdcHandlers.put(collection,candidate);
                            }
                        }
                );

        return cdcHandlers;

    }

    public IdStrategy getIdStrategy(String collection) {
        return null;
    }

}
