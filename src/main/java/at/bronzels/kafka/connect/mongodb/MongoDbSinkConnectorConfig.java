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

package at.bronzels.kafka.connect.mongodb;

import at.bronzels.kafka.connect.SinkConnectorConfig;
import at.bronzels.kafka.connect.mongodb.cdc.debezium.rdbms.mysql.MysqlHandler;
import at.bronzels.kafka.connect.mongodb.cdc.debezium.rdbms.postgres.PostgresHandler;
import at.bronzels.kafka.connect.mongodb.id.strategy.*;
import at.bronzels.kafka.connect.mongodb.writemodel.strategy.DeleteOneDefaultStrategy;
import at.bronzels.kafka.connect.mongodb.writemodel.strategy.WriteModelStrategy;
import at.grahsl.kafka.connect.processor.field.projection.FieldProjector;
import at.bronzels.kafka.connect.mongodb.cdc.CdcHandler;
import at.bronzels.kafka.connect.mongodb.cdc.debezium.mongodb.MongoDbHandler;
import at.bronzels.kafka.connect.mongodb.cdc.debezium.rdbms.RdbmsHandler;
import com.mongodb.MongoClientURI;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class MongoDbSinkConnectorConfig extends SinkConnectorConfig {
    public static final String CONNECTION_URI_DEFAULT = "mongodb://localhost:27017/kafkaconnect?w=1&journal=true";

    public MongoDbSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public MongoDbSinkConnectorConfig(Map<String, String> parsedConfig) {
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
                MongoDbSinkConnectorConfig config = new MongoDbSinkConnectorConfig(props);
                Stream.of(
                    ensureValid(CONNECTION_URI_CONF, MongoDbSinkConnectorConfig::buildClientURI),
                    ensureValid(KEY_PROJECTION_TYPE_CONF,
                            (MongoDbSinkConnectorConfig cfg) -> cfg.getKeyProjectionList("")),
                    ensureValid(VALUE_PROJECTION_TYPE_CONF,
                            (MongoDbSinkConnectorConfig cfg) -> cfg.getValueProjectionList("")),
                    ensureValid(FIELD_RENAMER_MAPPING,
                            (MongoDbSinkConnectorConfig cfg) -> cfg.parseRenameFieldnameMappings("")),
                    ensureValid(FIELD_RENAMER_REGEXP,
                            (MongoDbSinkConnectorConfig cfg) -> cfg.parseRenameRegExpSettings("")),
                    ensureValid(POST_PROCESSOR_CHAIN,
                            (MongoDbSinkConnectorConfig cfg) -> cfg.buildPostProcessorChain("")),
                    ensureValid(CHANGE_DATA_CAPTURE_HANDLER,
                            (MongoDbSinkConnectorConfig cfg) -> cfg.getCdcHandler(""))
                        .unless(config.getString(CHANGE_DATA_CAPTURE_HANDLER).isEmpty()),
                    ensureValid(DOCUMENT_ID_STRATEGIES_CONF,
                            (MongoDbSinkConnectorConfig cfg) -> cfg.getIdStrategy(""))
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
                .define(CONNECTION_URI_CONF, Type.STRING, CONNECTION_URI_DEFAULT, Importance.HIGH, CONNECTION_URI_DOC)
                .define(COLLECTIONS_CONF, Type.STRING, COLLECTIONS_DEFAULT, Importance.MEDIUM, COLLECTIONS_DOC)
                .define(COLLECTION_CONF, Type.STRING, COLLECTION_DEFAULT, Importance.HIGH, COLLECTION_DOC)
                .define(MAX_NUM_RETRIES_CONF, Type.INT, MAX_NUM_RETRIES_DEFAULT, ConfigDef.Range.atLeast(0), Importance.MEDIUM, MAX_NUM_RETRIES_DOC)
                .define(RETRIES_DEFER_TIMEOUT_CONF, Type.INT, RETRIES_DEFER_TIMEOUT_DEFAULT, ConfigDef.Range.atLeast(0), Importance.MEDIUM, RETRIES_DEFER_TIMEOUT_DOC)
                .define(VALUE_PROJECTION_TYPE_CONF, Type.STRING, VALUE_PROJECTION_TYPE_DEFAULT, EnumValidator.in(FieldProjectionTypes.values()), Importance.LOW, VALUE_PROJECTION_TYPE_DOC)
                .define(VALUE_PROJECTION_LIST_CONF, Type.STRING, VALUE_PROJECTION_LIST_DEFAULT, Importance.LOW, VALUE_PROJECTION_LIST_DOC)
                .define(DOCUMENT_ID_STRATEGY_CONF, Type.STRING, DOCUMENT_ID_STRATEGY_DEFAULT, emptyString().or(matching(FULLY_QUALIFIED_CLASS_NAME)), Importance.HIGH, DOCUMENT_ID_STRATEGY_CONF_DOC)
                .define(DOCUMENT_ID_STRATEGIES_CONF, Type.STRING, DOCUMENT_ID_STRATEGIES_DEFAULT, emptyString().or(matching(FULLY_QUALIFIED_CLASS_NAME_LIST)), Importance.LOW, DOCUMENT_ID_STRATEGIES_CONF_DOC)
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

    public MongoClientURI buildClientURI() {
        return new MongoClientURI(getString(CONNECTION_URI_CONF));
    }

    @Deprecated
    public WriteModelStrategy getWriteModelStrategy() {
        return getWriteModelStrategy("");
    }

    public WriteModelStrategy getWriteModelStrategy(String collection) {
        String strategyClassName = getString(WRITEMODEL_STRATEGY,collection);
        try {
            return (WriteModelStrategy) Class.forName(strategyClassName)
                    .getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new ConfigException(e.getMessage(),e);
        } catch (ClassCastException e) {
            throw new ConfigException("error: specified class "+ strategyClassName
                    + " violates the contract since it doesn't implement " +
                    WriteModelStrategy.class);
        }
    }

    public Map<String,WriteModelStrategy> getWriteModelStrategies() {

        Map<String, WriteModelStrategy> writeModelStrategies = new HashMap<>();

        writeModelStrategies.put(TOPIC_AGNOSTIC_KEY_NAME,getWriteModelStrategy(""));

        splitAndTrimAndRemoveConfigListEntries(getString(COLLECTIONS_CONF))
                .forEach(collection -> writeModelStrategies.put(collection,getWriteModelStrategy(collection)));

        return writeModelStrategies;

    }

    public WriteModelStrategy getDeleteOneModelDefaultStrategy(String collection) {

        //NOTE: DeleteOneModel requires the key document
        //which means that the only reasonable ID generation strategies
        //are those which refer to/operate on the key document.
        //Thus currently this means the IdStrategy must be either:

        //FullKeyStrategy
        //PartialKeyStrategy
        //ProvidedInKeyStrategy

        IdStrategy idStrategy = this.getIdStrategy(collection);

        if(!(idStrategy instanceof FullKeyStrategy)
                && !(idStrategy instanceof PartialKeyStrategy)
                && !(idStrategy instanceof ProvidedInKeyStrategy)) {
            throw new ConfigException(
                    DeleteOneDefaultStrategy.class.getName() + " can only be applied"
                            + " when the configured IdStrategy is either "
                            + FullKeyStrategy.class.getSimpleName() + " or "
                            + PartialKeyStrategy.class.getSimpleName() + " or "
                            + ProvidedInKeyStrategy.class.getSimpleName()
            );
        }

        return new DeleteOneDefaultStrategy(idStrategy);
    }

    public Map<String,WriteModelStrategy> getDeleteOneModelDefaultStrategies() {

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
                    .getConstructor(MongoDbSinkConnectorConfig.class)
                    .newInstance(this);
        } catch (ReflectiveOperationException e) {
            throw new ConfigException(e.getMessage(),e);
        } catch (ClassCastException e) {
            throw new ConfigException("error: specified class "+ cdcHandler
                    + " violates the contract since it doesn't implement " +
                    CdcHandler.class);
        }

    }

    public Map<String,CdcHandler> getCdcHandlers() {

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

    public static Set<String> getPredefinedIdStrategyClassNames() {
        Set<String> strategies = new HashSet<String>();
        strategies.add(BsonOidStrategy.class.getName());
        strategies.add(FullKeyStrategy.class.getName());
        strategies.add(KafkaMetaDataStrategy.class.getName());
        strategies.add(PartialKeyStrategy.class.getName());
        strategies.add(PartialValueStrategy.class.getName());
        strategies.add(ProvidedInKeyStrategy.class.getName());
        strategies.add(ProvidedInValueStrategy.class.getName());
        strategies.add(UuidStrategy.class.getName());
        return strategies;
    }

    @Deprecated
    public IdStrategy getIdStrategy() {
        return getIdStrategy("");
    }

    public IdStrategy getIdStrategy(String collection) {
        Set<String> availableIdStrategies = getPredefinedIdStrategyClassNames();

        Set<String> customIdStrategies = new HashSet<>(
                splitAndTrimAndRemoveConfigListEntries(getString(DOCUMENT_ID_STRATEGIES_CONF))
        );

        availableIdStrategies.addAll(customIdStrategies);

        String strategyClassName = getString(DOCUMENT_ID_STRATEGY_CONF,collection);

        if(!availableIdStrategies.contains(strategyClassName)) {
            throw new ConfigException("error: unknown id strategy "+strategyClassName);
        }

        try {
            if(strategyClassName.equals(PartialKeyStrategy.class.getName())
                    || strategyClassName.equals(PartialValueStrategy.class.getName())) {
                return (IdStrategy)Class.forName(strategyClassName)
                        .getConstructor(FieldProjector.class)
                        .newInstance(this.getKeyProjector(collection));
            }
            return (IdStrategy)Class.forName(strategyClassName)
                    .getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new ConfigException(e.getMessage(),e);
        } catch (ClassCastException e) {
            throw new ConfigException("error: specified class "+ strategyClassName
                    + " violates the contract since it doesn't implement " +
                    IdStrategy.class);
        }
    }

}
