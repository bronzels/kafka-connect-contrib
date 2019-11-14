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

package at.bronzels.kafka.connect;

import at.bronzels.kafka.connect.mongodb.id.strategy.IdStrategy;
import at.bronzels.kafka.connect.mongodb.id.strategy.PartialKeyStrategy;
import at.bronzels.kafka.connect.mongodb.id.strategy.PartialValueStrategy;

import at.grahsl.kafka.connect.CollectionAwareConfig;
import at.grahsl.kafka.connect.processor.*;
import at.grahsl.kafka.connect.processor.field.projection.FieldProjector;
import at.grahsl.kafka.connect.processor.field.renaming.FieldnameMapping;
import at.grahsl.kafka.connect.processor.field.renaming.RegExpSettings;
import at.grahsl.kafka.connect.processor.field.renaming.RenameByRegExp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

abstract public class SinkConnectorConfig extends CollectionAwareConfig {

    public enum FieldProjectionTypes {
        NONE,
        BLACKLIST,
        WHITELIST
    }

    protected static final Pattern CLASS_NAME = Pattern.compile("\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*");
    protected static final Pattern FULLY_QUALIFIED_CLASS_NAME = Pattern.compile("(" + CLASS_NAME + "\\.)*" + CLASS_NAME);
    protected static final Pattern FULLY_QUALIFIED_CLASS_NAME_LIST = Pattern.compile("(" + FULLY_QUALIFIED_CLASS_NAME + ",)*" + FULLY_QUALIFIED_CLASS_NAME);

    public static final String FIELD_LIST_SPLIT_CHAR = ",";
    public static final String FIELD_LIST_SPLIT_EXPR = "\\s*"+FIELD_LIST_SPLIT_CHAR+"\\s*";

    public static final String TOPIC_AGNOSTIC_KEY_NAME = "__default__";
    public static final String NAMESPACE_SEPARATOR = ".";

    public static final boolean SRC_FIELDNAME_WITH_UPPERCASE_DEFAULT = false;
    public static final String COLLECTIONS_DEFAULT = "";
    public static final String COLLECTION_DEFAULT = "";
    public static final int MAX_NUM_RETRIES_DEFAULT = 3;
    public static final int RETRIES_DEFER_TIMEOUT_DEFAULT = 5000;
    public static final String VALUE_PROJECTION_TYPE_DEFAULT = "none";
    public static final String VALUE_PROJECTION_LIST_DEFAULT = "";
    public static final String DOCUMENT_ID_STRATEGY_DEFAULT = "at.grahsl.kafka.connect.mongodb.strategy.BsonOidStrategy";
    public static final String DOCUMENT_ID_STRATEGIES_DEFAULT = "";
    public static final String KEY_PROJECTION_TYPE_DEFAULT = "none";
    public static final String KEY_PROJECTION_LIST_DEFAULT = "";
    public static final String FIELD_RENAMER_MAPPING_DEFAULT = "[]";
    public static final String FIELD_RENAMER_REGEXP_DEFAULT = "[]";
    public static final String POST_PROCESSOR_CHAIN_DEFAULT = "at.grahsl.kafka.connect.processor.DocumentIdAdder";
    public static final String CHANGE_DATA_CAPTURE_HANDLER_DEFAULT = "";
    public static final boolean DELETE_ON_NULL_VALUES_DEFAULT = false;
    public static final String MONGODB_WRITEMODEL_STRATEGY_DEFAULT = "at.grahsl.kafka.connect.mongodb.writemodel.strategy.ReplaceOneDefaultStrategy";
    public static final int MAX_BATCH_SIZE_DEFAULT = 0;
    public static final int RATE_LIMITING_TIMEOUT_DEFAULT = 0;
    public static final int RATE_LIMITING_EVERY_N_DEFAULT = 0;

    public static final String SRC_FIELDNAME_WITH_UPPERCASE_CONF = "src.fieldname.with.uppercase";
    protected static final String SRC_FIELDNAME_WITH_UPPERCASE_DOC = "field names from source connector can be in uppercase or not. if true, name convertion is applied as datawarehouse field names are unified all in lowercased.";

    public static final String CONNECTION_URI_CONF = "connection.uri";
    protected static final String CONNECTION_URI_DOC = "the monogdb connection URI as supported by the offical drivers";

    public static final String COLLECTION_CONF = "collection";
    protected static final String COLLECTION_DOC = "single sink collection name to write to";

    public static final String COLLECTIONS_CONF = "collections";
    protected static final String COLLECTIONS_DOC = "names of sink collections to write to for which there can be topic-level specific properties defined";

    public static final String MAX_NUM_RETRIES_CONF = "max.num.retries";
    protected static final String MAX_NUM_RETRIES_DOC = "how often a retry should be done on write errors";

    public static final String RETRIES_DEFER_TIMEOUT_CONF = "retries.defer.timeout";
    protected static final String RETRIES_DEFER_TIMEOUT_DOC = "how long in ms a retry should get deferred";

    public static final String VALUE_PROJECTION_TYPE_CONF = "value.projection.type";
    protected static final String VALUE_PROJECTION_TYPE_DOC = "whether or not and which value projection to use";

    public static final String VALUE_PROJECTION_LIST_CONF = "value.projection.list";
    protected static final String VALUE_PROJECTION_LIST_DOC = "comma separated list of field names for value projection";

    public static final String DOCUMENT_ID_STRATEGY_CONF = "document.id.strategy";
    protected static final String DOCUMENT_ID_STRATEGY_CONF_DOC = "class name of strategy to use for generating a unique document id (_id)";

    public static final String DOCUMENT_ID_STRATEGIES_CONF = "document.id.strategies";
    protected static final String DOCUMENT_ID_STRATEGIES_CONF_DOC = "comma separated list of custom strategy classes to register for usage";

    public static final String KEY_PROJECTION_TYPE_CONF = "key.projection.type";
    protected static final String KEY_PROJECTION_TYPE_DOC = "whether or not and which key projection to use";

    public static final String KEY_PROJECTION_LIST_CONF = "key.projection.list";
    protected static final String KEY_PROJECTION_LIST_DOC = "comma separated list of field names for key projection";

    public static final String FIELD_RENAMER_MAPPING = "field.renamer.mapping";
    protected static final String FIELD_RENAMER_MAPPING_DOC = "inline JSON array with objects describing field name mappings";

    public static final String FIELD_RENAMER_REGEXP = "field.renamer.regexp";
    protected static final String FIELD_RENAMER_REGEXP_DOC = "inline JSON array with objects describing regexp settings";

    public static final String POST_PROCESSOR_CHAIN = "post.processor.chain";
    protected static final String POST_PROCESSOR_CHAIN_DOC = "comma separated list of post processor classes to build the chain with";

    public static final String CHANGE_DATA_CAPTURE_HANDLER = "change.data.capture.handler";
    protected static final String CHANGE_DATA_CAPTURE_HANDLER_DOC = "class name of CDC handler to use for processing";

    public static final String DELETE_ON_NULL_VALUES = "delete.on.null.values";
    protected static final String DELETE_ON_NULL_VALUES_DOC = "whether or not the connector tries to delete documents based on key when value is null";

    public static final String WRITEMODEL_STRATEGY = "writemodel.strategy";
    protected static final String MONGODB_WRITEMODEL_STRATEGY_DOC = "how to build the write models for the sink documents";

    public static final String MAX_BATCH_SIZE = "max.batch.size";
    protected static final String MAX_BATCH_SIZE_DOC = "maximum number of sink records to possibly batch together for processing";

    public static final String RATE_LIMITING_TIMEOUT = "rate.limiting.timeout";
    protected static final String RATE_LIMITING_TIMEOUT_DOC = "how long in ms processing should wait before continue processing";

    public static final String RATE_LIMITING_EVERY_N = "rate.limiting.every.n";
    protected static final String RATE_LIMITING_EVERY_N_DOC = "after how many processed batches the rate limit should trigger (NO rate limiting if n=0)";

    protected static ObjectMapper objectMapper = new ObjectMapper();

    public static class RateLimitSettings {

        private final int timeoutMs;
        private final int everyN;
        private long counter;

        public RateLimitSettings(int timeoutMs, int everyN) {
            this.timeoutMs = timeoutMs;
            this.everyN = everyN;
        }

        public boolean isTriggered() {
            counter++;
            return (everyN != 0)
                    && (counter >= everyN)
                    && (counter % everyN == 0);
        }

        public int getTimeoutMs() {
            return timeoutMs;
        }

        public int getEveryN() {
            return everyN;
        }

        public long getCounter() {
            return counter;
        }
    }

    public SinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    @Deprecated
    public boolean isUsingBlacklistValueProjection() {
        return isUsingBlacklistValueProjection("");
    }

    public boolean isUsingBlacklistValueProjection(String collection) {
        return getString(VALUE_PROJECTION_TYPE_CONF,collection)
                .equalsIgnoreCase(FieldProjectionTypes.BLACKLIST.name());
    }

    @Deprecated
    public boolean isUsingWhitelistValueProjection() {
        return isUsingWhitelistValueProjection("");
    }

    public boolean isUsingWhitelistValueProjection(String collection) {
        return getString(VALUE_PROJECTION_TYPE_CONF,collection)
                .equalsIgnoreCase(FieldProjectionTypes.WHITELIST.name());
    }

    @Deprecated
    public boolean isUsingBlacklistKeyProjection() {
        return isUsingBlacklistKeyProjection("");
    }

    public boolean isUsingBlacklistKeyProjection(String collection) {
        return getString(KEY_PROJECTION_TYPE_CONF,collection)
                .equalsIgnoreCase(FieldProjectionTypes.BLACKLIST.name());
    }

    @Deprecated
    public boolean isUsingWhitelistKeyProjection() {
        return isUsingWhitelistKeyProjection("");
    }

    public boolean isUsingWhitelistKeyProjection(String collection) {
        return getString(KEY_PROJECTION_TYPE_CONF,collection)
                .equalsIgnoreCase(FieldProjectionTypes.WHITELIST.name());
    }

    @Deprecated
    public Set<String> getKeyProjectionList() {
        return getKeyProjectionList("");
    }

    public Set<String> getKeyProjectionList(String collection) {
        return buildProjectionList(getString(KEY_PROJECTION_TYPE_CONF,collection),
                getString(KEY_PROJECTION_LIST_CONF,collection)
        );
    }

    @Deprecated
    public Set<String> getValueProjectionList() {
        return getValueProjectionList("");
    }

    public Set<String> getValueProjectionList(String collection) {
        return buildProjectionList(getString(VALUE_PROJECTION_TYPE_CONF,collection),
                getString(VALUE_PROJECTION_LIST_CONF,collection)
        );
    }

    @Deprecated
    public Map<String, String> parseRenameFieldnameMappings() {
        return parseRenameFieldnameMappings("");
    }

    public Map<String, String> parseRenameFieldnameMappings(String collection) {
        try {
            String settings = getString(FIELD_RENAMER_MAPPING,collection);
            if(settings.isEmpty()) {
                return new HashMap<>();
            }

            List<FieldnameMapping> fm = objectMapper.readValue(
                    settings, new TypeReference<List<FieldnameMapping>>() {});

            Map<String, String> map = new HashMap<>();
            for (FieldnameMapping e : fm) {
                map.put(e.oldName, e.newName);
            }
            return map;
        } catch (IOException e) {
            throw new ConfigException("error: parsing rename fieldname mappings failed", e);
        }
    }

    @Deprecated
    public Map<String, RenameByRegExp.PatternReplace> parseRenameRegExpSettings() {
        return parseRenameRegExpSettings("");
    }

    public Map<String, RenameByRegExp.PatternReplace> parseRenameRegExpSettings(String collection) {
        try {
            String settings = getString(FIELD_RENAMER_REGEXP,collection);
            if(settings.isEmpty()) {
                return new HashMap<>();
            }

            List<RegExpSettings> fm = objectMapper.readValue(
                    settings, new TypeReference<List<RegExpSettings>>() {});

            Map<String, RenameByRegExp.PatternReplace> map = new HashMap<>();
            for (RegExpSettings e : fm) {
                map.put(e.regexp, new RenameByRegExp.PatternReplace(e.pattern,e.replace));
            }
            return map;
        } catch (IOException e) {
            throw new ConfigException("error: parsing rename regexp settings failed", e);
        }
    }

    @Deprecated
    public PostProcessor buildPostProcessorChain() {
       return buildPostProcessorChain("");
    }

    public PostProcessor buildPostProcessorChain(String collection) {

        Set<String> classes = new LinkedHashSet<>(
                splitAndTrimAndRemoveConfigListEntries(getString(POST_PROCESSOR_CHAIN,collection))
        );

        //if no post processors are specified
        //DocumentIdAdder is always used since it's mandatory
        if(classes.size() == 0) {
            return new DocumentIdAdder(this,collection);
        }

        PostProcessor first = null;

        if(!classes.contains(DocumentIdAdder.class.getName())) {
            first = new DocumentIdAdder(this,collection);
        }

        PostProcessor next = null;
        for(String clazz : classes) {
            try {
                if(first == null) {
                    first = (PostProcessor) Class.forName(clazz)
                            .getConstructor(SinkConnectorConfig.class, String.class)
                            .newInstance(this,collection);
                } else {
                    PostProcessor current = (PostProcessor) Class.forName(clazz)
                            .getConstructor(SinkConnectorConfig.class, String.class)
                            .newInstance(this,collection);
                    if(next == null) {
                        first.chain(current);
                        next = current;
                    } else {
                        next = next.chain(current);
                    }
                }
            } catch (ReflectiveOperationException e) {
                throw new ConfigException(e.getMessage(),e);
            } catch (ClassCastException e) {
                throw new ConfigException("error: specified class "+ clazz
                        + " violates the contract since it doesn't extend " +
                        PostProcessor.class.getName());
            }
        }

        return first;

    }

    public Map<String,PostProcessor> buildPostProcessorChains() {

        Map<String, PostProcessor> postProcessorChains = new HashMap<>();

        postProcessorChains.put(TOPIC_AGNOSTIC_KEY_NAME,buildPostProcessorChain(""));

        splitAndTrimAndRemoveConfigListEntries(getString(COLLECTIONS_CONF))
                .forEach(collection ->
                        postProcessorChains.put(collection,buildPostProcessorChain(collection))
                );

        return postProcessorChains;

    }

    protected Set<String> buildProjectionList(String projectionType, String fieldList) {

        if(projectionType.equalsIgnoreCase(FieldProjectionTypes.NONE.name()))
            return new HashSet<>();

        if(projectionType.equalsIgnoreCase(FieldProjectionTypes.BLACKLIST.name()))
            return new HashSet<>(splitAndTrimAndRemoveConfigListEntries(fieldList));

        if(projectionType.equalsIgnoreCase(FieldProjectionTypes.WHITELIST.name())) {

            //NOTE: for sub document notation all left prefix bound paths are created
            //which allows for easy recursion mechanism to whitelist nested doc fields

            HashSet<String> whitelistExpanded = new HashSet<>();
            List<String> fields = splitAndTrimAndRemoveConfigListEntries(fieldList);

            for(String f : fields) {
                if(!f.contains("."))
                    whitelistExpanded.add(f);
                else{
                    String[] parts = f.split("\\.");
                    String entry = parts[0];
                    whitelistExpanded.add(entry);
                    for(int s=1;s<parts.length;s++){
                        entry+="."+parts[s];
                        whitelistExpanded.add(entry);
                    }
                }
            }

            return whitelistExpanded;
        }

        throw new ConfigException("error: invalid settings for "+ projectionType);
    }

    protected List<String> splitAndTrimAndRemoveConfigListEntries(String entries) {
        return Arrays.stream(entries.trim().split(FIELD_LIST_SPLIT_EXPR))
                .filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    @Deprecated
    public boolean isUsingCdcHandler() {
        return isUsingCdcHandler("");
    }

    public boolean isUsingCdcHandler(String collection) {
        return !getString(CHANGE_DATA_CAPTURE_HANDLER,collection).isEmpty();
    }

    @Deprecated
    public boolean isDeleteOnNullValues() {
        return isDeleteOnNullValues("");
    }

    public boolean isDeleteOnNullValues(String collection) {
        return getBoolean(DELETE_ON_NULL_VALUES,collection);
    }

    public boolean isSrcFieldNameWithUppercase() {
        return getBoolean(SRC_FIELDNAME_WITH_UPPERCASE_CONF);
    }

    public RateLimitSettings getRateLimitSettings(String collection) {

        return new RateLimitSettings(
            this.getInt(RATE_LIMITING_TIMEOUT,collection),
            this.getInt(RATE_LIMITING_EVERY_N,collection)
        );

    }

    public Map<String,RateLimitSettings> getRateLimitSettings() {

        Map<String, RateLimitSettings> rateLimitSettings = new HashMap<>();

        rateLimitSettings.put(TOPIC_AGNOSTIC_KEY_NAME,getRateLimitSettings(""));

        splitAndTrimAndRemoveConfigListEntries(getString(COLLECTIONS_CONF))
                .forEach(collection -> rateLimitSettings.put(collection,getRateLimitSettings(collection)));

        return rateLimitSettings;

    }

    @Deprecated
    public FieldProjector getKeyProjector() {
        return getKeyProjector("");
    }

    public FieldProjector getKeyProjector(String collection) {

        if(getString(KEY_PROJECTION_TYPE_CONF,collection)
                .equalsIgnoreCase(FieldProjectionTypes.BLACKLIST.name())) {

            if(getString(DOCUMENT_ID_STRATEGY_CONF,collection).
                    equals(PartialValueStrategy.class.getName())) {

                return new BlacklistValueProjector(this,
                        this.getKeyProjectionList(collection),
                            cfg -> cfg.isUsingBlacklistKeyProjection(collection), collection);
            }

            if(getString(DOCUMENT_ID_STRATEGY_CONF,collection).
                    equals(PartialKeyStrategy.class.getName())) {

                return new BlacklistKeyProjector(this,
                        this.getKeyProjectionList(collection),
                            cfg -> cfg.isUsingBlacklistKeyProjection(collection), collection);
            }
        }

        if(getString(KEY_PROJECTION_TYPE_CONF,collection)
                .equalsIgnoreCase(FieldProjectionTypes.WHITELIST.name())) {

            if(getString(DOCUMENT_ID_STRATEGY_CONF,collection).
                    equals(PartialValueStrategy.class.getName())) {

                return new WhitelistValueProjector(this,
                        this.getKeyProjectionList(collection),
                            cfg -> cfg.isUsingWhitelistKeyProjection(collection), collection);
            }

            if(getString(DOCUMENT_ID_STRATEGY_CONF,collection).
                    equals(PartialKeyStrategy.class.getName())) {

                return new WhitelistKeyProjector(this,
                        this.getKeyProjectionList(collection),
                            cfg -> cfg.isUsingWhitelistKeyProjection(collection), collection);
            }

        }

        throw new ConfigException("error: settings invalid for "+ KEY_PROJECTION_TYPE_CONF);
    }

    //EnumValidator borrowed from
    //https://github.com/confluentinc/kafka-connect-jdbc/blob/master/src/main/java/io/confluent/connect/jdbc/sink/JdbcSinkConfig.java
    protected static class EnumValidator implements ConfigDef.Validator {
        private final List<String> canonicalValues;
        private final Set<String> validValues;

        private EnumValidator(List<String> canonicalValues, Set<String> validValues) {
            this.canonicalValues = canonicalValues;
            this.validValues = validValues;
        }

        public static <E> EnumValidator in(E[] enumerators) {
            final List<String> canonicalValues = new ArrayList<>(enumerators.length);
            final Set<String> validValues = new HashSet<>(enumerators.length * 2);
            for (E e : enumerators) {
                canonicalValues.add(e.toString().toLowerCase());
                validValues.add(e.toString().toUpperCase());
                validValues.add(e.toString().toLowerCase());
            }
            return new EnumValidator(canonicalValues, validValues);
        }

        @Override
        public void ensureValid(String key, Object value) {
            if (!validValues.contains(value)) {
                throw new ConfigException(key, value, "Invalid enumerator");
            }
        }

        @Override
        public String toString() {
            return canonicalValues.toString();
        }
    }

    public interface ValidatorWithOperators extends ConfigDef.Validator {

        default ValidatorWithOperators or(ConfigDef.Validator other) {
            return (name, value) -> {
                try {
                    this.ensureValid(name, value);
                } catch (ConfigException e) {
                    other.ensureValid(name, value);
                }
            };
        }

        default ValidatorWithOperators and(ConfigDef.Validator other) {
            return  (name, value) -> {
                this.ensureValid(name, value);
                other.ensureValid(name, value);
            };
        }
    }

    public static ValidatorWithOperators emptyString() {
        return (name, value) -> {
            // value type already validated when parsed as String, hence ignoring ClassCastException
            if (!((String) value).isEmpty()) {
                throw new ConfigException(name, value, "Not empty");
            }
        };
    }

    public static ValidatorWithOperators matching(Pattern pattern) {
        return (name, value) -> {
            // type already validated when parsing config, hence ignoring ClassCastException
            if (!pattern.matcher((String) value).matches()) {
                throw new ConfigException(name, value, "Does not match: " + pattern);
            }
        };
    }

    abstract public IdStrategy getIdStrategy(String collection);

}
