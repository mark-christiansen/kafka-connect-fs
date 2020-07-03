package com.github.mmolimar.kafka.connect.fs.guidewire.transform;

import com.google.common.base.CaseFormat;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class SetSchemaName implements Transformation {

    private String schemaNamespace;

    @Override
    public ConnectRecord apply(ConnectRecord record) {

        // update the key schema name based on the topic name plus "_key" converted to camel-case prefixed by the
        // schema namespace
        final Schema keySchema = record.keySchema();
        final Schema updatedKeySchema = updateSchema(keySchema, record.topic() + "_key");
        final Object updatedKey = updateSchemaIn(record.key(), updatedKeySchema);

        // update the value schema name based on the topic name converted to camel-case prefixed by the schema namespace
        final Schema valueSchema = record.valueSchema();
        final Schema updatedValueSchema = updateSchema(valueSchema, record.topic());
        final Object updatedValue = updateSchemaIn(record.value(), updatedValueSchema);

        return record.newRecord(record.topic(), record.kafkaPartition(), updatedKeySchema, updatedKey, updatedValueSchema, updatedValue, record.timestamp());
    }

    private Schema updateSchema(Schema schema, String topicName) {
        final boolean isArray = schema.type() == Schema.Type.ARRAY;
        final boolean isMap = schema.type() == Schema.Type.MAP;
        return new ConnectSchema(
                schema.type(),
                schema.isOptional(),
                schema.defaultValue(),
                schemaNamespace + CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, topicName),
                schema.version(),
                schema.doc(),
                schema.parameters(),
                schema.fields(),
                isMap ? schema.keySchema() : null,
                isMap || isArray ? schema.valueSchema() : null);
    }

    @Override
    public ConfigDef config() {
        return SetSchemaMetadataConfig.conf();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
        final SetSchemaMetadataConfig config = new SetSchemaMetadataConfig(map);
        schemaNamespace = config.getSchemaNamespace();
        if (schemaNamespace == null) {
            throw new ConfigException("Schema namespace not configured");
        } else if (!schemaNamespace.endsWith(".")) {
            schemaNamespace = schemaNamespace + ".";
        }
    }

    /**
     * Utility to check the supplied key or value for references to the old Schema,
     * and if so to return an updated key or value object that references the new Schema.
     * Note that this method assumes that the new Schema may have a different name and/or version,
     * but has fields that exactly match those of the old Schema.
     * <p>
     * Currently only {@link Struct} objects have references to the {@link Schema}.
     *
     * @param keyOrValue    the key or value object; may be null
     * @param updatedSchema the updated schema that has been potentially renamed
     * @return the original key or value object if it does not reference the old schema, or
     * a copy of the key or value object with updated references to the new schema.
     */
    private static Object updateSchemaIn(Object keyOrValue, Schema updatedSchema) {
        if (keyOrValue instanceof Struct) {
            Struct origStruct = (Struct) keyOrValue;
            Struct newStruct = new Struct(updatedSchema);
            for (Field field : updatedSchema.fields()) {
                // assume both schemas have exact same fields with same names and schemas ...
                newStruct.put(field, origStruct.get(field));
            }
            return newStruct;
        }
        return keyOrValue;
    }

    public static class SetSchemaMetadataConfig extends AbstractConfig {

        private static final String SCHEMA_NAMESPACE = "schema.namespace";
        private static final String SCHEMA_NAMESPACE_DOC = "Namespace for schema key and value names.";

        public SetSchemaMetadataConfig(ConfigDef config, Map<String, ?> parsedConfig) {
            super(config, parsedConfig);
        }

        public SetSchemaMetadataConfig(Map<String, ?> parsedConfig) {
            this(conf(), parsedConfig);
        }

        public static ConfigDef conf() {
            return new ConfigDef()
                    .define(
                            SCHEMA_NAMESPACE,
                            ConfigDef.Type.STRING,
                            ConfigDef.NO_DEFAULT_VALUE,
                            ConfigDef.Importance.HIGH,
                            SCHEMA_NAMESPACE_DOC
                    );
        }

        public String getSchemaNamespace() {
            return this.getString(SCHEMA_NAMESPACE);
        }
    }
}