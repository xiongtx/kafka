package org.apache.kafka.connect.transforms;

/**
 * Created by Tianxiang Xiong on 7/26/17.
 */

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireSchema;

public abstract class SetSchemaNonOptional<R extends ConnectRecord<R>> implements Transformation<R> {

    @Override
    public void configure(Map<String, ?> configs) {
        // Do nothing
    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        requireSchema(schema, "setting schema non-optional if k/v is non-null");

        // If schema is already non-optional, do nothing
        if (!schema.isOptional()) {
            return record;
        }

        // If k/v is null, schema MUST be optional
        // This SMT is then a mistake; crash early and loudly
        final Object keyOrValue = operatingKeyOrValue(record);
        if (keyOrValue == null) {
            throw new ConnectException("Cannot set schema non-optional for null k/v");
        }

        final boolean isArray = schema.type() == Schema.Type.ARRAY;
        final boolean isMap = schema.type() == Schema.Type.MAP;
        final boolean isStruct = schema.type() == Schema.Type.STRUCT;
        final Schema updatedSchema = new ConnectSchema(
                schema.type(),
                false,
                schema.defaultValue(),
                schema.name(),
                schema.version(),
                schema.doc(),
                schema.parameters(),
                isStruct ?  schema.fields() : null,
                isMap ? schema.keySchema() : null,
                isMap || isArray ? schema.valueSchema() : null
        );
        return newRecord(record, updatedSchema);
    }

    @Override
    public ConfigDef config() {
        // Empty config
        return new ConfigDef();
    }

    @Override
    public void close() {
        // Do nothing
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingKeyOrValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema);

    /**
     * Make the key schema non-optional
     */
    public static class Key<R extends ConnectRecord<R>> extends SetSchemaNonOptional<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingKeyOrValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema) {
            Object updatedKey = updateSchemaIn(record.key(), updatedSchema);
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedKey, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    /**
     * Make the value schema non-optional
     */
    public static class Value<R extends ConnectRecord<R>> extends SetSchemaNonOptional<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingKeyOrValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema) {
            Object updatedValue = updateSchemaIn(record.value(), updatedSchema);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
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
    protected static Object updateSchemaIn(Object keyOrValue, Schema updatedSchema) {
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

}
