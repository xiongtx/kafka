package org.apache.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by Tianxiang Xiong on 7/26/17.
 */
public class SetSchemaNonOptionalTest {
    private final SetSchemaNonOptional<SourceRecord> xformKey = new SetSchemaNonOptional.Key<>();
    private final SetSchemaNonOptional<SourceRecord> xformValue = new SetSchemaNonOptional.Value<>();

    // Key
    private final Schema keySchema = SchemaBuilder.OPTIONAL_STRING_SCHEMA;
    private final String key = "foo";

    // Value schema
    private final String fieldName1 = "f1";
    private final String fieldName2 = "f2";
    private final String fieldValue1 = "value1";
    private final int fieldValue2 = 1;
    private final Schema valueSchema = SchemaBuilder.struct()
        .name("my.orig.SchemaDefn")
        .field(fieldName1, Schema.STRING_SCHEMA)
        .field(fieldName2, Schema.INT32_SCHEMA)
        .optional()
        .build();
    private final Struct value = new Struct(valueSchema)
        .put(fieldName1, fieldValue1)
        .put(fieldName2, fieldValue2);

    @After
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test
    public void setKeyNonOptional() {
        final SourceRecord record = new SourceRecord(null, null, null, null,
                keySchema, key, valueSchema, value);

        final SourceRecord transformed = xformKey.apply(record);

        assertEquals(SchemaBuilder.STRING_SCHEMA, transformed.keySchema());
        assertEquals(key, transformed.key());
    }

    @Test(expected = ConnectException.class)
    public void setNullKeyNonOptional() {
        final SourceRecord record = new SourceRecord(null, null, null, null,
                keySchema, null, valueSchema, value);

        final SourceRecord transformed = xformKey.apply(record);

        assertEquals(keySchema, transformed.keySchema());
        assertNull(key);
    }

    @Test
    public void setValueNonOptional() {
        final SourceRecord record = new SourceRecord(null, null, null, null,
                keySchema, key, valueSchema, value);

        final SourceRecord transformed = xformValue.apply(record);

        assert(!transformed.valueSchema().isOptional());

        // Cannot compare structs directly b/c schemas are different
        // Need reflection to get values of struct
        try {
            Field f = Struct.class.getDeclaredField("values");
            f.setAccessible(true);
            assertArrayEquals((Object[]) f.get(value), (Object[]) f.get(transformed.value()));
        } catch (NoSuchFieldException|IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Test(expected = ConnectException.class)
    public void setNullValueNonOptional() {
        final SourceRecord record = new SourceRecord(null, null, null, null,
                keySchema, key, valueSchema, null);

        final SourceRecord transformed = xformValue.apply(record);

        assert(transformed.valueSchema().isOptional());
        assertNull(transformed.value());
    }

}
