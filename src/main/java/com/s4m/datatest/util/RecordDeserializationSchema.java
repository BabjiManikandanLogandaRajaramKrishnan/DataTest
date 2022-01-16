package com.s4m.datatest.util;

import com.s4m.datatest.entity.Record;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;

/**
 * Serializer for Record Datatype
 */
public class RecordDeserializationSchema implements DeserializationSchema<Record> {

    //Spurce - https://stackoverflow.com/questions/64513940/apache-flink-how-to-implement-custom-deserializer-implementing-deserialization

    private static final long serialVersionUID = 1L;

    @Override
    public Record deserialize(byte[] message) {
        String line = new String(message, StandardCharsets.UTF_8);
        String[] parts = line.split(",");

        Record inputRecord = new Record();
        inputRecord.setName(parts[1]);
        return inputRecord;
    }

    @Override
    public boolean isEndOfStream(Record nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Record> getProducedType() {
        return TypeInformation.of(Record.class);
    }
}
