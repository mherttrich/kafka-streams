package de.micha.tweetservice.serialize;


import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaSerializerDeserializer<T extends  org.apache.avro.specific.SpecificRecordBase> implements Serde<T> {

    private final Serde<T> inner;

    public KafkaSerializerDeserializer(Class<T> type) {
        inner = Serdes.serdeFrom(new ApacheAvroSerializer<>(), new ApacheAvroDeserializer<>(type));
    }

    @Override
    public Serializer<T> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<T> deserializer() {
        return inner.deserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.serializer().configure(configs, isKey);
        inner.deserializer().configure(configs, isKey);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }

}