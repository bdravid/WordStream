package com.bdravid.numberstream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerBuilder<K, V> {
    private final String bootstrapUrl;
    private final String keySerializerClassName;
    private final String valueSerializerClassName;

    private String ackConfig;

    public KafkaProducerBuilder(String bootstrapUrl, Class keySerializerClass, Class valueSerializerClass) {
        this.bootstrapUrl = bootstrapUrl;
        this.keySerializerClassName = keySerializerClass.getName();
        this.valueSerializerClassName = valueSerializerClass.getName();
    }

    public KafkaProducerBuilder<K, V> withAckConfig(String ackConfig) {
        this.ackConfig = ackConfig;
        return this;
    }

    public KafkaProducer<K, V> build(){
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapUrl);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClassName);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClassName);

        if (ackConfig != null) {
            properties.put(ProducerConfig.ACKS_CONFIG, ackConfig);
        }
        return new KafkaProducer<>(properties);
    }
}
