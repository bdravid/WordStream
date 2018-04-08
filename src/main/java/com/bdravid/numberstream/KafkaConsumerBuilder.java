package com.bdravid.numberstream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerBuilder<K, V> {
    private final String bootStrapUrl;
    private final String groupId;
    private final String keyDeserializerClassName;
    private final String valueDeserializerClassName;

    private String clientId;

    public KafkaConsumerBuilder(String bootStrapUrl, String groupId, Class keyDeserializerClassName, Class valueDeserializerClassName) {
        this.bootStrapUrl = bootStrapUrl;
        this.groupId = groupId;
        this.keyDeserializerClassName = keyDeserializerClassName.getName();
        this.valueDeserializerClassName = valueDeserializerClassName.getName();
    }

    public KafkaConsumerBuilder<K, V> withClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public KafkaConsumer<K,V> build(){
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapUrl);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);
        if (clientId != null) {
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        }
        return new KafkaConsumer<>(properties);
    }
}
