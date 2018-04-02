package com.bdravid.wordstream;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;

public class KafkaInteractions {

    public static final long POLL_TIMEOUT_MILLIS = Long.MAX_VALUE;
    public static Supplier<KafkaProducer<String, String>> kafkaProducerSupplier = Suppliers.memoize(KafkaInteractions::buildProducer);
    public static Supplier<KafkaConsumer<String, String>> kafkaConsumerSupplier = Suppliers.memoize(KafkaInteractions::buildConsumer);
    public static Supplier<StreamsConfig> kafkaStreamsConfigSupplier = Suppliers.memoize(KafkaInteractions::buildStreamsConfig);


    private static KafkaProducer<String,String> buildProducer() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(configs);
    }

    private static KafkaConsumer<String, String> buildConsumer(){
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "*");
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, "C1");
        return new KafkaConsumer<>(configs);
    }

    private static StreamsConfig buildStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return new StreamsConfig(props);
    }
}
