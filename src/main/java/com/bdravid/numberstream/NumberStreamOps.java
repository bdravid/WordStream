package com.bdravid.numberstream;

import org.apache.kafka.streams.StreamsBuilder;

public class NumberStreamOps {
    public static void main(String[] args) {
        StreamsBuilder sb = new StreamsBuilder();
        int sum = 0;
//        sb.stream(TOPIC_NUMBER_STREAM).mapValues(value -> sum + value);
//        KafkaStreams streams = new KafkaStreams(topology, props);
    }
}
