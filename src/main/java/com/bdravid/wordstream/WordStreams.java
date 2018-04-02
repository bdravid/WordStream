package com.bdravid.wordstream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.bdravid.wordstream.KafkaConstants.TOPIC_WORD_STREAM;

public class WordStreams {
    private static final Logger logger = LoggerFactory.getLogger(WordStreams.class);

    public void startStreaming(){
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(TOPIC_WORD_STREAM)
                .groupBy((k, v) -> v).count()
                .toStream()
                .foreach((val, count)
                        -> logger.info("Value : {}, count : {}", val, count));

        KafkaStreams wordStream = new KafkaStreams(builder.build(), KafkaInteractions.kafkaStreamsConfigSupplier.get());
        wordStream.setStateListener((newState, oldState) -> logger.info("Detected state change from : {} to {}", oldState, newState));
        wordStream.setUncaughtExceptionHandler((t, e) -> logger.error("Caught exception in wordStream thread {}", t, e));
        wordStream.start();
    }

    public static void main(String[] args) {
        new WordStreams().startStreaming();
    }
}
