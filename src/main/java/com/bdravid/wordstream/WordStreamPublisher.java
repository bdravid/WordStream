package com.bdravid.wordstream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

import static com.bdravid.wordstream.KafkaConstants.TOPIC_WORD_STREAM;
import static com.bdravid.wordstream.KafkaInteractions.kafkaProducerSupplier;

public class WordStreamPublisher {
    private final static Logger logger = LoggerFactory.getLogger(WordStreamPublisher.class);
    private KafkaProducer<String, String> producer = kafkaProducerSupplier.get();

    public void publish(String word) {
        producer.send(new ProducerRecord<>(TOPIC_WORD_STREAM, word));
    }

    public static void main(String args[]) {
        WordStreamPublisher publisher = new WordStreamPublisher();
        try {
            logger.info("Enter words to publish");
            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()) {
                String word = scanner.next();
                if (word.equalsIgnoreCase("ruko")) {
                    break;
                }
                publisher.publish(word);
                logger.info("Published message {}", word);
            }
        } finally {
            publisher.close();
        }
    }

    private void close() {
        producer.close();
    }
}
