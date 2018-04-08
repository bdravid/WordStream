package com.bdravid.numberstream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Scanner;

public class NumberStreamPublisher {
    private static final Logger logger = LoggerFactory.getLogger(NumberStreamPublisher.class);

    public static final String TOPIC_NUMBER_STREAM = "NumberStream";
    private KafkaProducer producer;

    private void init(){
        this.producer = new KafkaProducerBuilder<Integer, Integer>("localhost:9093", IntegerSerializer.class, IntegerSerializer.class)
        .withAckConfig("all")
        .build();
    }

    public void publish(int number){
        producer.send(new ProducerRecord<Integer, Integer>(TOPIC_NUMBER_STREAM, number));
    }

    public static void main(String args[]) {
        NumberStreamPublisher producer = new NumberStreamPublisher();
        producer.init();
        try {
            logger.info("Enter numbers to publish");
            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()) {
                String word = scanner.next();
                if (word.equalsIgnoreCase("ruko")) {
                    break;
                }
                Optional<Integer> parsed = tryParsing(word);
                if (parsed.isPresent()) {
                    producer.publish(parsed.get());
                    logger.info("Published number {}", parsed.get());
                } else {
                    logger.warn("Invalid number {}", word);
                }
            }
        } finally {
            producer.close();
        }
    }

    private void close() {
        producer.close();
    }

    private static Optional<Integer> tryParsing(String input) {
        try {
            Integer parsed = Integer.parseInt(input);
            return Optional.of(parsed);
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
